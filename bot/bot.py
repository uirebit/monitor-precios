import os
import json
import tempfile
import psycopg2
import logging
import requests
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from google.cloud import documentai
from google.api_core.client_options import ClientOptions

# ============================================================
# 🔧 CONFIGURACIÓN GLOBAL
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "monitorprecios")
DB_USER = os.getenv("DB_USER", "adm1n")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Google Document AI
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID", "ticket-reader-475515")
LOCATION = os.getenv("GOOGLE_LOCATION", "eu")
PROCESSOR_ID = os.getenv("GOOGLE_PROCESSOR_ID", "1e10d3e5409b524e")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/keys/ticket-reader-key.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS

# Ollama local
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://192.168.1.39:11434")  # o http://localhost:11434
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

# ============================================================
# ⚙️ CLIENTE DOCUMENT AI
# ============================================================

client_options = ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
docai_client = documentai.DocumentProcessorServiceClient(client_options=client_options)
processor_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"

# ============================================================
# 💾 FUNCIONES DE BD
# ============================================================

def save_ticket_to_db(tienda, total_ticket, productos):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        cur.execute("INSERT INTO tickets (tienda, total_ticket) VALUES (%s, %s) RETURNING id;", (tienda, total_ticket))
        ticket_id = cur.fetchone()[0]

        for p in productos:
            cur.execute("""
                INSERT INTO compras (fecha, tienda, producto, total_linea)
                VALUES (CURRENT_DATE, %s, %s, %s);
            """, (tienda, p.get("producto"), p.get("total_linea")))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"💾 Ticket {ticket_id} guardado ({len(productos)} líneas).")
        return ticket_id

    except Exception as e:
        logger.exception("❌ Error guardando en BD")
        return None

# ============================================================
# 🧠 FUNCIÓN: LLAMAR A LLAMA3 LOCAL
# ============================================================

def llamar_a_llama3(texto_ticket):
    prompt = f"""
Eres un modelo experto en interpretar tickets de supermercado en español.
Extrae los productos, cantidades y precios del siguiente texto y devuelve SOLO un JSON válido con el formato:
[
  {{"producto": "nombre", "cantidad": número o null, "precio_unitario": número o null, "total_linea": número}}
]

Texto:
\"\"\"{texto_ticket}\"\"\"
"""
    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt},
            stream=True,
            timeout=180
        )

        # Acumular la respuesta completa del stream
        full_text = ""
        for line in response.iter_lines():
            if not line:
                continue
            try:
                data = json.loads(line.decode("utf-8"))
                if "response" in data:
                    full_text += data["response"]
                if data.get("done"):
                    break
            except json.JSONDecodeError:
                continue

        logger.info("🧠 --- RESPUESTA COMPLETA DE LLAMA3 ---")
        logger.info(full_text)

        # Extraer JSON dentro del texto generado
        start = full_text.find("[")
        end = full_text.rfind("]") + 1
        if start == -1 or end == 0:
            logger.warning("⚠️ No se encontró JSON en la respuesta.")
            return []

        json_text = full_text[start:end]
        productos = json.loads(json_text)

        logger.info(f"✅ {len(productos)} productos detectados por Llama3.")
        return productos

    except Exception as e:
        logger.exception("❌ Error procesando con Llama3")
        return []


# ============================================================
# 📸 PROCESAR FOTO
# ============================================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        file = await update.message.photo[-1].get_file()
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            await file.download_to_drive(tmp.name)
            local_path = tmp.name
        logger.info(f"📷 Imagen recibida: {local_path}")

        # Procesar con Google Document AI
        with open(local_path, "rb") as image:
            raw_document = {"content": image.read(), "mime_type": "image/jpeg"}

        logger.info("📨 Enviando imagen a Google Document AI...")
        result = docai_client.process_document(request={"name": processor_name, "raw_document": raw_document})
        doc = result.document

        texto = doc.text or ""
        logger.info(f"📄 OCR extraído ({len(texto)} caracteres).")
        os.makedirs("/app/cache", exist_ok=True)
        with open("/app/cache/ticket_text.txt", "w", encoding="utf-8") as f:
            f.write(texto)
        logger.info("💾 Texto OCR guardado en /app/cache/ticket_text.txt")

        # Procesar con Llama3 local
        productos = llamar_a_llama3(texto)

        if not productos:
            await update.message.reply_text("⚠️ No se pudieron extraer productos del ticket.")
            return

        tienda = productos[0].get("tienda", "Desconocida")
        total_ticket = sum([p.get("total_linea", 0) for p in productos if p.get("total_linea")])

        ticket_id = save_ticket_to_db(tienda, total_ticket, productos)

        resumen = "\n".join([f"• {p['producto']} → {p.get('total_linea', '?')}€" for p in productos[:10]])
        await update.message.reply_text(
            f"✅ Ticket procesado correctamente.\n"
            f"💰 Total: {total_ticket:.2f} €\n\n"
            f"{resumen}"
        )

        os.remove(local_path)

    except Exception as e:
        logger.exception("❌ Error procesando ticket")
        await update.message.reply_text("⚠️ Error procesando el ticket. Revisa los logs.")

# ============================================================
# 🚀 MAIN
# ============================================================

def main():
    logger.info("🤖 Iniciando bot DocumentAI → Llama3...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.run_polling()

if __name__ == "__main__":
    main()
