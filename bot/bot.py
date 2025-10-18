import os
import json
import tempfile
import psycopg2
import logging
import requests
import asyncio
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
from google.cloud import documentai
from google.api_core.client_options import ClientOptions

# ============================================================
# 🔧 CONFIGURACIÓN GLOBAL
# ============================================================

os.makedirs("/app/cache", exist_ok=True)
log_file_path = "/app/cache/log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("🪵 Logging inicializado. Guardando en /app/cache/log.txt")

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
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://192.168.1.39:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

# ============================================================
# ⚙️ CLIENTE DOCUMENT AI
# ============================================================

client_options = ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
docai_client = documentai.DocumentProcessorServiceClient(client_options=client_options)
processor_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"

PHOTO_TIMEOUT = 10  # segundos de espera entre fotos del mismo ticket

# ============================================================
# 📸 MANEJO DE FOTOS (multi-selección automática)
# ============================================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Acumula fotos y las procesa automáticamente tras inactividad."""
    user_id = update.message.from_user.id

    if "ticket_photos" not in context.user_data:
        context.user_data["ticket_photos"] = []
    if "photo_timer" in context.user_data:
        context.user_data["photo_timer"].cancel()

    file = await update.message.photo[-1].get_file()
    tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
    await file.download_to_drive(tmp.name)

    context.user_data["ticket_photos"].append(tmp.name)
    await update.message.reply_text(f"📸 Foto {len(context.user_data['ticket_photos'])} recibida. Esperando más...")

    logger.info(f"📷 Foto añadida por usuario {user_id} (total={len(context.user_data['ticket_photos'])})")

    async def process_after_delay():
        await asyncio.sleep(PHOTO_TIMEOUT)
        await process_ticket_photos(update, context)

    context.user_data["photo_timer"] = asyncio.create_task(process_after_delay())

async def process_ticket_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa todas las fotos del ticket tras inactividad."""
    photos = context.user_data.get("ticket_photos", [])
    if not photos:
        await update.message.reply_text("⚠️ No se detectaron fotos.")
        return

    await update.message.reply_text(f"🔍 Procesando {len(photos)} fotos del ticket...")
    combined_text = ""

    for i, path in enumerate(photos, start=1):
        with open(path, "rb") as image:
            raw_document = {"content": image.read(), "mime_type": "image/jpeg"}
        result = docai_client.process_document(request={"name": processor_name, "raw_document": raw_document})
        doc = result.document
        combined_text += (doc.text or "") + "\n"
        logger.info(f"🧾 OCR extraído de imagen {i}: {len(doc.text or '')} caracteres")

    combined_path = "/app/cache/ticket_text_combined.txt"
    with open(combined_path, "w", encoding="utf-8") as f:
        f.write(combined_text)
    logger.info(f"💾 Texto combinado guardado en {combined_path}")

    resultado_llama = llamar_a_llama3(combined_text)
    tienda = resultado_llama.get("tienda", "Desconocida")
    numero_ticket = resultado_llama.get("numero_ticket", None)
    fecha = resultado_llama.get("fecha", datetime.now().date())
    productos = resultado_llama.get("productos", [])

    if not productos:
        await update.message.reply_text("⚠️ No se pudieron extraer productos del ticket.")
        context.user_data["ticket_photos"] = []
        return

    for p in productos:
        for key in ["total_linea", "precio_unitario"]:
            val = p.get(key, 0)
            if isinstance(val, str):
                val = val.replace("€", "").replace(",", ".").strip()
                try:
                    val = float(val)
                except ValueError:
                    val = 0.0
            p[key] = val
        if not p.get("cantidad"):
            p["cantidad"] = 1

    total_ticket = sum([p.get("total_linea", 0) or 0 for p in productos])
    resultado_guardado = save_ticket_to_db(tienda, total_ticket, productos, numero_ticket, fecha)

    if resultado_guardado.get("duplicado"):
        await update.message.reply_text(
            f"⚠️ Ticket duplicado detectado.\n"
            f"🏪 Tienda: {tienda}\n🧾 Nº: {numero_ticket or 'N/D'}\n📅 Fecha: {fecha}\n➡️ No se ha guardado nuevamente."
        )
    else:
        await update.message.reply_text(
            f"✅ Ticket procesado correctamente ({len(productos)} productos).\n"
            f"🏪 {tienda}\n🧾 {numero_ticket or 'N/D'}\n📅 {fecha}\n💰 {total_ticket:.2f} €"
        )

    for path in photos:
        os.remove(path)
    context.user_data["ticket_photos"] = []

# ============================================================
# 💾 FUNCIONES DE BD
# ============================================================

def save_ticket_to_db(tienda, total_ticket, productos, numero_ticket=None, fecha=None):
    try:
        if fecha is None:
            fecha = datetime.now().date()

        logger.info(f"🧾 Guardando ticket '{numero_ticket or 'sin_numero'}' de '{tienda}' ({len(productos)} productos) | Fecha: {fecha}")

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        if numero_ticket:
            cur.execute("""
                SELECT id FROM tickets
                WHERE tienda = %s AND numero_ticket = %s AND DATE(fecha) = %s;
            """, (tienda, numero_ticket, fecha))
            existente = cur.fetchone()
            if existente:
                logger.warning(f"⚠️ Ticket duplicado detectado (tienda='{tienda}', numero='{numero_ticket}', fecha='{fecha}').")
                cur.close()
                conn.close()
                return {"ticket_id": existente[0], "duplicado": True}

        cur.execute("""
            INSERT INTO tickets (fecha, tienda, total_ticket, numero_ticket)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (fecha, tienda, total_ticket, numero_ticket))
        ticket_id = cur.fetchone()[0]
        logger.info(f"🆔 Ticket insertado con ID {ticket_id}")

        def limpiar_numero(valor):
            if isinstance(valor, (int, float)):
                return float(valor)
            if not valor:
                return 0.0
            val = str(valor).replace("€", "").replace(",", ".").strip()
            try:
                return float(val)
            except:
                return 0.0

        for i, p in enumerate(productos, start=1):
            try:
                producto = (p.get("producto") or "").strip()
                cantidad = limpiar_numero(p.get("cantidad")) or 1
                precio_unitario = limpiar_numero(p.get("precio_unitario"))
                total_linea = limpiar_numero(p.get("total_linea"))
                categoria = (p.get("categoria") or "otros").strip()

                if total_linea == 0 and precio_unitario > 0:
                    total_linea = round(cantidad * precio_unitario, 2)

                cur.execute("""
                    INSERT INTO compras (
                        fecha, tienda, producto, producto_normalizado,
                        categoria_normalizada, cantidad, precio_unitario, total_linea
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    fecha, tienda, producto, producto.lower(),
                    categoria.lower(), cantidad, precio_unitario, total_linea
                ))

                logger.info(f"   ✅ {i}/{len(productos)}: {producto} ({cantidad} × {precio_unitario} = {total_linea} €)")

            except Exception as e:
                logger.error(f"   ⚠️ Error insertando producto {i}: {p}")
                logger.exception(e)

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"💾 Ticket {ticket_id} guardado correctamente.")
        return {"ticket_id": ticket_id, "duplicado": False}

    except Exception:
        logger.exception("❌ Error general guardando en BD")
        return None

# ============================================================
# 🧠 LLAMAR A LLAMA3
# ============================================================

def llamar_a_llama3(texto_ticket):
    try:
        prompt_path = "/app/prompts/ticket_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt_template = f.read()

        prompt = prompt_template.replace("{texto_ticket}", texto_ticket)

        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt},
            stream=True,
            timeout=180
        )

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

        raw_path = "/app/cache/llama_raw.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(full_text)

        start = full_text.find("{")
        end = full_text.rfind("}") + 1
        if start == -1 or end == 0:
            logger.warning("⚠️ No se encontró JSON en la respuesta.")
            return {"tienda": "Desconocida", "numero_ticket": None, "fecha": None, "productos": []}

        data = json.loads(full_text[start:end])

        return {
            "tienda": data.get("tienda", "Desconocida"),
            "numero_ticket": data.get("numero_ticket", None),
            "fecha": data.get("fecha", None),
            "productos": data.get("productos", [])
        }

    except Exception:
        logger.exception("❌ Error procesando con Llama3")
        return {"tienda": "Desconocida", "numero_ticket": None, "fecha": None, "productos": []}

# ============================================================
# 🚀 MAIN
# ============================================================

def main():
    logger.info("🤖 Iniciando bot DocumentAI → Llama3 (multi-foto automática)...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    async def cancel_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data["ticket_photos"] = []
        await update.message.reply_text("❌ Ticket cancelado. Puedes empezar uno nuevo enviando una foto.")
        logger.info(f"🛑 Ticket cancelado por usuario {update.message.from_user.id}")

    app.add_handler(CommandHandler("cancel", cancel_ticket))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.run_polling()

if __name__ == "__main__":
    main()
