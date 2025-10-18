import os
import re
import json
import tempfile
import psycopg2
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from google.cloud import documentai
from google.api_core.client_options import ClientOptions

# ============================================================
# 🔧 CONFIGURACIÓN GLOBAL
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "monitorprecios")
DB_USER = os.getenv("DB_USER", "adm1n")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

BOT_TOKEN = os.getenv("BOT_TOKEN")
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID", "ticket-reader-475515")
LOCATION = os.getenv("GOOGLE_LOCATION", "eu")
PROCESSOR_ID = os.getenv("GOOGLE_PROCESSOR_ID", "1e10d3e5409b524e")

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/keys/ticket-reader-key.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS

# ============================================================
# ⚙️ CONFIGURAR CLIENTE DOCUMENT AI
# ============================================================

client_options = ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
docai_client = documentai.DocumentProcessorServiceClient(client_options=client_options)
processor_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"

# ============================================================
# 💾 FUNCIÓN GUARDAR EN BASE DE DATOS
# ============================================================

def save_ticket_to_db(tienda, total_ticket, productos):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Insertar ticket
        cur.execute("""
            INSERT INTO tickets (tienda, total_ticket)
            VALUES (%s, %s) RETURNING id;
        """, (tienda, total_ticket))
        ticket_id = cur.fetchone()[0]

        # Insertar líneas
        for p in productos:
            cur.execute("""
                INSERT INTO compras (fecha, tienda, producto, total_linea)
                VALUES (CURRENT_DATE, %s, %s, %s);
            """, (tienda, p.get("producto"), p.get("total", None)))

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"💾 Ticket {ticket_id} guardado ({len(productos)} líneas).")
        return ticket_id

    except Exception as e:
        logger.exception("❌ Error guardando en BD")
        return None

# ============================================================
# 📸 MANEJO DE FOTOS (OCR + PARSEO)
# ============================================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Descargar imagen
        file = await update.message.photo[-1].get_file()
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            await file.download_to_drive(tmp.name)
            local_path = tmp.name
        logger.info(f"📷 Imagen recibida: {local_path}")

        # Procesar con Document AI
        with open(local_path, "rb") as image:
            raw_document = {"content": image.read(), "mime_type": "image/jpeg"}

        logger.info("📨 Enviando imagen a Google Document AI...")
        result = docai_client.process_document(request={"name": processor_name, "raw_document": raw_document})
        doc = result.document

        # Guardar respuesta completa (para depuración)
        os.makedirs("/app/cache", exist_ok=True)
        with open("/app/cache/documentai_raw.json", "w", encoding="utf-8") as f:
            f.write(documentai.Document.to_json(doc))
        logger.info("💾 OCR completo guardado en /app/cache/documentai_raw.json")

        # =======================
        # 🔍 Extraer entidades principales
        # =======================
        tienda, fecha, total, moneda = None, None, None, None
        raw_lines = []

        for ent in doc.entities:
            t, v = ent.type_.lower(), ent.mention_text.strip()
            if "supplier_name" in t:
                tienda = v
            elif "receipt_date" in t:
                fecha = v
            elif "currency" in t:
                moneda = v
            elif "total_amount" in t:
                total = v
            elif "line_item" in t:
                raw_lines.append(v)

        total_ticket = float(total.replace(",", ".")) if total else None
        logger.info(f"🏪 Tienda: {tienda or 'Desconocida'} | 📅 Fecha: {fecha} | 💰 Total: {total_ticket} {moneda or '€'}")
        logger.info(f"🧾 {len(raw_lines)} líneas brutas detectadas por Google.")

        # =======================
        # 🧠 Reconstrucción simplificada de productos
        # =======================
        productos = []
        i = 0
        while i < len(raw_lines):
            current = raw_lines[i]
            next_line = raw_lines[i + 1] if i + 1 < len(raw_lines) else None

            # Línea con texto y número
            if re.search(r"[A-Za-zÁÉÍÓÚÑa-záéíóúñ].*\d", current):
                productos.append({"producto": current})
                i += 1
                continue

            # Línea + precio separado
            if next_line and re.match(r"^[0-9]+[.,][0-9]{1,2}$", next_line):
                productos.append({
                    "producto": current,
                    "total": float(next_line.replace(",", "."))
                })
                i += 2
                continue

            productos.append({"producto": current})
            i += 1

        # =======================
        # 📜 LOGS LIMPIOS — solo lo importante
        # =======================
        logger.info("✅ Productos listos para guardar:")
        for p in productos:
            line = f"• {p['producto']}"
            if p.get('total'): line += f" → {p['total']} €"
            logger.info(line)

        # Guardar en BD
        ticket_id = save_ticket_to_db(tienda or "Desconocida", total_ticket, productos)

        # Enviar resumen a Telegram
        resumen = "\n".join([f"• {p['producto']}" for p in productos[:10]])
        await update.message.reply_text(
            f"✅ Ticket de {tienda or 'desconocida'} procesado.\n"
            f"🧾 {len(productos)} líneas detectadas\n💰 Total: {total_ticket} {moneda or '€'}\n\n"
            f"{resumen}"
        )

        os.remove(local_path)

    except Exception as e:
        logger.exception("❌ Error procesando foto")
        await update.message.reply_text("⚠️ Error procesando el ticket, revisa los logs en consola.")

# ============================================================
# 🤖 BOT TELEGRAM — INICIO
# ============================================================

def main():
    logger.info("🚀 Iniciando bot de seguimiento de precios...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.run_polling()

if __name__ == "__main__":
    main()

