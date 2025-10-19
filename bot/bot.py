import os
import json
import tempfile
import logging
import asyncio
import redis.asyncio as aioredis
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
import uuid

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

# ============================================================
# ⚙️ VARIABLES DE ENTORNO
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ============================================================
# 🔌 CONEXIÓN REDIS
# ============================================================

redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
PHOTO_TIMEOUT = 10  # segundos de espera entre fotos del mismo ticket

# ============================================================
# 📸 MANEJO DE FOTOS (acumulación + envío a Redis)
# ============================================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if "ticket_photos" not in context.user_data:
        context.user_data["ticket_photos"] = []
    if "photo_timer" in context.user_data:
        context.user_data["photo_timer"].cancel()

    file = await update.message.photo[-1].get_file()
    os.makedirs("/app/cache/images", exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(dir="/app/cache/images", suffix=".jpg", delete=False)
    await file.download_to_drive(tmp.name)

    context.user_data["ticket_photos"].append(tmp.name)
    await update.message.reply_text(f"📸 Foto {len(context.user_data['ticket_photos'])} recibida. Esperando más...")
    logger.info(f"📷 Foto añadida por usuario {user_id} (total={len(context.user_data['ticket_photos'])})")

    async def process_after_delay():
        await asyncio.sleep(PHOTO_TIMEOUT)
        await enqueue_ocr_task(update, context)

    context.user_data["photo_timer"] = asyncio.create_task(process_after_delay())


async def enqueue_ocr_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photos = context.user_data.get("ticket_photos", [])
    if not photos:
        await update.message.reply_text("⚠️ No se detectaron fotos.")
        return

    task_id = str(uuid.uuid4())
    user_id = update.message.from_user.id

    await redis_client.xadd("ocr_tasks", {
        "task_id": task_id,
        "user_id": str(user_id),
        "photo_paths": json.dumps(photos),
        "timestamp": datetime.utcnow().isoformat(),
    })

    logger.info(f"📨 Publicada tarea OCR {task_id} con {len(photos)} fotos por usuario {user_id}")
    await update.message.reply_text(
        f"🧾 Ticket recibido y enviado a procesamiento OCR.\n📸 Fotos: {len(photos)}",
        parse_mode="Markdown",
    )

    context.user_data["ticket_photos"] = []

# ============================================================
# 📬 LISTENER DE RESPUESTAS (Redis → Telegram)
# ============================================================

async def listen_for_responses(app):
    """Escucha la cola 'bot_responses' y envía mensajes al usuario."""
    redis_sub = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    logger.info("📨 Escuchando respuestas en 'bot_responses'...")

    last_id = "0"
    while True:
        try:            
            messages = await redis_sub.xread({"bot_responses": last_id}, count=1, block=5000)
            if not messages:                
                continue

            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    user_id = int(fields.get("user_id", "0") or 0)
                    status = fields.get("status", "unknown")
                    tienda = fields.get("tienda", "Desconocida")
                    fecha = fields.get("fecha", "")
                    total = fields.get("total", "0")
                    productos = fields.get("productos", "0")
                    msg = fields.get("msg","No message present.")

                    if status == "ok-todo":
                        texto = (
                            f"✅ *Ticket procesado correctamente*\n"
                            f"🏪 {tienda}\n"
                            f"📅 {fecha}\n"
                            f"🛒 {productos} productos\n"
                            f"💰 Total: {total} €"
                        )
                    
                    elif status in ("ok", "warning"):
                        texto = f"{msg}"
                    
                    else:
                        texto = (
                            f"⚠️ Error al procesar el ticket (estado: {status}).\n"
                            f"⚠️ Mensaje: {msg})."
                        )
                                
                    if user_id:
                        await app.bot.send_message(chat_id=user_id, text=texto, parse_mode="Markdown")
                        logger.info(f"📬 Respuesta enviada a Telegram (user_id={user_id})")

                    await redis_sub.xdel("bot_responses", msg_id)
                    last_id = msg_id
        except Exception as e:
            logger.exception(f"❌ Error escuchando respuestas: {e}")
            await asyncio.sleep(3)

# ============================================================
# 🛑 CANCELAR TICKET
# ============================================================

async def cancel_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["ticket_photos"] = []
    await update.message.reply_text("❌ Ticket cancelado. Puedes empezar uno nuevo enviando una foto.")
    logger.info(f"🛑 Ticket cancelado por usuario {update.message.from_user.id}")

# ============================================================
# 🚀 MAIN (versión estable)
# ============================================================

async def main():
    logger.info("🤖 Iniciando bot de entrada (Telegram ↔ Redis)...")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("cancel", cancel_ticket))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    # Inicializa y arranca manualmente (sin cerrar el loop)
    await app.initialize()
    await app.start()

    # Crear la tarea del listener y mantener referencia
    listener_task = asyncio.create_task(listen_for_responses(app))
    logger.info("📡 Listener de Redis iniciado.")

    try:
        await app.updater.start_polling()
        logger.info("🚀 Bot ejecutándose con polling activo.")
        await asyncio.Event().wait()  # Mantiene el loop activo
    except asyncio.CancelledError:
        logger.warning("🛑 Cancelando ejecución del bot...")
    finally:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            logger.info("✅ Listener de Redis detenido correctamente.")

        await app.stop()
        await app.shutdown()
        logger.info("🧹 Bot detenido y limpiado correctamente.")

if __name__ == "__main__":
    asyncio.run(main())
