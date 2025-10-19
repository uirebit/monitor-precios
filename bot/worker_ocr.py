import os
import json
import asyncio
import logging
import redis.asyncio as aioredis
from google.cloud import documentai
from google.api_core.client_options import ClientOptions

# ============================================================
# 🔧 CONFIGURACIÓN GLOBAL
# ============================================================

os.makedirs("/app/cache", exist_ok=True)
log_file_path = "/app/cache/worker_ocr.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - OCR - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
LOCATION = os.getenv("GOOGLE_LOCATION")
PROCESSOR_ID = os.getenv("GOOGLE_PROCESSOR_ID")

client_options = ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
docai_client = documentai.DocumentProcessorServiceClient(client_options=client_options)
processor_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"

redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)

# ============================================================
# 🧠 WORKER OCR LOOP
# ============================================================

async def worker_ocr():
    logger.info("🧾 Worker OCR iniciado. Escuchando tareas en 'ocr_tasks'...")

    while True:
        messages = await redis_client.xread({"ocr_tasks": "0"}, count=1, block=5000)
        if not messages:
            continue

        for stream, msgs in messages:
            for msg_id, fields in msgs:
                task_id = fields.get("task_id")
                user_id = int(fields.get("user_id", "0"))
                
                photo_paths = json.loads(fields.get("photo_paths", "[]"))
                combined_text = ""

                try:
                    logger.info(f"📥 Procesando tarea OCR {task_id} ({len(photo_paths)} fotos)")

                    for i, path in enumerate(photo_paths, start=1):
                        with open(path, "rb") as image:
                            raw_document = {"content": image.read(), "mime_type": "image/jpeg"}
                        result = docai_client.process_document(
                            request={"name": processor_name, "raw_document": raw_document}
                        )
                        text = result.document.text or ""
                        combined_text += text + "\n"
                        logger.info(f"🧾 OCR imagen {i}/{len(photo_paths)} ({len(text)} caracteres)")

                    await redis_client.xadd("ia_tasks", {
                        "task_id": task_id,
                        "user_id": user_id,
                        "ocr_text": combined_text
                    })
                    
                    mensaje = {
                            "task_id": task_id or "",
                            "user_id": user_id or "",  
                            "status": "ok",
                            "msg": "✅🧾 OCR imagen procesada correctamente."
                        }
                    await redis_client.xadd("bot_responses", mensaje)
                    logger.info(f"📤 Respuesta enviada al bot: {mensaje}")

                    logger.info(f"📤 Enviada tarea a IA ({task_id})")
                    
                      # 🔥 Limpieza de imágenes locales tras procesar
                    for path in photo_paths:
                        try:
                            os.remove(path)
                            logger.info(f"🗑️ Eliminada imagen temporal: {path}")
                        except FileNotFoundError:
                            pass
                    
                    await redis_client.xdel("ocr_tasks", msg_id)

                except Exception as e:
                    logger.exception(f"❌ Error procesando OCR ({task_id}): {e}")

asyncio.run(worker_ocr())
