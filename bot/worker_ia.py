import os
import json
import asyncio
import logging
import requests
import redis.asyncio as aioredis
import sys
from datetime import datetime

# ============================================================
# 🔧 CONFIGURACIÓN
# ============================================================

os.makedirs("/app/cache", exist_ok=True)
log_file_path = "/app/cache/worker_ia.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - IA - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
logger.info("🧠 Worker IA iniciando...")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://192.168.1.39:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

# ============================================================
# ⚙️ REDIS
# ============================================================

try:
    redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    logger.info(f"🔗 Conectado a Redis en {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logger.exception(f"❌ No se pudo conectar a Redis: {e}")
    sys.exit(1)

# ============================================================
# 🤖 LLAMAR A LLAMA3
# ============================================================

def llamar_a_llama3(texto_ticket):
    """Envía el texto del ticket al modelo Llama3 vía Ollama y devuelve el JSON."""
    try:
        prompt_path = "/app/prompts/ticket_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt_template = f.read()

        prompt = prompt_template.replace("{texto_ticket}", texto_ticket)
        logger.info("🧾 Solicitando inferencia a Llama3...")

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

        if not full_text.strip():
            logger.warning("⚠️ Respuesta vacía de Llama3.")
            return {}

        start = full_text.find("{")
        end = full_text.rfind("}") + 1
        if start == -1 or end == 0:
            logger.warning("⚠️ No se encontró JSON válido en respuesta IA.")
            return {}

        parsed = json.loads(full_text[start:end])
        logger.info(f"📦 JSON IA parseado correctamente ({len(parsed.get('productos', []))} productos)")
        return parsed

    except Exception as e:
        logger.exception("❌ Error procesando con Llama3")
        return {}

# ============================================================
# 🧠 WORKER LOOP
# ============================================================

async def worker_ia():
    logger.info("🧠 Worker IA iniciado. Escuchando tareas en 'ia_tasks'...")
    sys.stdout.flush()

    last_id = "0"

    while True:
        try:
            messages = await redis_client.xread({"ia_tasks": last_id}, count=1, block=5000)

            if not messages:
                logger.debug("⏳ Esperando nuevas tareas en 'ia_tasks'...")
                continue

            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    task_id = fields.get("task_id")
                    user_id = fields.get("user_id")
                    ocr_text = fields.get("ocr_text")

                    logger.info(f"📥 Recibida tarea IA {task_id} (msg_id={msg_id})")
                    if not ocr_text:
                        logger.warning(f"⚠️ Tarea IA {task_id} sin texto OCR.")
                        await redis_client.xdel("ia_tasks", msg_id)
                        continue

                    resultado_llama = llamar_a_llama3(ocr_text)
                    
                    if not resultado_llama:
                        logger.error(f"❌ IA no generó resultado. Cancelando tarea {task_id}.")
                        await redis_client.xdel("ia_tasks", msg_id)  # eliminar de la cola
                        
                        mensaje = {
                            "task_id": task_id or "",
                            "user_id": user_id or "",  
                            "status": "Error",
                            "msg": "❌ IA no generó resultado."
                        }
                        await redis_client.xadd("bot_responses", mensaje)
                        logger.info(f"📤 Respuesta enviada al bot: {mensaje}")
                        
                        continue
                        
                    logger.debug(f"🧩 Resultado bruto IA: {resultado_llama}")
                    
                    mensaje = {
                            "task_id": task_id or "",
                            "user_id": user_id or "",  
                            "status": "ok",
                            "msg": "✅🧠 Procesado IA con llama correcto."
                        }
                    await redis_client.xadd("bot_responses", mensaje)
                    logger.info(f"📤 Respuesta enviada al bot: {mensaje}")

                    # Registrar salida de IA en cache (auditoría)
                    cache_path = f"/app/cache/ia_{task_id}.json"
                    with open(cache_path, "w", encoding="utf-8") as f:
                        json.dump(resultado_llama, f, ensure_ascii=False, indent=2)

                    # Enviar a siguiente fase
                    await redis_client.xadd("db_tasks", {
                        "task_id": task_id,
                        "user_id": user_id,
                        "timestamp": datetime.utcnow().isoformat(),
                        "resultado": json.dumps(resultado_llama)
                    })
                    logger.info(f"📤 Enviada tarea a 'db_tasks' ({task_id})")

                    # Marcar mensaje como procesado
                    await redis_client.xdel("ia_tasks", msg_id)
                    last_id = msg_id

        except Exception as e:
            logger.exception(f"❌ Error en ciclo principal IA: {e}")
            await asyncio.sleep(3)  # evita loop rápido en caso de fallo

asyncio.run(worker_ia())
