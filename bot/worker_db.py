import os
import json
import asyncio
import logging
import psycopg2
import redis.asyncio as aioredis
import sys
from datetime import datetime

# ============================================================
# 🔧 CONFIGURACIÓN
# ============================================================

os.makedirs("/app/cache", exist_ok=True)
log_file_path = "/app/cache/worker_db.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - DB - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
logger.info("💾 Worker BD iniciando...")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_HOST = os.getenv("DB_HOST", "monitor-precios-srv")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "monitorprecios")
DB_USER = os.getenv("DB_USER", "adm1n")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

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
# 💾 GUARDAR EN BD
# ============================================================

def parse_float(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).lower().replace("kg", "").replace("g", "").replace(",", ".")
    s = s.strip()
    try:
        return float(s)
    except:
        return 0.0

async def save_ticket_to_db(data, task_id, user_id):
    try:
        tienda = data.get("tienda", "Desconocida")
        numero_ticket = data.get("numero_ticket")
        fecha = data.get("fecha") or datetime.now().date()
        productos = data.get("productos", [])        

        if not isinstance(productos, list):
            try:
                productos = json.loads(productos) if isinstance(productos, str) else []
                logger.warning(f"⚠️ 'productos' no era lista, reconvertido: {type(productos)}")
            except Exception:
                logger.error(f"❌ 'productos' malformado: {productos}")
                productos = []

        total_ticket = sum(parse_float(p.get("total_linea") or 0) for p in productos)
        logger.info(f"🧾 Guardando ticket '{numero_ticket or 'sin_numero'}' de '{tienda}' ({len(productos)} productos, total={total_ticket:.2f})")

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        # --- Detección de duplicados (case-insensitive)
        if numero_ticket:
            cur.execute("""
                SELECT id FROM tickets
                WHERE LOWER(tienda) = LOWER(%s)
                AND numero_ticket = %s AND DATE(fecha) = %s;
            """, (tienda, numero_ticket, fecha))
            if cur.fetchone():
                logger.warning(f"⚠️ Ticket duplicado ({tienda}, {numero_ticket}, {fecha})")
                conn.close()
                mensaje = {
                    "task_id": task_id or "",
                    "user_id": user_id or "",  
                    "status": "warning" or "",
                    "msg": f"⚠️ Ticket duplicado ({tienda}, {numero_ticket}, {fecha})"
                }
                await redis_client.xadd("bot_responses", mensaje)
                logger.info(f"📤 Respuesta enviada al bot: {mensaje}")
                return

        # --- Inserción principal
        cur.execute("""
            INSERT INTO tickets (fecha, tienda, total_ticket, numero_ticket)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (fecha, tienda, total_ticket, numero_ticket))
        ticket_id = cur.fetchone()[0]

        for i, p in enumerate(productos, start=1):
            try:
                producto = (p.get("producto") or "").strip()
                categoria = (p.get("categoria") or "otros").strip()
                cantidad = parse_float(p.get("cantidad") or 1)
                precio_unitario = parse_float(p.get("precio_unitario") or 0)
                total_linea = parse_float(p.get("total_linea") or cantidad * precio_unitario)

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

                logger.info(f"   ✅ {i}/{len(productos)}: {producto} ({cantidad} × {precio_unitario} = {total_linea})")

            except Exception as e:
                logger.exception(f"   ⚠️ Error insertando producto {i}: {p} | {e}")

        conn.commit()
        conn.close()
        logger.info(f"💾 Ticket {ticket_id} guardado correctamente ({len(productos)} productos).")
        
        # --- Enviar confirmación al bot vía Redis
        mensaje = {
            "task_id": task_id or "",
            "user_id": user_id or "",
            "status": "ok-todo",
            "tienda": tienda or "",
            "fecha": str(fecha) or "",
            "total": round(total_ticket, 2) or 0,
            "productos": len(productos) or 0
        }
        await redis_client.xadd("bot_responses", mensaje)
        logger.info(f"📤 Respuesta enviada al bot: {mensaje}")

        # Auditoría local
        cache_path = f"/app/cache/db_{ticket_id}.json"
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug(f"🗂️ Datos del ticket guardados en {cache_path}")

    except Exception as e:
        logger.exception(f"❌ Error general guardando en BD: {e}")


# ============================================================
# 🔁 WORKER BD LOOP
# ============================================================

async def worker_db():
    logger.info("💾 Worker BD iniciado. Escuchando 'db_tasks'...")
    sys.stdout.flush()

    last_id = "0"

    while True:
        try:
            messages = await redis_client.xread({"db_tasks": last_id}, count=1, block=5000)
            if not messages:
                logger.debug("⏳ Esperando tareas DB...")
                continue

            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    task_id = fields.get("task_id")
                    user_id = fields.get("user_id")
                    resultado_raw = fields.get("resultado", "{}")

                    logger.info(f"📥 Recibida tarea DB {task_id} (msg_id={msg_id})")

                    try:
                        resultado = json.loads(resultado_raw)
                        await save_ticket_to_db(resultado, task_id, user_id)
                        logger.info(f"📤 Tarea DB {task_id} completada y eliminada de Redis.")
                    except json.JSONDecodeError:
                        logger.error(f"❌ JSON inválido en tarea DB {task_id}: {resultado_raw[:100]}")
                        mensaje = {
                            "task_id": task_id or "",
                            "user_id": user_id or "",  
                            "status": "Error" or "",
                            "msg": f"❌ JSON inválido en tarea DB {task_id}: {resultado_raw[:100]}"
                        }
                        await redis_client.xadd("bot_responses", mensaje)
                        logger.info(f"📤 Respuesta enviada al bot: {mensaje}")
                    except Exception as e:
                        logger.exception(f"❌ Error procesando DB ({task_id}): {e}")
                        mensaje = {
                            "task_id": task_id or "",
                            "user_id": user_id or "",  
                            "status": "Error" or "",
                            "msg": f"❌ Error procesando DB ({task_id})."
                        }
                        await redis_client.xadd("bot_responses", mensaje)
                        logger.info(f"📤 Respuesta enviada al bot: {mensaje}")
                        
                    last_id = msg_id

        except Exception as e:
            logger.exception(f"❌ Error en ciclo principal DB: {e}")
            await asyncio.sleep(3)

asyncio.run(worker_db())
