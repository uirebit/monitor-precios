# ===========================
# 🔹 Etapa 1: Build temporal
# ===========================
FROM python:3.11-slim AS builder

# Variables y optimizaciones
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /install

# Instala dependencias básicas necesarias para compilaciones
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copia solo el requirements.txt
COPY requirements.txt .

# Instala las dependencias en una carpeta aislada
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt --target=/install

# ===========================
# 🔹 Etapa 2: Imagen final
# ===========================
FROM python:3.11-slim

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Madrid

WORKDIR /app

# Copia dependencias desde el builder (sin herramientas de compilación)
COPY --from=builder /install /usr/local/lib/python3.11/site-packages

# Copia el código del bot
COPY . .

# Permisos (por si Synology mapea volúmenes con UID distintos)
RUN chmod -R 755 /app

# Comando principal
CMD ["python", "bot.py"]

