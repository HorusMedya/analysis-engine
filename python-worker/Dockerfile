FROM python:3.12-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY . .

# Run as non-root user
RUN useradd -m celeryuser
USER celeryuser

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]
