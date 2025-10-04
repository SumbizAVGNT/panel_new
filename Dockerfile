FROM python:3.11-slim

# системные либы, чтобы Pillow собирался/работал
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential gcc libjpeg62-turbo-dev zlib1g-dev libpng-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# зависимости: если есть requirements.txt — используем его,
# иначе ставим минимальный набор под ваш код
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt || \
    pip install --no-cache-dir \
      flask==3.0.3 websockets requests pillow python-dotenv pymysql

# сам проект
COPY .. /app

ENV PYTHONUNBUFFERED=1

# по умолчанию запустим панель (compose переопределит)
CMD ["python", "panel_new/run.py"]
