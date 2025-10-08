FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential gcc libjpeg62-turbo-dev zlib1g-dev libpng-dev \
      procps bash util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# (опционально) заранее установить зависимости, если есть
COPY requirements.txt /tmp/requirements.txt
RUN if [ -s /tmp/requirements.txt ]; then pip install --no-cache-dir -r /tmp/requirements.txt; fi

# оба entrypoint-а в образ: sh-обёртка и python-скрипт
COPY docker/git-entrypoint.sh /usr/local/bin/git-entrypoint.sh
COPY docker/git_entrypoint.py /usr/local/bin/git_entrypoint.py

RUN chmod +x /usr/local/bin/git-entrypoint.sh \
    && chmod +x /usr/local/bin/git_entrypoint.py

ENV PYTHONUNBUFFERED=1

# базовый ENTRYPOINT можно оставить, compose всё равно перегружает его для каждого сервиса
ENTRYPOINT ["/usr/local/bin/git-entrypoint.sh"]
CMD ["python", "run.py"]
