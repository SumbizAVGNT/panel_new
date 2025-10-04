FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential gcc libjpeg62-turbo-dev zlib1g-dev libpng-dev \
      procps bash util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN if [ -s /tmp/requirements.txt ]; then pip install --no-cache-dir -r /tmp/requirements.txt; fi

COPY docker/git-entrypoint.sh /usr/local/bin/git-entrypoint.sh
RUN chmod +x /usr/local/bin/git-entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/usr/local/bin/git-entrypoint.sh"]
CMD ["python", "run.py"]
