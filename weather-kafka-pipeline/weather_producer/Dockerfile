FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    iputils-ping \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY s3aproducer.py .

CMD ["python", "-u", "s3aproducer.py"]