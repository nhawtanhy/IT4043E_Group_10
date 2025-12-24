FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir requests confluent-kafka

COPY app/weather_producer.py /app/weather_producer.py

CMD ["python", "/app/weather_producer.py"]