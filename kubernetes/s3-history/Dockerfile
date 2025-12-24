FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    boto3 \
    pandas \
    pyarrow

COPY app/s3_weather_history.py /app/s3_weather_history.py

CMD ["python", "/app/s3_weather_history.py"]