

🌦️ Weather Data Pipeline

Kafka → Spark Streaming → Elasticsearch → Kibana
Kafka → HDFS → Spark Batch → Elasticsearch

This system collects weather data from an API, sends it into Kafka, processes it in real time with Spark Streaming, stores enriched results in Elasticsearch for visualization, and simultaneously writes raw data to HDFS for deeper batch analytics.

⸻

📦 System Components
	•	Zookeeper — coordinates Kafka
	•	Kafka Broker — receives and stores weather messages
	•	Weather Producer — generates real-time weather events
	•	Spark Streamer — real-time processing + publishes to Elasticsearch
	•	HDFS (Namenode + Datanode) — stores raw Parquet files
	•	Spark HDFS Streamer (optional) — Kafka → HDFS Parquet writer
	•	Spark Batch — daily batch ETL and aggregations
	•	Elasticsearch — stores streaming + batch results
	•	Kibana — visualization dashboard

⸻

🚀 1. Build Services Before Running

Spark Streaming

docker compose build spark-streamer

Spark Batch

docker compose build spark-batch

Spark HDFS Streamer (optional)

docker compose build spark-hdfs-streamer


⸻

🚀 2. Start the System

2.1 Core Services: Zookeeper + Kafka

docker compose up -d zookeeper
docker compose up -d kafka

2.2 Elasticsearch + Kibana

docker compose up -d elasticsearch
docker compose up -d kibana

2.3 Weather Producer

docker compose up -d weather-producer
docker logs -f weather-producer

2.4 Spark Streaming (real-time)

docker compose up -d spark-streamer
docker logs -f spark-streamer
docker exec -it spark-streamer rm -rf /checkpoint

Expected log:

========== BATCH X ==========
[OK] Saved batch X → Elasticsearch



🔍 6. Quick Inspection Commands

Kafka — view messages

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic weather_raw \
  --from-beginning \
  --max-messages 10

Kafka — check offsets

docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic weather_raw

HDFS — list files

docker exec -it namenode hdfs dfs -ls -R /weather

Elasticsearch — test index

curl http://localhost:9201/weather/_search?pretty
curl http://localhost:9201/weather_agg/_search?pretty

Spark logs

docker logs -f spark-streamer
docker logs -f spark-hdfs-streamer
docker logs -f spark-batch


⸻

🛑 7. Stop the Entire System

Stop containers:

docker compose down

Stop and remove volumes:

docker compose down -v


