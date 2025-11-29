Dưới đây là file README.md hoàn chỉnh, bạn chỉ cần copy nguyên văn vào README.md trong repo.
Mình đã format Markdown đầy đủ, đẹp, rõ ràng, có icon, có heading, dễ đọc cho người mới.

⸻


# 🌦️ Weather Data Pipeline  
**Kafka → Spark Streaming → Elasticsearch → Kibana**  
**Kafka → HDFS → Spark Batch → Elasticsearch**

Hệ thống này thu thập dữ liệu thời tiết từ API, đưa vào Kafka, xử lý real-time bằng Spark Streaming, lưu vào Elasticsearch để làm dashboard, đồng thời ghi raw data vào HDFS để chạy Spark Batch phân tích sâu.

---

## 📦 Thành phần hệ thống

- **Zookeeper** — điều phối Kafka
- **Kafka Broker** — nhận và lưu message thời tiết
- **Weather Producer** — sinh dữ liệu thời gian thực
- **Spark Streamer** — xử lý real-time + gửi vào Elasticsearch
- **HDFS (Namenode + Datanode)** — lưu Parquet để phân tích
- **Spark HDFS Streamer** (tuỳ chọn) — Kafka → HDFS Parquet
- **Spark Batch** — chạy phân tích theo ngày, tổng hợp số liệu
- **Elasticsearch** — lưu streaming + batch
- **Kibana** — dashboard trực quan

---

# 🚀 1. Build các service trước khi chạy

### Spark Streaming
```bash
docker compose build spark-streamer

Spark Batch

docker compose build spark-batch

Spark HDFS Streamer (nếu dùng)

docker compose build spark-hdfs-streamer


⸻

🚀 2. Khởi động hệ thống

2.1 Core: Zookeeper + Kafka

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

Bạn cần thấy log:

========== BATCH X ==========
[OK] Saved batch X → Elasticsearch


⸻

🗂️ 3. Khởi động HDFS

3.1 Namenode

docker compose up -d namenode

Nếu mới lần đầu:

docker exec -it namenode hdfs namenode -format

3.2 Datanode

docker compose up -d datanode

3.3 Kiểm tra HDFS

docker exec -it namenode hdfs dfsadmin -report
docker exec -it namenode hdfs dfs -ls /


⸻

📁 4. Spark HDFS Streamer (Kafka → HDFS)

Chạy nếu muốn lưu raw data vào HDFS:

docker compose up -d spark-hdfs-streamer
docker logs -f spark-hdfs-streamer

Check file xuất hiện:

docker exec -it namenode hdfs dfs -ls /weather/parquet


⸻

📊 5. Spark Batch ETL (HDFS → Elasticsearch)

Chạy batch một lần:

docker compose run spark-batch spark-submit /app/spark_batch.py

Hoặc chạy container luôn:

docker compose up -d spark-batch
docker logs -f spark-batch

Kết quả được ghi vào index:

weather_agg


⸻

🔍 6. Các lệnh kiểm tra nhanh

Kafka — xem message

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic weather_raw \
  --from-beginning \
  --max-messages 10

Kafka — xem offset

docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic weather_raw

HDFS — liệt kê file

docker exec -it namenode hdfs dfs -ls -R /weather

Elasticsearch — test index

curl http://localhost:9201/weather/_search?pretty
curl http://localhost:9201/weather_agg/_search?pretty

Logs Spark

docker logs -f spark-streamer
docker logs -f spark-hdfs-streamer
docker logs -f spark-batch


⸻

🛑 7. Dừng hệ thống

Dừng container:

docker compose down

Dừng + xoá volume:

docker compose down -v


⸻

🧬 Architecture Overview

[Weather Producer]
        |
        v
     [Kafka] -----> [Spark HDFS Streamer] ---> [HDFS]
        |
        v
[Spark Streaming] ---> [Elasticsearch] ---> [Kibana Dashboard]

[Spark Batch] <------ đọc từ HDFS ---------> xử lý --> Elasticsearch


⸻

🎉 Kết luận

Tài liệu này dành cho người mới chạy pipeline lần đầu — chỉ cần copy/paste từng bước là chạy được ngay.
Nếu bạn muốn, mình có thể xuất thành file PDF, hoặc chia thành dạng Quickstart 5 phút, hoặc thêm Mermaid diagram cho dễ nhìn hơn.

---

Nếu bạn muốn mình **xuất file README.md thực sự** (file tải về được), chỉ cần nói **"xuất file markdown"** — mình sẽ tạo file cho bạn.