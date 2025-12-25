FROM apache/spark:3.3.3

USER root

# =========================================================
# 1. Thư mục bắt buộc cho SparkSubmit + spark.jars.packages
# =========================================================
RUN mkdir -p /tmp/.ivy/cache /tmp/.ivy/jars /tmp/spark-upload && \
    chown -R 185:185 /tmp/.ivy /tmp/spark-upload

# (Optional nhưng rất an toàn)
RUN mkdir -p /opt/spark/.ivy/cache /opt/spark/.ivy/jars && \
    chown -R 185:185 /opt/spark/.ivy

# =========================================================
# 2. Elasticsearch Spark connector (MATCH Spark 3.3.x)
# =========================================================
WORKDIR /opt/spark
RUN curl -fLo jars/elasticsearch-spark-30_2.12-8.11.3.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.3/elasticsearch-spark-30_2.12-8.11.3.jar

# =========================================================
# 3. (Optional) Chuẩn bị cho ML về sau
# =========================================================
# Spark MLlib đã có sẵn trong Spark
# Nếu sau này cần thêm lib Python (sklearn, pandas, v.v.)
# thì mở block này
#
# RUN pip install --no-cache-dir scikit-learn pandas numpy

# =========================================================
# 4. App code
# =========================================================
WORKDIR /app
COPY app /app
RUN chown -R 185:185 /app

USER 185