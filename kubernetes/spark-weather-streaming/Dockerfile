FROM apache/spark:3.3.3

USER root

# =========================================================
# 1. Thư mục bắt buộc cho SparkSubmit + spark.jars.packages
# =========================================================
# /tmp/.ivy          : Ivy cache
# /tmp/spark-upload  : nơi spark-submit copy jar về
RUN mkdir -p /tmp/.ivy/cache /tmp/.ivy/jars /tmp/spark-upload && \
    chown -R 185:185 /tmp/.ivy /tmp/spark-upload

# (Optional nhưng an toàn) ivy dưới /opt/spark
RUN mkdir -p /opt/spark/.ivy/cache /opt/spark/.ivy/jars && \
    chown -R 185:185 /opt/spark/.ivy

# =========================================================
# 2. Kafka connector (MATCH Spark 3.3.3)
# =========================================================
WORKDIR /opt/spark
RUN curl -fLo jars/spark-sql-kafka-0-10_2.12-3.3.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar && \
    curl -fLo jars/spark-token-provider-kafka-0-10_2.12-3.3.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar && \
    curl -fLo jars/kafka-clients-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar && \
    curl -fLo jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# =========================================================
# 3. App code
# =========================================================
# =========================================================
# 2. Elasticsearch Spark connector (MATCH Spark 3.3.x)
# =========================================================
RUN curl -fLo jars/elasticsearch-spark-30_2.12-8.11.3.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.3/elasticsearch-spark-30_2.12-8.11.3.jar

WORKDIR /app
COPY app /app
RUN chown -R 185:185 /app

USER 185