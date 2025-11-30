# ============================================
# Stage 1 — Build Spark + PySpark Base Image
# ============================================
FROM eclipse-temurin:17-jdk AS spark-base

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    curl python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# ----------- Install Spark ----------
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o spark.tgz && \
    tar -xzf spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# ----------- Install PySpark ----------
RUN pip3 install --break-system-packages pyspark==${SPARK_VERSION}

# ----------- Add Kafka Connectors ----------
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar

RUN curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

RUN curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar

# ----------- Missing dependency ----------
RUN curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ----------- Elasticsearch connector ----------
RUN curl -L -o /opt/spark/jars/elasticsearch-spark-30_2.12-8.11.0.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar

# ============================================
# Stage 2 — Runtime: Run PySpark Job
# ============================================
FROM spark-base AS spark-runner

WORKDIR /app
COPY spark.py .     

CMD ["spark-submit", "--master", "local[*]", "/app/spark.py"]
