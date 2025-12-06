# ============================================
# Stage 1 — Build Spark + PySpark Base Image
# ============================================
FROM eclipse-temurin:17-jdk AS spark-base

USER root

# [FIX] Added python3 and python3-pip. 
# The base image is Java only, so pip3 install would fail without this.
RUN apt-get update && \
    apt-get install -y curl python3 python3-pip && \
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
# ENV PATH="$SPARK_HOME/bin:$PATH"

# ----------- Install PySpark ----------
RUN pip3 install --break-system-packages pyspark==${SPARK_VERSION}

# ----------- AWS / S3 Dependencies (MODIFIED) -----------
# Note: Spark 3.5.1 uses Hadoop 3.3.4 internally. 
# We must use hadoop-aws-3.3.4 and a matching aws-java-sdk-bundle (1.12.262 is the standard match).
# The standalone aws-java-sdk jar usually fails due to missing transitive deps, so we use the 'bundle'.

RUN curl -L -o /opt/spark/jars/s3-2.18.41.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    \
    && curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \ 
    \
    && curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# ---------------------------------------

# ----------- Elasticsearch connector ----------
RUN curl -L -o /opt/spark/jars/elasticsearch-spark-30_2.12-8.11.0.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar

# ============================================
# Stage 2 — Runtime: Run PySpark Job
# ============================================
FROM spark-base AS spark-runner

WORKDIR /app
COPY spark.py .     

CMD ["spark-submit", "--master", "local[*]", "/app/spark_batch.py"]