FROM apache/spark:3.3.3

USER root

# =====================================================
# 1️⃣ Python deps (BẮT BUỘC cho Spark ML)
# =====================================================
RUN pip3 install --no-cache-dir \
    numpy \
    pandas \
    pyarrow

# =====================================================
# 2️⃣ Hadoop S3A support (GIỐNG SILVER)
# =====================================================
RUN curl -fLo /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fLo /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# =====================================================
# 3️⃣ Spark submit / ivy (TRÁNH crash khi submit)
# =====================================================
RUN mkdir -p /tmp/.ivy/cache /tmp/.ivy/jars /tmp/spark-upload && \
    chown -R 185:185 /tmp/.ivy /tmp/spark-upload

# =====================================================
# 4️⃣ Elasticsearch Spark connector
# =====================================================
WORKDIR /opt/spark
RUN curl -fLo jars/elasticsearch-spark-30_2.12-8.11.3.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.3/elasticsearch-spark-30_2.12-8.11.3.jar

# =====================================================
# 5️⃣ App code + model
# =====================================================
WORKDIR /app
COPY app /app
COPY models /models

RUN chown -R 185:185 /app /models

# =====================================================
# 6️⃣ Run as spark user
# =====================================================
USER 185