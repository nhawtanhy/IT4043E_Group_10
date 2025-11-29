FROM confluentinc/cp-kafka-connect-base:7.6.1

USER root
RUN microdnf install -y curl unzip && microdnf clean all

RUN mkdir -p /usr/share/java/hdfs-sink

# Tải plugin HDFS Sink 5.5.3 (bản ổn định cuối cùng)
RUN curl -L --fail -o /tmp/hdfs.zip \
    https://repo1.maven.org/maven2/io/confluent/kafka-connect-hdfs/5.5.3/kafka-connect-hdfs-5.5.3.zip && \
    unzip /tmp/hdfs.zip -d /usr/share/java/hdfs-sink && \
    rm /tmp/hdfs.zip

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/hdfs-sink"

USER appuser