from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HDFSTest").getOrCreate()

# Tạo dữ liệu đơn giản
df = spark.createDataFrame([(1, "hello"), (2, "spark"), (3, "hdfs")], ["id", "text"])

# Ghi vào HDFS
df.write.mode("overwrite").json("hdfs://namenode:8020/test_batch/out")

spark.stop()
