from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Kafka에서 읽어올 토픽 설정
kafka_topic = "qlinx-orders-topic"
kafka_bootstrap_servers = "localhost:9092"

# Kafka 스트림 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# JSON 데이터 파싱
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 콘솔에 출력
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()