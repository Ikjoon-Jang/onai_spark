from pyspark.sql import SparkSession

def get_kafka_data():
    spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()
    kafka_topic = "qlinx-orders-topic"
    kafka_bootstrap_servers = "3.36.178.68:9092"

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    return df