from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType
from rdflib import Graph, URIRef, Literal, Namespace
import requests

# ✅ Apache Jena Fuseki 서버 정보
FUSEKI_ENDPOINT = "http://3.36.178.68:3030/dataset/update"  # dataset 이름에 맞게 수정

# ✅ Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaToFuseki") \
    .getOrCreate()

# ✅ Kafka 설정
kafka_topic = "qlinx-orders-topic"
kafka_bootstrap_servers = "3.36.178.68:9092"

# ✅ JSON 스키마 정의
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("departure", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("status", StringType(), True)
])

# ✅ Kafka 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# ✅ Kafka 데이터 변환
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ✅ RDF 변환 함수
def convert_to_rdf(row):
    g = Graph()
    ns = Namespace("http://qlinx.oneqic.co.kr/orders#")

    order_uri = URIRef(ns[row["order_id"]])
    g.add((order_uri, ns.departure, Literal(row["departure"])))
    g.add((order_uri, ns.destination, Literal(row["destination"])))
    g.add((order_uri, ns.customer, Literal(row["customer"])))
    g.add((order_uri, ns.status, Literal(row["status"])))

    return g.serialize(format='turtle')

# ✅ Fuseki에 RDF 데이터를 INSERT하는 함수
def insert_into_fuseki(rdf_data):
    query = f"""
    INSERT DATA {{
        {rdf_data}
    }}
    """
    headers = {"Content-Type": "application/sparql-update"}
    response = requests.post(FUSEKI_ENDPOINT, data=query, headers=headers)
    return "Success" if response.status_code == 200 else f"Error: {response.status_code}"

# ✅ Spark UDF로 변환
convert_to_rdf_udf = udf(convert_to_rdf, StringType())
insert_into_fuseki_udf = udf(insert_into_fuseki, StringType())

# ✅ RDF 변환 및 Fuseki 저장
df_rdf = df_parsed.withColumn("rdf", convert_to_rdf_udf(df_parsed))
df_fuseki = df_rdf.withColumn("fuseki_status", insert_into_fuseki_udf(df_rdf["rdf"]))

# ✅ 콘솔 출력 및 Fuseki 업로드
query = df_fuseki.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()