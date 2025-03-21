from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# 간단한 DataFrame 생성
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()  # 데이터 출력

spark.stop()  # Spark 종료