from data_consumer import get_kafka_data
from rdf_converter import convert_to_rdf
from fuseki_uploader import upload_to_fuseki

# ✅ Kafka에서 데이터 가져오기
df = get_kafka_data()

# ✅ DataFrame을 RDF로 변환
rdf_data = df.rdd.map(lambda row: convert_to_rdf(row)).collect()

# ✅ RDF를 Fuseki에 저장
for rdf in rdf_data:
    upload_to_fuseki(rdf)