from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="3.36.178.68:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

data = {
    "order_id": "250320-000000001",
    "departure": "l0001",
    "destination": "l0009",
    "customer": "c0001",
    "status": "new"
}

producer.send("qlinx-orders-topic", value=data)
producer.flush()