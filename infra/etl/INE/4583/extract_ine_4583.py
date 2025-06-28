import requests
from kafka import KafkaProducer
import json

url = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/4583"
resp = requests.get(url)
resp.raise_for_status()
data = resp.json()

producer = KafkaProducer(
    bootstrap_servers='tfm_kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for serie in data:
    producer.send('ine.4583.raw', value=serie)

producer.flush()
print("Datos enviados a Kafka topic: ine.4583.raw")