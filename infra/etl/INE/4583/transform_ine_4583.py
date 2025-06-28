from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'ine.4583.raw',
    bootstrap_servers='tfm_kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="transformadores_ine"
)

producer = KafkaProducer(
    bootstrap_servers='tfm_kafka:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

for serie in consumer:
    item = serie.value
    nombre = item.get("Nombre", "")
    if "Ambos sexos" in nombre and "Propiedad. Hogar." in nombre:
        grupo = "total" if "Todas las edades" in nombre else "16_29" if "De 16 a 29 años" in nombre else None
        if grupo:
            for punto in item.get("Data", []):
                doc = {
                    "year": punto["Anyo"],
                    "grupo": grupo,
                    "pct": punto["Valor"]
                }
                producer.send("ine.4583.processed", value=doc)