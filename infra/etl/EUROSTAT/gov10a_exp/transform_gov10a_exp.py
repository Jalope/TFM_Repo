# etl/Eurostat/gov_10a_exp/transform_gov10a_exp.py

from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'eurostat.gov10a.raw',
    bootstrap_servers='tfm_kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='eurostat_transform_group',
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers='tfm_kafka:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

for msg in consumer:
    doc = msg.value
    geo = doc.get("geo")
    year = doc.get("year")
    pct_gdp = doc.get("pct_gdp")

    if geo in ["EA19", "EA20", "EU27_2020"]:
        region = "Europe"
    elif geo == "ES":
        region = "Spain"
    else:
        region = geo

    # Emitimos documento limpio
    transformed = {
        "region": region,
        "year": year,
        "pct_gdp": pct_gdp
    }

    producer.send("eurostat.gov10a.processed", value=transformed)

producer.flush()