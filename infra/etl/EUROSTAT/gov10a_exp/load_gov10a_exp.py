# etl/Eurostat/gov_10a_exp/load_gov10a_exp.py
from kafka import KafkaConsumer
from etl.common.db import get_db
from time import sleep
import json

db = get_db()
sleep(10)  # esperar a Kafka
coll = db["EUROSTAT_GOV10aEXP_VIVIENDA"]

# Configurar el consumidor de Kafka
consumer = KafkaConsumer(
    'eurostat.gov10a.processed',
    bootstrap_servers='tfm_kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='eurostat_loader_group',
    auto_offset_reset='earliest'
)

# Insertar o actualizar por (region, year)
for msg in consumer:
    doc = msg.value
    if not all(k in doc for k in ["region", "year", "pct_gdp"]):
        continue

    filtro = {"region": doc["region"], "year": doc["year"]}
    actualizacion = {"$set": doc}
    coll.update_one(filtro, actualizacion, upsert=True)
    
print("Datos cargados en MongoDB: EUROSTAT_GOV10aEXP_VIVIENDA")