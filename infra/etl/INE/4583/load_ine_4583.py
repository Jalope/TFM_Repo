# load_ine_4583.py
from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import defaultdict
import json
from time import sleep
import os
from etl.common.db import get_db

# Conexión a MongoDB
db = get_db()
sleep(10) # Esperar a que el consumidor se conecte
coll = db["INE_4583_PERFIL_HOGAR"]
sleep(10)  # Esperar a que el consumidor se conecte
coll.drop()

consumer = KafkaConsumer(
    'ine.4583.processed',
    bootstrap_servers='tfm_kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="cargadores_ine"
)

# Agrupar por año
pivot_data = defaultdict(dict)

for msg in consumer:
    doc = msg.value
    year = doc["year"]
    grupo = doc["grupo"]
    pivot_data[year][f"pct_{grupo}"] = doc["pct"]
    pivot_data[year]["year"] = year

    # si ambos grupos están presentes, insertar
    if "pct_total" in pivot_data[year] and "pct_16_29" in pivot_data[year]:
        coll.replace_one({"year": year}, pivot_data[year], upsert=True)