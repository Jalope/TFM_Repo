# etl/Eurostat/gov_10a_exp/extract_gov10a_exp.py

import requests
import pandas as pd
from kafka import KafkaProducer
import json

URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/gov_10a_exp/1.0/*.*.*.*.*.*?c[freq]=A&c[unit]=PC_GDP&c[sector]=S13&c[cofog99]=GF06&c[na_item]=TE&c[geo]=EU27_2020,EA20,EA19&c[TIME_PERIOD]=2023,2022,2021,2020,2019,2018,2017,2016,2015,2014,2013,2012,2011,2010,2009,2008,2007,2006,2005,2004,2003,2002,2001,2000,1999,1998,1997,1996,1995,1994,1993,1992,1991,1990&compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name"

# Descargar y cargar el CSV comprimido directamente
df_raw = requests.get(URL)
df_raw.raise_for_status()
df = pd.read_csv(URL, sep=",", compression="gzip", na_values=[':'], dtype=str)

# Limpiar y transformar los datos
df = df[['geo', 'TIME_PERIOD', 'OBS_VALUE']].copy()
df.rename(columns={'TIME_PERIOD': 'year', 'OBS_VALUE': 'pct_gdp'}, inplace=True)
df.dropna(inplace=True)

df['year'] = df['year'].astype(int)
df['pct_gdp'] = pd.to_numeric(df['pct_gdp'], errors='coerce')
df['geo'] = df['geo'].astype(str)

# Enviar a Kafka topic
producer = KafkaProducer(
    bootstrap_servers="tfm_kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for _, row in df.iterrows():
    doc = {
        "geo": row["geo"],
        "year": int(row["year"]),
        "pct_gdp": float(row["pct_gdp"])
    }
    producer.send("eurostat.gov10a.raw", value=doc)

producer.flush()
print("Datos enviados a Kafka topic: eurostat.gov10a.raw")