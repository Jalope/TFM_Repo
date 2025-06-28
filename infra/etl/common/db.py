# etl/common/db.py
import os
from urllib.parse import quote_plus
from pymongo import MongoClient

def get_db():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("MONGO_URI no está definido")
    client = MongoClient(uri)
    return client["tfm_db"]