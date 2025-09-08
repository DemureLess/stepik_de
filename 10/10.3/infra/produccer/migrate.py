#!/usr/bin/env python3
"""
Простой мигратор: MongoDB -> Kafka

Параметры через ENV:
  - MONGO_URI или MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD, MONGO_AUTH_SOURCE
  - KAFKA_BOOTSTRAP_SERVERS
  - COLLECTION
  - TOPIC
  - BATCH_SIZE (опц., по умолчанию 1000)
  - QUERY_FILTER (опц., JSON-строка)

Скрипт одноразовый: старт -> миграция -> выход с кодом 0/1
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
import re
import hashlib

from pymongo import MongoClient
from bson import ObjectId
from confluent_kafka import Producer


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("mongo-kafka-migrator")


def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name, default)
    return value


def build_mongo_uri() -> str:
    direct = getenv("MONGO_URI")
    if direct:
        return direct

    host = getenv("MONGO_HOST", "localhost")
    port = getenv("MONGO_PORT", "27017")
    db = getenv("MONGO_DB")
    user = getenv("MONGO_USER")
    pwd = getenv("MONGO_PASSWORD")
    auth_src = getenv("MONGO_AUTH_SOURCE", "admin")

    if not db:
        raise ValueError("MONGO_DB обязателен")

    if user and pwd:
        return f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource={auth_src}"
    return f"mongodb://{host}:{port}/{db}"


def to_json(doc: Dict[str, Any]) -> str:
    def default(o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)

    return json.dumps(doc, default=default, ensure_ascii=False)


def main() -> int:
    try:
        mongo_uri = build_mongo_uri()
        kafka_servers = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        collection = getenv("COLLECTION")
        topic = getenv("TOPIC")
        batch_size = int(getenv("BATCH_SIZE", "1000"))
        query_filter_raw = getenv("QUERY_FILTER")

        if not collection or not topic:
            raise ValueError("COLLECTION и TOPIC обязательны")

        query_filter: Optional[Dict[str, Any]] = None
        if query_filter_raw:
            query_filter = json.loads(query_filter_raw)

        log.info(f"MongoDB: {mongo_uri}")
        log.info(f"Kafka: {kafka_servers}")
        log.info(f"Коллекция: {collection}, Топик: {topic}")

        mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo.admin.command("ping")
        db_obj = mongo.get_default_database()
        db_name = db_obj.name if db_obj is not None else None
        if not db_name:
            # В URI может не быть дефолтной БД, попробуем взять из строки
            db_name = os.getenv("MONGO_DB")
        if not db_name:
            raise ValueError("Не удалось определить имя базы данных")

        coll = mongo[db_name][collection]

        producer = Producer(
            {
                "bootstrap.servers": kafka_servers,
                "linger.ms": 10,
            }
        )

        cursor = coll.find(query_filter or {}).batch_size(batch_size)
        sent = 0

        def normalize_phone(raw: str) -> str:
            if raw is None:
                return ""
            if not isinstance(raw, str):
                raw = str(raw)
            digits = re.sub(r"[^0-9]", "", raw)
            if digits.startswith("8") and len(digits) == 11:
                digits = "7" + digits[1:]
            if len(digits) == 10:
                digits = "7" + digits
            if len(digits) == 11 and digits.startswith("7"):
                return "+" + digits
            return "+" + digits if digits and not raw.startswith("+") else raw

        def md5_hex(value: str) -> str:
            if value is None:
                value = ""
            if not isinstance(value, str):
                value = str(value)
            return hashlib.md5(value.encode("utf-8")).hexdigest()

        def transform(obj: Any) -> Any:
            if isinstance(obj, dict):
                new_obj: Dict[str, Any] = {}
                for k, v in obj.items():
                    kl = k.lower()
                    if "phone" in kl:
                        normalized = normalize_phone(v)
                        new_obj[k] = md5_hex(normalized)
                    elif "email" in kl:
                        lowered = v.lower() if isinstance(v, str) else str(v or "")
                        new_obj[k] = md5_hex(lowered)
                    else:
                        new_obj[k] = transform(v)
                return new_obj
            if isinstance(obj, list):
                return [transform(x) for x in obj]
            return obj

        for doc in cursor:
            key = str(doc.get("_id", ""))
            doc = transform(doc)
            value = to_json(doc)
            producer.produce(topic=topic, key=key, value=value)

            sent += 1
            if sent % batch_size == 0:
                producer.poll(0)
                log.info(f"Отправлено {sent} документов")

        producer.flush()
        log.info(f"Готово. Отправлено {sent} документов")
        return 0

    except Exception as e:
        log.error(f"Ошибка миграции: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
