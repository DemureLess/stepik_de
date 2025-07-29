import psycopg2
import json
import time
import logging
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_producer():
    return KafkaProducer(
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS")],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def send_to_kafka():
    producer = get_producer()
    conn = get_postgres_connection()
    cursor = conn.cursor()

    topic = os.getenv("KAFKA_TOPIC")

    try:
        while True:
            # Получаем записи, которые еще не были отправлены в Kafka
            cursor.execute("""
                SELECT id, user_id, event_type, event_timestamp 
                FROM user_logins 
                WHERE sent_to_kafka = FALSE 
                ORDER BY event_timestamp
                LIMIT 10
            """)

            records = cursor.fetchall()

            if not records:
                logger.info("Нет новых записей для отправки")
                time.sleep(5)
                continue

            for record in records:
                id, user_id, event_type, event_timestamp = record

                # Формируем сообщение
                message = {
                    "id": id,
                    "user_id": user_id,
                    "event_type": event_type,
                    "event_timestamp": event_timestamp.isoformat()
                    if event_timestamp
                    else None,
                }

                # Отправляем в Kafka
                future = producer.send(topic, message)
                future.get(timeout=10)  # Ждем подтверждения отправки

                # Помечаем запись как отправленную
                cursor.execute(
                    """
                    UPDATE user_logins 
                    SET sent_to_kafka = TRUE 
                    WHERE id = %s
                """,
                    (id,),
                )

                logger.info(f"Отправлено в Kafka: {message}")

            conn.commit()
            time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Остановка producer...")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    finally:
        cursor.close()
        conn.close()
        producer.close()


if __name__ == "__main__":
    logger.info("Запуск producer для отправки данных из PostgreSQL в Kafka...")
    send_to_kafka()
