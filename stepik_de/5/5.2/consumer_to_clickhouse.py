import json
import logging
import os
from kafka import KafkaConsumer
from clickhouse_driver import Client
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


def get_consumer():
    return KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS")],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="clickhouse_consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def get_clickhouse_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DATABASE"),
    )


def init_clickhouse_table(client):
    # Создаем таблицу в ClickHouse
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS user_logins (
        id UInt32,
        user_id UInt32,
        event_type String,
        event_timestamp DateTime,
        processed_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (user_id, event_timestamp)
    """

    try:
        client.execute(create_table_sql)
        logger.info("Таблица user_logins создана в ClickHouse")
    except Exception as e:
        logger.info(f"Таблица уже существует или ошибка: {e}")


def consume_from_kafka():
    consumer = get_consumer()
    client = get_clickhouse_client()

    # Инициализируем таблицу
    init_clickhouse_table(client)

    try:
        logger.info("Ожидание сообщений из Kafka...")
        for message in consumer:
            try:
                data = message.value
                logger.info(f"Получено сообщение: {data}")

                # Вставляем данные в ClickHouse
                insert_sql = """
                INSERT INTO user_logins (id, user_id, event_type, event_timestamp)
                VALUES (%(id)s, %(user_id)s, %(event_type)s, %(event_timestamp)s)
                """

                # Преобразуем timestamp если он есть
                if data.get("event_timestamp"):
                    from datetime import datetime

                    timestamp = datetime.fromisoformat(
                        data["event_timestamp"].replace("Z", "+00:00")
                    )
                else:
                    timestamp = None

                client.execute(
                    insert_sql,
                    {
                        "id": data["id"],
                        "user_id": data["user_id"],
                        "event_type": data["event_type"],
                        "event_timestamp": timestamp,
                    },
                )

                logger.info(f"Данные сохранены в ClickHouse: ID={data['id']}")

            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("Остановка consumer...")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    finally:
        consumer.close()
        client.disconnect()


if __name__ == "__main__":
    logger.info(
        "Запуск consumer для получения данных из Kafka и сохранения в ClickHouse..."
    )
    consume_from_kafka()
