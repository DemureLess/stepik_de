import psycopg2
import logging
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def init_database():
    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    cursor = conn.cursor()

    # Создание таблицы user_logins в ее начальном состоянии
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS user_logins (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    cursor.execute(create_table_sql)

    # Добавление поля sent_to_kafka если его нет
    try:
        cursor.execute(
            "ALTER TABLE user_logins ADD COLUMN sent_to_kafka BOOLEAN DEFAULT FALSE;"
        )
        logger.info("Добавлено поле sent_to_kafka")
    except psycopg2.errors.DuplicateColumn:
        logger.info("Поле sent_to_kafka уже существует")

    # Проверяем, есть ли уже данные в таблице
    cursor.execute("SELECT COUNT(*) FROM user_logins")
    count = cursor.fetchone()[0]

    if count == 0:
        # Вставка тестовых данных только если таблица пустая
        test_data = [
            (1, "login"),
            (2, "registration"),
            (1, "purchase"),
            (3, "login"),
            (2, "logout"),
            (1, "logout"),
            (4, "registration"),
            (3, "purchase"),
        ]

        insert_sql = """
        INSERT INTO user_logins (user_id, event_type) 
        VALUES (%s, %s)
        """

        for user_id, event_type in test_data:
            cursor.execute(insert_sql, (user_id, event_type))

        logger.info(f"Добавлено {len(test_data)} тестовых записей")
    else:
        logger.info(
            f"В таблице уже есть {count} записей, пропускаем вставку тестовых данных"
        )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("База данных инициализирована успешно!")


if __name__ == "__main__":
    init_database()
