import os
from datetime import datetime, timedelta

from airflow import DAG
from utils.utils import load_config
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
INTERVAL = None


def load_mapping() -> dict:
    """Загружает маппинг коллекций и топиков из общей конфигурации"""
    data = load_config()
    return data.get("mongodb_kafka_migration", {})


def get_kafka_connection_params() -> dict:
    """Получает параметры подключения к Kafka"""
    kafka_conn = BaseHook.get_connection("kafka_default")
    return {
        "bootstrap_servers": f"{kafka_conn.host}:29092",
        "group_id": "clickhouse_consumer_group",
    }


def drop_clickhouse_table_sql(topic: str) -> str:
    """Создает SQL для удаления таблицы Kafka в ClickHouse"""
    return f"DROP TABLE IF EXISTS kafka_{topic}_queue;"


def create_clickhouse_table_sql(topic: str) -> str:
    """Создает SQL для создания таблицы в ClickHouse"""
    kafka_params = get_kafka_connection_params()

    return f"""
    CREATE TABLE kafka_{topic}_queue (
        payload String
    ) ENGINE = Kafka('{kafka_params["bootstrap_servers"]}', '{topic}', '{kafka_params["group_id"]}', 'JSONAsString')
    SETTINGS kafka_thread_per_consumer = 0, kafka_num_consumers = 1, kafka_skip_broken_messages = 1;
    """


def drop_target_table_sql(collection: str) -> str:
    """Создает SQL для удаления целевой таблицы"""
    return f"DROP TABLE IF EXISTS {collection}_data;"


def create_target_table_sql(collection: str) -> str:
    """Создает SQL для создания целевой таблицы"""
    # Сохраняем полный JSON документ
    return f"""
    CREATE TABLE {collection}_data (
        id String,
        full_data String,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (created_at);
    """


def drop_materialized_view_sql(collection: str) -> str:
    """Создает SQL для удаления материализованного представления"""
    return f"DROP VIEW IF EXISTS {collection}_mv;"


def create_materialized_view_sql(collection: str, topic: str) -> str:
    """Создает SQL для материализованного представления"""
    return f"""
    CREATE MATERIALIZED VIEW {collection}_mv TO {collection}_data AS
    SELECT 
        JSONExtractString(payload, '_id') AS id,
        payload AS full_data
    FROM kafka_{topic}_queue
    WHERE payload != '' AND isValidJSON(payload);
    """


def create_kafka_table_task(topic: str, **context):
    """Создает Kafka таблицу в ClickHouse во время выполнения"""
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    # Получаем SQL для создания таблицы
    sql = create_clickhouse_table_sql(topic)

    # Создаем и выполняем SQLExecuteQueryOperator
    sql_task = SQLExecuteQueryOperator(
        task_id=f"create_kafka_table_{topic}",
        conn_id="clickhouse_default",
        sql=sql,
    )

    return sql_task.execute(context)


default_args = {
    "owner": "pavel.romanovskiy",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 29),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=INTERVAL,
    catchup=False,
    default_args=default_args,
    description="Создает материализованные представления ClickHouse с движком Kafka",
    tags=["stand preparation"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    mapping = load_mapping()  # {collection: topic}
    for collection, topic in mapping.items():
        with TaskGroup(group_id=f"kafka_to_clickhouse_{topic}") as topic_group:
            # Удаление материализованного представления
            drop_mv = SQLExecuteQueryOperator(
                task_id=f"drop_mv_{collection}",
                conn_id="clickhouse_default",
                sql=drop_materialized_view_sql(collection),
            )

            # Удаление Kafka таблицы
            drop_kafka_table = SQLExecuteQueryOperator(
                task_id=f"drop_kafka_table_{topic}",
                conn_id="clickhouse_default",
                sql=drop_clickhouse_table_sql(topic),
            )

            # Удаление целевой таблицы
            drop_target_table = SQLExecuteQueryOperator(
                task_id=f"drop_target_table_{collection}",
                conn_id="clickhouse_default",
                sql=drop_target_table_sql(collection),
            )

            # Создание Kafka таблицы - используем PythonOperator
            create_kafka_table = PythonOperator(
                task_id=f"create_kafka_table_{topic}",
                python_callable=create_kafka_table_task,
                op_kwargs={
                    "topic": topic,
                },
            )

            # Создание целевой таблицы
            create_target_table = SQLExecuteQueryOperator(
                task_id=f"create_target_table_{collection}",
                conn_id="clickhouse_default",
                sql=create_target_table_sql(collection),
            )

            # Создание материализованного представления
            create_mv = SQLExecuteQueryOperator(
                task_id=f"create_mv_{collection}",
                conn_id="clickhouse_default",
                sql=create_materialized_view_sql(collection, topic),
            )

            # Определяем зависимости - сначала все удаления, затем все создания
            drop_mv >> drop_target_table >> drop_kafka_table
            (
                drop_kafka_table
                >> [create_kafka_table, create_target_table]
                >> create_mv
                >> end
            )
