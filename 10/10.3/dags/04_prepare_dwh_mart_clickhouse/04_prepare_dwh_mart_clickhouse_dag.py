from datetime import datetime, timedelta
import os

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from utils.utils import load_config

DWH_SCHEMA = "dwh"
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
INTERVAL = None

default_args = {
    "owner": "pavel.romanovskiy",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 29),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def read_sql_file(filename):
    """Читает содержимое SQL файла"""
    sql_dir = os.path.join(os.path.dirname(__file__), "sql")
    with open(os.path.join(sql_dir, filename), "r", encoding="utf-8") as f:
        return f.read()


# Загружаем конфигурацию
config = load_config()

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Создает таблицы и материализованные представления в ClickHouse для DWH",
    tags=["stand preparation"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    create_db = SQLExecuteQueryOperator(
        task_id=f"create_database_{DWH_SCHEMA}",
        conn_id="clickhouse_default",
        sql=f"CREATE DATABASE IF NOT EXISTS {DWH_SCHEMA};",
    )

    # Генерируем группы задач для каждой таблицы из конфига
    table_groups = []
    mapping = config.get("mongodb_kafka_migration", {})
    for collection, topic in mapping.items():
        with TaskGroup(group_id=f"dwh_mart_clickhouse_{topic}") as topic_group:
            drop_mv = SQLExecuteQueryOperator(
                task_id=f"drop_mv_{DWH_SCHEMA}.{collection}_parsed_mv",
                conn_id="clickhouse_default",
                sql=f"DROP VIEW IF EXISTS {DWH_SCHEMA}.{collection}_parsed_mv;",
            )
            drop_table = SQLExecuteQueryOperator(
                task_id=f"drop_table_{DWH_SCHEMA}.{collection}_parsed",
                conn_id="clickhouse_default",
                sql=f"DROP TABLE IF EXISTS {DWH_SCHEMA}.{collection}_parsed;",
            )
            create_table = SQLExecuteQueryOperator(
                task_id=f"create_table_{DWH_SCHEMA}.{collection}_parsed",
                conn_id="clickhouse_default",
                sql=read_sql_file(f"{collection}.ddl"),
            )
            create_mv = SQLExecuteQueryOperator(
                task_id=f"create_mv_{DWH_SCHEMA}.{collection}_parsed_mv",
                conn_id="clickhouse_default",
                sql=read_sql_file(f"{collection}_mv.sql"),
            )
            drop_mv >> drop_table >> create_table >> create_mv
        table_groups.append(topic_group)

    for table_group in table_groups:
        start >> create_db >> table_group >> end
