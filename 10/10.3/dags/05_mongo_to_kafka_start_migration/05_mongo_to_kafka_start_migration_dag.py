import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from utils.utils import load_config
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
INTERVAL = None


def load_mapping() -> dict:
    data = load_config()
    return data.get("mongodb_kafka_migration", {})


def build_env_for_pair(collection: str, topic: str) -> dict:
    """Получаем соединения внутри функции, а не на уровне модуля"""
    try:
        # Mongo connection
        mongo_conn = BaseHook.get_connection("mongodb_default")
        # Kafka connection
        kafka_conn = BaseHook.get_connection("kafka_default")
    except Exception as e:
        raise Exception(f"Не удалось получить соединения: {e}")

    env = {
        "MONGO_HOST": mongo_conn.host or "localhost",
        "MONGO_PORT": str(mongo_conn.port or 27017),
        "MONGO_DB": (mongo_conn.schema or "test_db"),
        "MONGO_USER": (mongo_conn.login or ""),
        "MONGO_PASSWORD": (mongo_conn.password or ""),
        "MONGO_AUTH_SOURCE": json.loads(mongo_conn.extra or "{}").get(
            "auth_source", "admin"
        ),
        "KAFKA_BOOTSTRAP_SERVERS": (kafka_conn.host or "localhost")
        + (f":{kafka_conn.port}" if kafka_conn.port else ""),
        "COLLECTION": collection,
        "TOPIC": topic,
        "BATCH_SIZE": str(Variable.get("MIGRATION_BATCH_SIZE", default_var="1000")),
    }

    # QUERY_FILTER как JSON-строка (опционально из Variable)
    query_filter = Variable.get("MIGRATION_QUERY_FILTER", default_var=None)
    if query_filter:
        env["QUERY_FILTER"] = query_filter

    return env


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
    description="Запускает миграцию Mongo -> Kafka через DockerOperator для набора коллекций",
    tags=["migration"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    mapping = load_mapping()

    for collection, topic in mapping.items():
        env = build_env_for_pair(collection, topic)
        migrate_task = DockerOperator(
            task_id=f"migrate__{collection}_to_{topic}",
            image="simple-migrator:latest",
            auto_remove=True,
            command=None,
            environment=env,
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_network",
            mount_tmp_dir=False,
        )

        start >> migrate_task >> end
