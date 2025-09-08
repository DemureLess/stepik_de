from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from utils.s3_to_mongo_operator import S3ToMongoOperator
from utils.utils import load_config


# оставляем существующую функцию load_config из utils


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

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Загрузка данных из S3 в MongoDB",
    schedule_interval=INTERVAL,
    catchup=False,
    tags=["stand preparation"],
)


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# Получаем папки из конфигурации
config = load_config()
mongodb_kafka_migration = config["mongodb_kafka_migration"]
bucket_name = "raw-data"
mongo_db = "retail_data"


# Создаем TaskGroup для каждой папки
for folder_name, colletion_name in mongodb_kafka_migration.items():
    with TaskGroup(group_id=f"s3_to_mongo_{folder_name}", dag=dag) as transfer_group:
        # Sensor для проверки наличия файлов в S3
        check_s3_files = S3KeySensor(
            task_id=f"check_s3_ready_{folder_name}",
            bucket_key=f"{bucket_name}/{folder_name}/*.json",
            bucket_name=f"{bucket_name}",
            wildcard_match=True,
            aws_conn_id="s3_default",
            timeout=1800,
            poke_interval=30,
            mode="poke",
            soft_fail=False,
            dag=dag,
        )

        # Оператор для загрузки данных
        load_data = S3ToMongoOperator(
            task_id=f"load_{folder_name}_to_mongo",
            s3_conn_id="s3_default",
            s3_bucket=f"{bucket_name}",
            s3_prefix=f"{bucket_name}/{folder_name}/",
            mongo_conn_id="mongodb_default",
            mongo_db=f"{mongo_db}",
            mongo_collection=colletion_name,
            dag=dag,
        )

        start >> check_s3_files >> load_data >> end
