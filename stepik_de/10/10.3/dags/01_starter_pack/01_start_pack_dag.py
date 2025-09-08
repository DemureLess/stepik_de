from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
import boto3
from botocore.exceptions import ClientError

from utils.utils import load_config


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
    description="Создание подключений для стартового пакета",
    schedule_interval=INTERVAL,
    catchup=False,
    tags=["stand preparation"],
)


def create_minio_buckets(bucket_name: str):
    """Создать бакет в MinIO"""
    from airflow.hooks.base import BaseHook

    # Получаем connection из Airflow
    s3_conn = BaseHook.get_connection("s3_default")

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_conn.extra_dejson.get("endpoint_url"),
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        region_name=s3_conn.extra_dejson.get("region_name"),
    )

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} created successfully")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyExists":
            logging.info(f"Bucket {bucket_name} already exists")
        else:
            logging.error(f"Error creating bucket {bucket_name}: {e}")


def create_kafka_topics(topic_name: str):
    """Создать топик Kafka"""
    import time

    from airflow.hooks.base import BaseHook
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    # Получаем connection из Airflow
    kafka_conn = BaseHook.get_connection("kafka_default")

    # Добавляем логирование
    logging.info(f"Подключение к Kafka: {kafka_conn.host}:{kafka_conn.port}")

    # Попытки подключения с задержкой
    max_retries = 10
    retry_delay = 15

    for attempt in range(max_retries):
        admin_client = None
        try:
            # Пробуем разные варианты подключения
            # Строим список адресов на основе настроек Connection.
            # В приоритете — явный порт из коннекта, затем дефолтный внутренний 29092.
            bootstrap_servers_options = []
            if kafka_conn.host and kafka_conn.port:
                bootstrap_servers_options.append(f"{kafka_conn.host}:{kafka_conn.port}")
            # Добавим стандартный внутренний адрес как запасной вариант
            if f"{kafka_conn.host}:{kafka_conn.port}" != "kafka:29092":
                bootstrap_servers_options.append("kafka:29092")

            for bootstrap_servers in bootstrap_servers_options:
                try:
                    logging.info(f"Попытка подключения к: {bootstrap_servers}")

                    admin_client = KafkaAdminClient(
                        bootstrap_servers=bootstrap_servers,
                        client_id="airflow_admin",
                        request_timeout_ms=60000,
                        connections_max_idle_ms=60000,
                        api_version_auto_timeout_ms=60000,
                        security_protocol="PLAINTEXT",
                    )

                    # Проверяем подключение
                    metadata = admin_client.describe_cluster()
                    logging.info(f"Успешное подключение к Kafka: {bootstrap_servers}")

                    # Создаем топик с параметрами по умолчанию
                    topic = NewTopic(
                        name=topic_name,
                        num_partitions=3,
                        replication_factor=1,
                        topic_configs={
                            "cleanup.policy": "delete",
                            "retention.ms": "604800000",
                        },
                    )

                    try:
                        admin_client.create_topics([topic], validate_only=False)
                        logging.info(
                            f"Топик {topic_name} создан успешно через {bootstrap_servers}"
                        )
                    except TopicAlreadyExistsError:
                        logging.info(
                            f"Топик {topic_name} уже существует (через {bootstrap_servers})"
                        )

                    # Проверяем что топик действительно создался
                    topics = admin_client.list_topics()
                    if topic_name in topics:
                        logging.info(
                            f"Подтверждено: топик {topic_name} существует в списке топиков"
                        )
                    else:
                        logging.warning(
                            f"Топик {topic_name} не найден в списке после создания"
                        )

                    admin_client.close()
                    return

                except Exception as conn_error:
                    logging.warning(
                        f"Не удалось подключиться к {bootstrap_servers}: {conn_error}"
                    )
                    if admin_client:
                        admin_client.close()
                        admin_client = None
                    continue

            # Если все варианты подключения не сработали
            raise Exception("Не удалось подключиться ни к одному из bootstrap серверов")

        except TopicAlreadyExistsError:
            logging.info(f"Топик {topic_name} уже существует")
            if admin_client:
                admin_client.close()
            return
        except Exception as e:
            logging.warning(
                f"Попытка {attempt + 1}/{max_retries}: Ошибка при создании топика {topic_name}: {e}"
            )
            if admin_client:
                admin_client.close()
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                logging.error(
                    f"Не удалось создать топик {topic_name} после {max_retries} попыток"
                )
                raise


def generate_data():
    """Генерировать тестовые данные и загружать их в MinIO"""
    import tempfile
    import shutil
    from airflow.hooks.base import BaseHook

    from utils.data_generator import generate_all_data

    # Создаем временную директорию для генерации данных
    temp_dir = tempfile.mkdtemp()
    data_dir = os.path.join(temp_dir, "data")

    try:
        # Генерируем данные во временную директорию
        generate_all_data(data_dir)

        # Загружаем данные в MinIO используя connection
        s3_conn = BaseHook.get_connection("s3_default")

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_conn.extra_dejson.get("endpoint_url"),
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            region_name=s3_conn.extra_dejson.get("region_name"),
        )

        # Загружаем все файлы из временной директории в бакет raw-data
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    # Создаем ключ для S3 (путь в бакете)
                    relative_path = os.path.relpath(file_path, data_dir)
                    s3_key = f"raw-data/{relative_path}"

                    # Загружаем файл в MinIO
                    s3_client.upload_file(file_path, "raw-data", s3_key)
                    logging.info(f"Загружен файл: {s3_key}")

        logging.info("Все данные успешно загружены в бакет raw-data")

    except Exception as e:
        logging.error(f"Ошибка при генерации данных: {e}")
        raise
    finally:
        # Очищаем временную директорию
        shutil.rmtree(temp_dir)


# Загружаем конфигурацию
config = load_config()


# Создание TaskGroup для топиков Kafka
with TaskGroup(group_id="Kafka_create_topics", dag=dag) as kafka_topics_group:
    kafka_topics = config["mongodb_kafka_migration"]

    kafka_topic_tasks = []

    # Динамически создаем таски для каждого топика
    for topic_name in kafka_topics.values():
        task = PythonOperator(
            task_id=f"create_kafka_topic_{topic_name}",
            python_callable=create_kafka_topics,
            op_kwargs={"topic_name": topic_name},
            dag=dag,
        )
        kafka_topic_tasks.append(task)

# Создание TaskGroup для MinIO и данных
with TaskGroup(group_id="MinIO_setup_and_data", dag=dag) as minio_data_group:
    # Создание тасков для создания бакетов MinIO
    minio_bucket_tasks = []
    buckets = config["minio_buckets"]

    for bucket_name in buckets:
        task = PythonOperator(
            task_id=f"create_minio_bucket_{bucket_name}",
            python_callable=create_minio_buckets,
            op_kwargs={"bucket_name": bucket_name},
            dag=dag,
        )
        minio_bucket_tasks.append(task)

    # Создание таска для генерации данных
generate_data_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

(start >> kafka_topics_group >> minio_data_group >> generate_data_task >> end)
