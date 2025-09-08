import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.s3_key_sensor import S3KeySensor


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
INTERVAL = "0 11 * * *"
ds = "{{ ds_nodash }}"

# Определяем путь к папке DAG'а
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
APP_FILE = "07_report_genteration_app.py"
APP_PATH = os.path.join(DAG_FOLDER, APP_FILE)

# Параметры для приложения
INPUT_TABLE = "dws_customer_features_di"
OUTPUT_BUCKET = "out"
OUTPUT_PATH = "customer_features"
FILE_NAME = "analytic_result"

# Получаем соединения
s3_conn = BaseHook.get_connection("s3_default")

# Spark конфигурация
spark_config = {
    "spark.master": "spark://spark:7077",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "2",
    # JAR файлы
    "spark.jars": (
        "/opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar,"
        "/opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-jdbc-0.6.3.jar,"
        "/opt/spark-3.4.2-bin-hadoop3/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark-3.4.2-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.262.jar"
    ),
    # S3/MinIO конфигурация из s3_default connection
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": s3_conn.extra_dejson["endpoint_url"],
    "spark.hadoop.fs.s3a.access.key": s3_conn.login,
    "spark.hadoop.fs.s3a.secret.key": s3_conn.password,
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.region": s3_conn.extra_dejson["region_name"],
    # Дополнительные настройки
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.adaptive.enabled": "true",
}

default_args = {
    "owner": "pavel.romanovskiy",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Генерируем финальный csv из максимальной партиции",
    schedule_interval=INTERVAL,
    catchup=False,
    tags=["export csv"],
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# Сенсор для проверки наличия партиции в S3
check_partition_sensor = S3KeySensor(
    task_id=f"check_partition_{INPUT_TABLE}_exists",
    bucket_name="processed-data",
    bucket_key=f"{INPUT_TABLE}/ds={ds}/*",
    wildcard_match=True,
    aws_conn_id="s3_default",
    timeout=300,
    poke_interval=30,
    dag=dag,
)

spark_task = SparkSubmitOperator(
    task_id=f"spark_{FILE_NAME}_generation",
    application=APP_PATH,
    conn_id="spark_default",
    conf=spark_config,
    application_args=[
        "--input_table",
        INPUT_TABLE,
        "--output_bucket",
        OUTPUT_BUCKET,
        "--output_path",
        OUTPUT_PATH,
        "--file_name",
        FILE_NAME,
    ],
    dag=dag,
)

start >> check_partition_sensor >> spark_task >> end
