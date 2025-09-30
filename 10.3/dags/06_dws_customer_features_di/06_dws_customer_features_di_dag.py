import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Формируем название дага из названия файла
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

INTERVAL = "0 10 * * *"
DS = "{{ ds_nodash }}"

# Определяем путь к папке DAG'а
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
APP_FILE = "06_dws_customer_features_di_app.py"
APP_PATH = os.path.join(DAG_FOLDER, APP_FILE)

# Параметры для приложения
TABLE_IN = "dwh.purchases_parsed"
TABLE_OUT = "dws_customer_features_di"
BUCKET_NAME = "processed-data"


clickhouse_conn = BaseHook.get_connection("clickhouse_default")
s3_conn = BaseHook.get_connection("s3_default")

spark_config = {
    "spark.master": "spark://spark:7077",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.executor.instances": "1",
    # JAR для подключения к s3/clickhouse
    "spark.jars": (
        "/opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar,"
        "/opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-jdbc-0.6.3.jar,"
        "/opt/spark-3.4.2-bin-hadoop3/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark-3.4.2-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.262.jar"
    ),
    # ClickHouse конфигурация
    "spark.sql.catalog.clickhouse": "com.clickhouse.spark.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host": clickhouse_conn.host,
    "spark.sql.catalog.clickhouse.port": str(clickhouse_conn.port),
    "spark.sql.catalog.clickhouse.user": clickhouse_conn.login,
    "spark.sql.catalog.clickhouse.password": clickhouse_conn.password,
    "spark.sql.catalog.clickhouse.database": clickhouse_conn.schema or "default",
    "spark.sql.catalog.clickhouse.protocol": "http",
    "spark.sql.catalog.clickhouse.http.version": "HTTP_1_1",
    # MinIO конфигурация
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
    "spark.sql.execution.arrow.pyspark.enabled": "false",
}


default_args = {
    "owner": "pavel.romanovskiy",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 28),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="расчет признаков клиентов",
    schedule_interval=INTERVAL,
    catchup=True,
    max_active_tasks=2,
    tags=["dwh transformation"],
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

spark_task = SparkSubmitOperator(
    task_id="spark_customer_features_di",
    application=APP_PATH,
    conn_id="spark_default",
    deploy_mode="client",
    conf=spark_config,
    application_args=[
        "--table_in",
        TABLE_IN,
        "--table_out",
        TABLE_OUT,
        "--ds",
        DS,
        "--bucket_name",
        BUCKET_NAME,
    ],
    dag=dag,
)

start >> spark_task >> end
