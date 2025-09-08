import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def spark_app(input_table, output_bucket, output_path, file_name):
    TASK_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

    spark = SparkSession.builder.appName(TASK_NAME).getOrCreate()

    # Читаем данные из S3/MinIO parquet файлов
    input_path = f"s3a://processed-data/{input_table}/"

    try:
        # Читаем все партиции и берем максимальную
        df = spark.read.parquet(input_path)
        max_ds = df.select(F.max("ds")).collect()[0][0]

        result_df = df.filter(F.col("ds") == max_ds)

        # Пишем во временную директорию, затем переносим в целевую
        output_dir = f"s3a://{output_bucket}/{output_path}/"
        temp_output_dir = f"{output_dir}__tmp__/"

        # FS (инициализация)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(output_dir), hadoop_conf
        )
        out_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_dir)
        temp_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_output_dir)

        # Полная очистка целевой и временной директории
        if fs.exists(out_path):
            fs.delete(out_path, True)
        if fs.exists(temp_path):
            fs.delete(temp_path, True)

        # Пишем во временную директорию один файл (part-*.csv), без _SUCCESS
        result_df.coalesce(1).write.mode("overwrite").option("header", "true").option(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        ).csv(temp_output_dir)

        # Ищем part-*.csv в temp и переносим под заданным именем
        csv_file = None
        for status in fs.listStatus(temp_path):
            name = status.getPath().getName()
            if name.startswith("part-") and name.endswith(".csv"):
                csv_file = status.getPath()
                break

        if csv_file is None:
            raise RuntimeError("Temporary CSV part file was not produced")

        final_name = (
            file_name if str(file_name).endswith(".csv") else f"{file_name}.csv"
        )
        target_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
            f"{output_dir}{final_name}"
        )
        fs.rename(csv_file, target_path)

        # Удаляем временную директорию
        fs.delete(temp_path, True)

    except Exception:
        raise

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Входная таблица")
    parser.add_argument("--output_bucket", required=True, help="Выходной бакет")
    parser.add_argument("--output_path", required=True, help="Выходной путь")
    parser.add_argument("--file_name", required=True, help="Имя файла")
    args = parser.parse_args()

    spark_app(
        args.input_table,
        args.output_bucket,
        args.output_path,
        args.file_name,
    )
