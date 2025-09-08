import json
from typing import List, Dict, Any

import pymongo
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3ToMongoOperator(BaseOperator):
    """
    Оператор для транспортировки данных из S3 в MongoDB
    """

    def __init__(
        self,
        s3_conn_id: str,
        s3_bucket: str,
        s3_prefix: str,
        mongo_conn_id: str,
        mongo_db: str,
        mongo_collection: str,
        file_pattern: str = "*.json",
        batch_size: int = 100,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.file_pattern = file_pattern
        self.batch_size = batch_size

    def execute(self, context):
        self.log.info("Начинаем транспортировку данных из S3 в MongoDB")
        self.log.info(f"S3: {self.s3_bucket}/{self.s3_prefix}")
        self.log.info(f"MongoDB: {self.mongo_db}.{self.mongo_collection}")

        try:
            # Подключение к S3 через S3Hook
            s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

            # Подключение к MongoDB через pymongo напрямую
            mongo_conn = BaseHook.get_connection(self.mongo_conn_id)

            # Формируем MongoDB URI на основе полей подключения и extra (auth_source)
            self.log.info(
                f"Используем соединение MongoDB по conn_id: {self.mongo_conn_id}"
            )
            host = mongo_conn.host or "localhost"
            port = mongo_conn.port or 27017
            login = mongo_conn.login or ""
            password = mongo_conn.password or ""
            database = mongo_conn.schema or "admin"
            extra_dejson = {}
            try:
                extra_dejson = json.loads(mongo_conn.extra) if mongo_conn.extra else {}
            except Exception:
                extra_dejson = {}
            auth_source = extra_dejson.get("auth_source", "admin")

            if login and password:
                mongo_uri = f"mongodb://{login}:{password}@{host}:{port}/{database}?authSource={auth_source}"
            else:
                mongo_uri = f"mongodb://{host}:{port}/{database}"

            self.log.info("MongoDB URI сформирован из Connection полей")

            client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)

            # Проверяем подключение
            client.admin.command("ping")

            # Оптимизируем настройки MongoDB
            client.max_pool_size = 10
            client.min_pool_size = 1

            db = client[self.mongo_db]
            collection = db[self.mongo_collection]

            # Получение списка всех файлов в папке
            files = self._list_s3_files(s3_hook)

            if not files:
                self.log.warning(f"Файлы не найдены в {self.s3_prefix}")
                return 0

            self.log.info(f"Найдено {len(files)} файлов для обработки")

            total_documents = 0

            # Обработка файлов батчами для избежания таймаутов
            for i in range(0, len(files), self.batch_size):
                batch_files = files[i : i + self.batch_size]
                self.log.info(
                    f"Обрабатываем батч {i // self.batch_size + 1}: {len(batch_files)} файлов"
                )

                batch_documents = []

                for file_key in batch_files:
                    try:
                        documents = self._process_s3_file(s3_hook, file_key, context)
                        if documents:
                            batch_documents.extend(documents)

                    except Exception as e:
                        self.log.error(f"Ошибка при обработке файла {file_key}: {e}")
                        continue

                # Вставляем батч документов
                if batch_documents:
                    try:
                        inserted_count = self._insert_batch_to_mongodb(
                            collection, batch_documents
                        )
                        total_documents += inserted_count
                        self.log.info(
                            f"Батч {i // self.batch_size + 1}: вставлено {inserted_count} документов"
                        )

                    except Exception as e:
                        self.log.error(
                            f"Ошибка при вставке батча {i // self.batch_size + 1}: {e}"
                        )
                        continue

            self.log.info(f"Всего обработано документов: {total_documents}")
            return total_documents

        except Exception as e:
            self.log.error(f"Ошибка при транспортировке данных: {e}")
            raise

    def _list_s3_files(self, s3_hook) -> List[str]:
        """Получить список файлов из S3 по префиксу"""
        try:
            keys = s3_hook.list_keys(
                bucket_name=self.s3_bucket, prefix=self.s3_prefix, delimiter="/"
            )

            if not keys:
                return []

            json_files = [key for key in keys if key.endswith(".json")]
            return json_files

        except Exception as e:
            self.log.error(f"Ошибка при получении списка файлов: {e}")
            return []

    def _process_s3_file(
        self, s3_hook, file_key: str, context: Dict[str, Any]
    ) -> List[Dict]:
        """Обработать один файл из S3"""
        try:
            file_content = s3_hook.read_key(file_key, self.s3_bucket)
            data = json.loads(file_content)

            if isinstance(data, list):
                documents = data
            else:
                documents = [data]

            return documents

        except Exception as e:
            self.log.error(f"Ошибка при обработке файла {file_key}: {e}")
            return []

    def _insert_batch_to_mongodb(self, collection, documents: List[Dict]) -> int:
        """Вставить батч документов в MongoDB"""
        if not documents:
            return 0

        # Обычная вставка
        if len(documents) == 1:
            collection.insert_one(documents[0])
            return 1
        else:
            result = collection.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
