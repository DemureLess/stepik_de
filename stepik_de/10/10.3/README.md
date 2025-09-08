# Итоговый проект 

Задача:
1. 45 JSON файлов, описывающих каждый магазин. Структура - Выше.
2. Необходимо создать не менее 20 JSON файлов, которые будут описывать товары - структура выше.
3. Необходимо создать минимум 1 покупателя в каждом магазине. Структура - Выше.
4. Не менее 200 покупок от различных покупателей в разных магазинах. Структура выше.

5. Добавить все JSON файлы в NoSQL хранилище (Docker). 

6. Uеобходимо при помощи Kafka загрузить эти данные в RAW (сырое) хранилище. 

7. Персональная информация (телефон и почта) должны быть зашифрованы любым удобным способом до загрузки их в Clickhouse.

8.Постройте дашборд в графане на основе которого можно будет сделать вывод о том, что количество покупок действительно 200, а магазинов 45.

9. В случае превышения > 50 % дубликатов в исходных таблицах - алертить в телеграмм при помощи Grafana.

10. Создать витрину в clickhouse на SQL, приложить SQL скрипт, который выполняет очистку данных и делает это при помощи MV.

11. Построить ETL процесс на PySpark (локально). 

12. Сформировать файл csv вида и положить его в S3


## Решение

### Подготовка
Для решения задачи нам понадобится набор сервисов: Airflow, небольшой PySpark кластер, Jupyter Lab, ClickHouse, Grafana, Kafka, MinIO S3, MongoDB, Traefik (сделаем из него прокси чтобы обращаться к сервисам по именам).

Для удобства локальной разработки и использования везде (кроме S3) отключена авторизация.

В Airflow необходимые коннекшены для баз данных заводим через Docker Compose.

Запускать сервисы будем через Docker Compose, разработка и проверка выполнена только на M1 процессорах.

### Инструкция по запуску
1. Запускаем Docker на локальном ПК
2. Клонируем репозиторий `git clone`
3. В директории 10.3 запускаем `docker compose up -d`
4.
![](https://github.com/DemureLess/stepik_de/blob/main/10/10.3/img_10.3.1.jpg)

 Стартовая страница (http://localhost/)
- [airflow.localhost](http://airflow.localhost/)
- [kafka.localhost](http://kafka.localhost/)
- [clickhouse.localhost](http://clickhouse.localhost/)
- [minio-console.localhost](http://minio-console.localhost)
- [spark.localhost](http://spark.localhost/)
- [jupyter.localhost](http://jupyter.localhost/)
- [grafana.localhost](http://grafana.localhost/)



В демонстрационных целях, а также чтобы иметь возможность отслеживать/перезапускать, предложено решение выполнить все задачи на Airflow.
Данные бакетов - генерируются из config файла.

![](https://github.com/DemureLess/stepik_de/blob/main/10/10.3/img_10.3.2.png)

### Даги
1. **01_start_pack_dag** - выполняет создание Kafka топиков, S3 бакетов, генерацию тестовых данных

2. **02_s3_to_mongo_transport_dag** - загружает данные из S3 в MongoDB. 

3. **03_prepare_clickhouse_for_kafka_migration_dag** - настройка таблиц и view ClickHouse для приема данных из Kafka

4. **04_prepare_dwh_mart_clickhouse_dag** - готовим нашу целевую базу данных, настраиваем view и таблицы (отсутствие дубликатов)

5. **05_mongo_to_kafka_start_migration_dag** - сама миграция. В продюсере происходит обработка полей телефоны/электронная почта и шифрование. Чтобы не изобретать сервис миграции принято решение продюсер упаковать в контейнер и запускать его через Airflow

6. **06_dws_customer_features_di_dag** - ETL на PySpark. Выполняется каждый день в 10:00 по UTC и данные пишет в партиционированную таблицу с хранением в S3

7. **07_report_generation_dag** - читаем максимальную партицию Spark'ом из S3 и пишем результирующий CSV

### Grafana
При развертывании данные для подключения БД, дашборда и алертинга берутся из папки grafana. Алерты настроены

![](https://github.com/DemureLess/stepik_de/blob/main/10/10.3/img_10.3.3.png)

![](https://github.com/DemureLess/stepik_de/blob/main/10/10.3/img_10.3.4.png)


### Jupyter Lab
Предоставлены стартовые ноутбуки с вариантами подключения к бд, pyspark, s3.


### Структура проекта

```
10.3/
├─ docker-compose.yml                  # Локальная инфраструктура (Airflow, Spark, MinIO, ClickHouse, Kafka, Grafana)
├─ airflow_config/                     # Конфиги для настроки Airflow
│  ├─ airflow.cfg
│  └─ webserver_config.py
├─ dags/                               # DAG'и и приложения
│  ├─ 01_starter_pack/
│  ├─ 02_s3_to_mongo_transport/
│  ├─ 03_prepare_clickhouse_for_kafka_migration/
│  ├─ 04_prepare_dwh_mart_clickhouse/
│  │  └─sql/                           # sql/ddl для вью и таблиц
│  ├─ 05_mongo_to_kafka_start_migration/        
│  ├─ 06_dws_customer_features_di/
│  │  ├─ 06_dws_customer_features_di_dag.py
│  │  └─ 06_dws_customer_features_di_app.py
│  └─ 07_report_customers_features_df/
│     ├─ 07_report_genteration_dag.py
│     └─ 07_report_genteration_app.py  # 
├─ grafana/                            # алерты, источники)
│  └─ provisioning/
│     ├─ dashboards/
│     ├─ datasources/
│     └─ alerting/
│        ├─ alerts.yml                 # Правила алёртов (receiver "911")
│        └─ contactpoints.yml          # Contact point Telegram "911"
│
├─ infra/                              # Dockerfile'ы  сервисов
│  ├─ airflow/
│  ├─ jupyter/
│  └─ start_panel/
└── notebooks/                          # Jupyter ноутбуки
```


Разработано Павлом Романовским