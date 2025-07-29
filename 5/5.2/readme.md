# Миграция данных из PostgreSQL в ClickHouse при помощи Kafka с защитой от дублей

Этот пайплайн реализует надежную миграцию данных из PostgreSQL в ClickHouse через Apache Kafka с защитой от дублирования записей.

## Архитектура

1. **PostgreSQL** - источник данных с таблицей `user_logins`
2. **Kafka** - промежуточное хранилище сообщений
3. **ClickHouse** - целевая система для аналитики
4. **Producer** - читает данные из PostgreSQL и отправляет в Kafka
5. **Consumer** - получает данные из Kafka и сохраняет в ClickHouse

## Защита от дублей

- В таблице `user_logins` добавлено поле `sent_to_kafka BOOLEAN DEFAULT FALSE`
- Producer отправляет только записи с `sent_to_kafka = FALSE`
- После успешной отправки в Kafka поле обновляется на `TRUE`
- Это гарантирует, что каждая запись будет отправлена только один раз


## Подготовка

Скопируйте .env.example в .env и настройте параметры или задайте свои


`cp .env.example .env`

установка зависимостей

`pip install -r requirements.txt`


## Запуск пайплайна:

1. `docker-compose up -d` - запуск инфраструктуры (PostgreSQL, Kafka, ClickHouse)
2. `python init_database.py` - инициализация таблицы и добавление поля sent_to_kafka
3. `python producer_pg_to_kafka.py` - запуск producer'а
4. `python consumer_to_clickhouse.py` - запуск consumer'а

## Проверка работы

- Producer выводит информацию об отправленных сообщениях
- Consumer выводит информацию о полученных и сохраненных данных
- В ClickHouse можно проверить данные: `SELECT * FROM user_logins ORDER BY event_timestamp`



Бонсус и демо фото:

Подготовка
![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_0.png)

Добавлен UI для Kafka
Доступ через веб инетрфейс

http://localhost:8082/

![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_1.png)

![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_3.png)

![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_4.png)

![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_5.png)


Демонстрация работы продюсера (слева) и консьюмера (справа)

![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_2.png)


Данные в  Clickhouse

![](https://github.com/DemureLess/stepik_de/blob/main/5/5.2/img/i_5_2_6.png)



