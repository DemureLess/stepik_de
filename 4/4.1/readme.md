# Учебный образ Clickhouse + Postgres

```
docker-compose up --build -d

```

Затем
```
python main.py

```

В консоли

```

(base) pavel.romanovsky@B 4.1 % python main.py
PostgreSQL: [(1, 'PAVEL', 'pavel@pavel.ru', 'admin', datetime.datetime(2025, 7, 17, 14, 2, 9, 805896)), (2, 'Alice', None, None, datetime.datetime(2025, 7, 17, 12, 27, 4, 754778))]
ClickHouse: [(1, 'Bob'), (1, 'Bob'), (1, 'Bob'), (1, 'Bob')]
MongoDB: [{'name': 'Charlie'}, {'name': 'Charlie'}, {'name': 'Charlie'}, {'name': 'Charlie'}]

```
