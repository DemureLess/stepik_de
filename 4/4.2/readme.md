# Описание

1. Используем docker-comopose из [степа 4.1](../4.1/docker-compose.yml)

```
docker-compose up -d

```

2. Переходим в dbeaver (соединение уже должно быть)
[Выполняем из  SQL](result.sql)


![](https://github.com/DemureLess/stepik_de/blob/main/4/4.2/img/i_4_2_1.png)

3. Принудительно вызываем функцию генерации отчета

```
select export_audit_to_csv();

```

4.Заходим в докер

``` docker exec -it postgres_db bash ```

![](https://github.com/DemureLess/stepik_de/blob/main/4/4.2/img/i_4_2_2.png)


5. Проверяем состояние Cron

``` select * from cron.job ```

![](https://github.com/DemureLess/stepik_de/blob/main/4/4.2/img/i_4_2_3.png)
