# Описание

1. Используем docker-comopose из [степа 4.1](../4.1/docker-compose.yml)

```
docker-compose up -d

```

2. Переходим в dbeaver (соединение уже должно быть)
[Выполняем из  SQL](result.sql)


![](img/i_4_1_1.png)

3. Принудительно вызываем функцию генерации отчета

```
select export_audit_to_csv();

```

4.Заходим в докер

``` docker exec -it postgres_db bash ```

![](img/i_4_1_2.png)