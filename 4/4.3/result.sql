-- 1. Сырые логи -  (храним 30 дней)
DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- 2. Агрегированная таблица (храним агрегаты 180 дней)
DROP TABLE IF EXISTS user_events_agg;

CREATE TABLE user_events_agg
(
    event_date Date,
    event_type String,
    uniq_users AggregateFunction(uniq, UInt32),
    total_points_spent AggregateFunction(sum, UInt64),
    actions_count AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree()
PARTITION BY event_date
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- 3. Материализованное представление для автоматического обновления агрегатов
DROP TABLE IF EXISTS user_events_mv;

CREATE MATERIALIZED VIEW user_events_mv
TO user_events_agg
AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS uniq_users,
    sumState(points_spent) AS total_points_spent,
    countState() AS actions_count
FROM user_events
GROUP BY event_date, event_type;

-- 4. Пример аналитического запроса по дням (через merge-функции)
SELECT
    event_date,
    event_type,
    uniqMerge(uniq_users) AS unique_users,
    sumMerge(total_points_spent) AS total_spent,
    countMerge(actions_count) AS total_actions
FROM user_events_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;


--5 Считаем Ретешен 7 дней для каждогй когорты, в данном случае когорта 1 день

-- приводжу даты к нужному виду - поутно убираю дубликаты
WITH traffic AS (
    SELECT DISTINCT 
        toDate(event_time) AS ds, 
        user_id
    FROM user_events a
),

-- создаю таблицу с когортами и датами посещения;
-- в другой базе данных я бы использовал условие в секции ON ds_cohort <= ds_visit
-- но в клие я обойду это через секуию where

retention_prep AS (
    SELECT 
        user_id, 
        a.ds AS ds_cohort, 
        b.ds AS ds_visit,
        b.ds - a.ds AS diff
    FROM traffic a 
    LEFT JOIN traffic b
    ON a.user_id = b.user_id 
    ORDER BY user_id, ds_cohort
)

SELECT 
    ds_cohort, 
    COUNT(DISTINCT user_id) AS total_users_day_0, 
    COUNT(DISTINCT IF(diff > 0 AND diff <= 7, user_id, NULL)) AS returned_in_7_days,
    ROUND(COUNT(DISTINCT IF(diff > 0 AND diff <= 7, user_id, NULL)) / COUNT(DISTINCT user_id) * 100, 2) AS retention_7d_percent
FROM retention_prep
-- так как для не эквивалентных данных клику нужно делать ASOF JOIN можно обойти это в секции where
WHERE ds_cohort <= ds_visit
GROUP BY ds_cohort
ORDER BY ds_cohort;

