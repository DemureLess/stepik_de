-- 1. Создание таблиц (если еще не созданы)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- 2. Функция логирования
CREATE OR REPLACE FUNCTION audit_user_changes() 
RETURNS TRIGGER AS $$
DECLARE
BEGIN
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit (
            user_id,
            field_changed,
            old_value,
            new_value,
            changed_by
        ) VALUES (
            OLD.id,
            'name',
            OLD.name,
            NEW.name,
            current_user
        );
    END IF;

    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit (
            user_id,
            field_changed,
            old_value,
            new_value,
            changed_by
        ) VALUES (
            OLD.id,
            'email',
            OLD.email,
            NEW.email,
            current_user
        );
    END IF;

    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit (
            user_id,
            field_changed,
            old_value,
            new_value,
            changed_by
        ) VALUES (
            OLD.id,
            'role',
            OLD.role,
            NEW.role,
            current_user
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Триггер для таблицы user
CREATE TRIGGER users_audit_trigger
    AFTER UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION audit_user_changes();

-- 4. Наполняем users тестовыми данными
INSERT INTO users (name, email, role)
VALUES 
    ('pavel', 'pavel@pavel.com', 'user'),
    ('Artem', 'artem@novadata.com', 'user'),
    ('Anastasia', 'anastasia@novadata.com', 'user');

-- 5. Апдейтим изменения для пользователя 1
UPDATE users
SET 
    name = 'Pavel',
    email = 'pavel@pavel.ru',
    role = 'admin'
WHERE id = 1;

-- 6. Функция экспорта данных
CREATE OR REPLACE FUNCTION export_audit_to_csv()
RETURNS void AS $outer$
DECLARE
    path TEXT := '/tmp/users_audit_export_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';
BEGIN
    EXECUTE format(
        $inner$
        COPY (
            SELECT 
                user_id,
                field_changed,
                old_value,
                new_value,
                changed_by,
                changed_at
            FROM users_audit
            WHERE changed_at >= NOW() - INTERVAL '1 day'
            ORDER BY changed_at
        ) TO '%s' WITH CSV HEADER
        $inner$, path
    );
END;
$outer$ LANGUAGE plpgsql;

-- 7. Установка расширения pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 8. Настраиваем cron
SELECT cron.schedule(
    job_name := 'daily_audit_export',
    schedule := '0 3 * * *',
    command := $$SELECT export_audit_to_csv();$$
);
