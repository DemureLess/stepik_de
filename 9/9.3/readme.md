# Парсер books.toscrape.com

Сайт:
- небольшого объема
- не имеет карты сайта по стандартному пути
поэтому.

Решение   - забрать все url, затем фильтровать нужные.
Итоговые значения будут парсить значения буду для url карточек товара.


## Установка и настройка

Клонируйте репозиторий 

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка переменных окружения

Скопируйте `.env.example` в `.env` и заполните ваши данные:

```bash
cp .env.example .env
```

Отредактируйте `.env` файл:

```bash
AWS_ACCESS_KEY_ID=your_actual_access_key
AWS_SECRET_ACCESS_KEY=your_actual_secret_key
AWS_ENDPOINT=https://s3.amazonaws.com
AWS_BUCKET=your_actual_bucket_name
```

Папка crwler содержит:

 - http_client.py - обертка с политикой ретраев
 - collector.py - сборщик ссылок со страниц

 и эта часть может быть переиспользована в других проектах без изменений

 Папка pareser содержит:
 - books_tocrape.py для конкретного сайта books.toscrape.com

Папка utils содержит:
-  s3.py клиент (из прошлых заданий)

Файл pipeline.ipynb - демонстарция работы парсера 
