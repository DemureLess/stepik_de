from datetime import datetime, timedelta
import json
import logging
from pymongo import MongoClient


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archived_collection = db["archived_users"]


def archive_users():
    today = datetime.now()
    today_minus_14 = today - timedelta(days=14)
    today_minus_30 = today - timedelta(days=30)

    # Находим пользователей, которые зарегистрировались более 30 дней назад
    old_users = collection.distinct(
        "user_id", {"user_info.registration_date": {"$lt": today_minus_30}}
    )

    # Находим пользователей, которые были активны за последние 14 дней
    active_users = collection.distinct(
        "user_id", {"event_time": {"$gte": today_minus_14}}
    )

    # Пользователи для архивирования: старые, но неактивные 14 дней
    users_to_archive_ids = list(set(old_users) - set(active_users))

    # Получаем полные документы для архивирования
    users_to_archive = list(collection.find({"user_id": {"$in": users_to_archive_ids}}))

    logger.info(f"Найдено {len(users_to_archive)} пользователей для архивирования")

    if users_to_archive:
        # Перемещаем в архивную коллекцию
        archived_collection.insert_many(users_to_archive)

        # Удаляем из основной коллекции
        collection.delete_many({"user_id": {"$in": users_to_archive_ids}})

        # Создаем отчет
        today_str = today.strftime("%Y-%m-%d")
        report = {
            "date": today_str,
            "archived_users_count": len(users_to_archive),
            "archived_user_ids": users_to_archive_ids,
        }

        report_filename = f"{today_str}.json"
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"Архивировано {len(users_to_archive)} пользователей")
        logger.info(f"Отчет сохранен в {report_filename}")

    else:
        logger.info("Нет пользователей для архивирования")


if __name__ == "__main__":
    archive_users()
