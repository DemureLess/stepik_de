from datetime import datetime, timedelta
import json

from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]

today = datetime.now()
today_minus_14 = today - timedelta(days=14)
today_minus_30 = today - timedelta(days=30)


query = {
    "user_info.registration_date": {"$lt": today_minus_30},
    "event_time": {"$lt": today_minus_14},
}

users_to_archive = list(collection.find(query))

archived_collection = db["archived_users"]

ids_to_remove = []

for user in users_to_archive:
    ids_to_remove.append(user["_id"])

collection.delete_many({"_id": {"$in": ids_to_remove}})

today_str = today.strftime("%Y-%m-%d")
report = {
    "date": today_str,
    "archived_users_count": len(users_to_archive),
    "archived_user_ids": [user["user_id"] for user in users_to_archive],
}

report_filename = f"{today_str}.json"
with open(report_filename, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False)
