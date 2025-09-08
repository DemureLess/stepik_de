import os
import json


def load_config():
    """Загрузить конфигурацию из JSON файла"""
    # Определяем путь к конфигурации относительно текущего файла
    config_path = os.path.join(
        os.path.dirname(__file__), "config", "connections_config.json"
    )

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Конфигурационный файл не найден по пути: {config_path}"
        )

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
