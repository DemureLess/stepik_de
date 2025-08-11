import os
import asyncio
import logging
import shutil
from pathlib import Path
import pandas as pd
from watchfiles import awatch
from s3 import AsyncObjectStorage

# Создаем папки
Path("in").mkdir(exist_ok=True)
Path("archive").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Создаем файл логов, если он не существует
log_file = Path("logs/pipeline.log")
log_file.touch(exist_ok=True)

# Настройка логирования для нашего логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Очищаем существующие обработчики, чтобы избежать дублирования
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Создаем форматтер
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Создаем обработчик для файла
file_handler = logging.FileHandler("logs/pipeline.log", mode="a")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Создаем обработчик для консоли
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Добавляем обработчики к логгеру
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Инициализация S3 клиента
storage = AsyncObjectStorage(
    key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint=os.getenv("AWS_ENDPOINT"),
    bucket=os.getenv("AWS_BUCKET"),
)


async def process_csv_file(file_path: Path):
    """Обрабатывает CSV файл: читает, фильтрует, сохраняет во временный файл"""
    try:
        # Читаем CSV файл
        logger.info(f"Читаем CSV файл: {file_path.name}")
        df = pd.read_csv(file_path)
        logger.info(f"Прочитано {len(df)} строк из файла {file_path.name}")

        # Выполняем фильтрацию (пример: удаляем строки с пустыми значениями)
        initial_rows = len(df)
        df_filtered = df.dropna()
        filtered_rows = len(df_filtered)
        logger.info(
            f"После фильтрации осталось {filtered_rows} строк (удалено {initial_rows - filtered_rows})"
        )

        # Сохраняем во временный файл
        temp_file = Path("temp") / f"processed_{file_path.name}"
        Path("temp").mkdir(exist_ok=True)
        df_filtered.to_csv(temp_file, index=False)
        logger.info(f"Обработанный файл сохранен во временный файл: {temp_file}")

        return temp_file

    except Exception as e:
        logger.error(f"Ошибка при обработке CSV файла {file_path.name}: {str(e)}")
        return None


async def upload_logs_to_storage():
    """Загружает логи в хранилище"""
    try:
        log_file = Path("logs/pipeline.log")
        if log_file.exists():
            s3_key = "logs/pipeline.log"
            await storage.send_file(str(log_file), s3_key)
            logger.info(f"Логи загружены в хранилище: {s3_key}")
        else:
            logger.warning("Файл логов не найден")
    except Exception as e:
        logger.error(f"Ошибка при загрузке логов в хранилище: {str(e)}")


async def process_file(file_path: Path):
    """Основной процесс обработки файла"""
    try:
        logger.info(f"Начинаем обработку файла: {file_path.name}")

        # Обрабатываем CSV файл
        temp_file = await process_csv_file(file_path)
        if temp_file is None:
            logger.error(f"Не удалось обработать файл {file_path.name}")
            return

        # Загружаем обработанный файл в S3
        s3_key = f"processed_csv/{file_path.name}"
        await storage.send_file(str(temp_file), s3_key)
        logger.info(f"Обработанный файл {file_path.name} загружен в S3")

        # Проверяем наличие файла в S3
        if await storage.file_exists(s3_key):
            logger.info(f"Файл {file_path.name} успешно записан в S3")

            # Перемещаем исходный файл в архив
            archive_path = Path("archive") / file_path.name
            shutil.move(str(file_path), str(archive_path))
            logger.info(f"Исходный файл {file_path.name} перемещен в архив")

            # Удаляем временный файл
            temp_file.unlink()
            logger.info(f"Временный файл {temp_file.name} удален")

            # Загружаем логи в хранилище
            await upload_logs_to_storage()

        else:
            logger.error(f"Файл {file_path.name} не найден в S3 после загрузки")

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_path.name}: {str(e)}")


async def watch_folder():
    """Отслеживает папку на появление новых файлов"""
    logger.info("Начинаем отслеживать папку in")

    async for changes in awatch("in"):
        for change_type, file_path in changes:
            file_path = Path(file_path)

            if change_type == 1 and file_path.suffix.lower() == ".csv":
                # Проверяем, что файл все еще существует
                if not file_path.exists():
                    logger.debug(f"Файл {file_path.name} уже не существует, пропускаем")
                    continue

                logger.info(f"Обнаружен новый CSV файл: {file_path}")
                await process_file(file_path)

            elif file_path.suffix.lower() != ".csv":
                logger.info(f"Неподдерживаемый тип файла: {file_path.name}")


async def main():
    """Главная функция"""
    try:
        logger.info("Запуск пайплайна обработки CSV файлов")
        await watch_folder()
    except KeyboardInterrupt:
        logger.info("Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
