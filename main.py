import asyncio
import aiohttp
import json
import random
from custom_logger import logger
from datetime import datetime
from pathlib import Path
from openpyxl import Workbook
from config import DATA_DIR, BASE_URL, LINK_FIELDS

DATA_DIR.mkdir(exist_ok=True)


async def fetch_json(session: aiohttp.ClientSession, url: str):
    """Отправка ассинхронного GET запроса
     Возвращает json -> dict или None"""
    async with session.get(url) as resp:
        if resp.status != 200:
            return None
        return await resp.json()


def save_to_txt(file_path: Path, entity_data: dict):
    """
    Создаем или перезаписываем существующий файл txt
    Записываем все поля из сущности рест с ссылками в формате json
    синхронная запись
    """
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(entity_data, f, indent=4, ensure_ascii=False)
        f.write("\n\nUpdated at: " + datetime.now().strftime("%d.%m.%Y %H:%M:%S"))


def convert_excel_value(value):
    """
    Вспомогательная функция для преобразования типов для xlsx
    """

    if value is None:
        return None

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, str):
        if value.isdigit():
            return int(value)

        try:
            return float(value)
        except ValueError:
            pass

        return value

    return str(value)


def save_to_xlsx(file_path: Path, entity_data: dict):
    """
    Создаем или перезаписываем существующий файл xlsx
    Записываем все поля из сущности рест без ссылок и с приведенными типами
    синхронная запись
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Data"

    ws.append(["Field", "Value"])

    for key, value in entity_data.items():

        if key in LINK_FIELDS:
            continue

        excel_value = convert_excel_value(value)
        ws.append([key, excel_value])

    ws.append([])
    ws.append(["Updated at:     ", datetime.now().strftime("%d.%m.%Y %H:%M:%S")])

    wb.save(file_path)


async def process_entity(session, entity_type: str, id_min: int, id_max: int):
    """
    Основной процесс обработки:
    1) получение http ответа по url
    2) запись в файл txt или xlsx
    """
    random_id = random.randint(id_min, id_max)
    url = f"{BASE_URL}/{entity_type}/{random_id}/"

    data = await fetch_json(session, url)
    if not data:
        logger.error(f"[{entity_type}] id={random_id} 404 Not Found")
        return

    file_txt = DATA_DIR / f"{entity_type}.txt"
    file_xlsx = DATA_DIR / f"{entity_type}.xlsx"

    # Синхронная запись в файлы
    save_to_txt(file_txt, data)
    save_to_xlsx(file_xlsx, data)

    # Асинхронная запись в файлы c использованием to_thread(позволяет выполнить синхронную запись в отдельном потоке)
    # await asyncio.to_thread(save_to_txt, file_txt, data)
    # await asyncio.to_thread(save_to_xlsx, file_xlsx, data)

    logger.info(f"[{entity_type}] updated id={random_id}")


async def people_worker(session, max_requests: int | None):
    """Worker(ассинхронная ф-я в цикле) для /people: выполняет запросы до max_requests или бесконечно"""
    count = 0
    while True:
        await process_entity(session, "people", 1, 100)
        count += 1

        if max_requests is not None and count >= max_requests:
            logger.info(f"[people] completed fixed {max_requests} requests")
            return

        # ограничение скорости запросов
        # await asyncio.sleep(random.uniform(1.5, 3.5))
        await asyncio.sleep(2)


async def starships_worker(session, max_requests: int | None):
    """Worker(ассинхронная ф-я в цикле) для /starships: выполняет запросы до max_requests или бесконечно"""
    count = 0
    while True:
        await process_entity(session, "starships", 1, 100)
        count += 1

        if max_requests is not None and count >= max_requests:
            logger.info(f"[people] completed fixed {max_requests} requests")
            return

        #ограничение скорости запросов
        # await asyncio.sleep(random.uniform(1.5, 3.5))
        await asyncio.sleep(2)


async def main(max_people_requests: int | None = None, max_starships_requests: int | None = None):
    """запускает people и starships функции параллельно с указанными лимитами."""
    # создание клиентской сессии
    async with aiohttp.ClientSession() as session:
        # корутины(асинхронные функции)(в нашем случае 2 потока запросов) работают параллельно в event loop(событийном цикле)
        await asyncio.gather(
            people_worker(session, max_people_requests),
            starships_worker(session, max_starships_requests)
        )


# Бесконечная работа(по умолчанию)
# if __name__ == "__main__":
#     asyncio.run(main())

# Возможность добавления фиксированного количества запросов для каждого из потоков(people_worker, starships_worker)
if __name__ == "__main__":
    asyncio.run(main(max_people_requests=5, max_starships_requests=5))
