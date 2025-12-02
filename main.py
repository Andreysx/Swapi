import asyncio
import aiohttp
import json
import random
from datetime import datetime
from pathlib import Path
from openpyxl import Workbook

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://swapi.dev/api"

# Список ссылочных полей SWAPI
LINK_FIELDS = {
    "films",
    "vehicles",
    "starships",
    "homeworld",
    "species",
    "url"
}


async def fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        if resp.status != 200:
            return None
        return await resp.json()


def write_txt(file_path: Path, entity_data: dict):
    """
    Создаем или перезаписываем существующий файл txt
    Записываем все поля из сущности рест с ссылками в формате json
    """
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(entity_data, f, indent=4, ensure_ascii=False)
        f.write("\n\nUpdated: " + datetime.now().isoformat())


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


def write_xlsx(file_path: Path, entity_data: dict):
    """
    Создаем или перезаписываем существующий файл xlsx
    Записываем все поля из сущности рест без ссылок и с приведенными типами
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

    # строка обновления
    ws.append([])
    ws.append(["Updated at:     ", datetime.now().isoformat()])

    wb.save(file_path)


async def process_entity(session, entity_type: str, id_min: int, id_max: int):
    """
    Основной процесс обработки:
    1) получение /entity/id/
    2) запись TXT + XLSX
    """
    random_id = random.randint(id_min, id_max)
    url = f"{BASE_URL}/{entity_type}/{random_id}/"

    data = await fetch_json(session, url)
    if not data:
        print(f"[{entity_type}] id={random_id} 404 Not Found")
        return

    file_txt = DATA_DIR / f"{entity_type}.txt"
    file_xlsx = DATA_DIR / f"{entity_type}.xlsx"

    write_txt(file_txt, data)
    write_xlsx(file_xlsx, data)

    print(f"[{entity_type}] updated id={random_id}")


async def people_worker(session, max_requests: int | None):
    count = 0
    while True:
        await process_entity(session, "people", 1, 100)
        count += 1

        if max_requests is not None and count >= max_requests:
            print(f"[people] completed fixed {max_requests} requests")
            return

        await asyncio.sleep(2)


async def starships_worker(session, max_requests: int | None):
    count = 0
    while True:
        await process_entity(session, "starships", 1, 100)
        count += 1

        if max_requests is not None and count >= max_requests:
            print(f"[starships] completed fixed {max_requests} requests")
            return

        await asyncio.sleep(2)


async def main(max_people_requests: int | None = None, max_starships_requests: int | None = None):
    async with aiohttp.ClientSession() as session:
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
