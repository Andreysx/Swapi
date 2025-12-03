from pathlib import Path

# Корневая директория для данных
DATA_DIR = Path("data")

# URL API
BASE_URL = "https://swapi.dev/api"

# Список ссылочных полей SWAPI — не включаются в xlsx
LINK_FIELDS = {
    "films",
    "vehicles",
    "starships",
    "homeworld",
    "species",
    "url"
}
