import logging
import requests
from datetime import datetime
import json
from typing import List, Dict, Any, Optional

# --- НАСТРОЙКА ЛОГГЕРА ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
)
logger = logging.getLogger("click_ru_to_clickhouse")


# --- ПОДКЛЮЧЕНИЕ К CLICK.RU ---
API_URL = "https://api.click.ru/V0/users"
API_TOKEN = "657657dsleofkmf34256noisdfbfo34354a"  # <--- ВАШ API ТОКЕН
USER_ID = "111111111"                            # <--- ВАШ UID


# --- ПОДКЛЮЧЕНИЕ К CLICKHOUSE ---
CH_HOST = "localhost"               # ХОСТ CLICKHOUSE
CH_PORT = "8123"                    # ПОРТ CLICKHOUSE
CH_USER = "default"                 # ПОЛЬЗОВАТЕЛЬ CLICKHOUSE
CH_PASSWORD = "123"                 # ПАРОЛЬ CLICKHOUSE
CH_DATABASE = "test_db"             # <--- ИМЯ БАЗЫ ДАННЫХ CLICKHOUSE
CH_TABLENAME = "click_ru_balances"  # <--- ИМЯ ТАБЛИЦЫ CLICKHOUSE
# ---------------------


# --- ПАРАМЕТРЫ ДЛЯ ПОДКЛЮЧЕНИЯ К CLICKHOUSE ---
CLICKHOUSE_URL: str = f"http://{CH_HOST}:{CH_PORT}/"
CLICKHOUSE_HEADERS: Dict[str, str] = {
    "X-ClickHouse-User": CH_USER,
    "X-ClickHouse-Key": CH_PASSWORD,
    "Content-Type": "application/json"
}
CLICKHOUSE_TIMEOUT: int = 4


# --- ЗАПРОС НА СОЗДАНИЕ БАЗЫ ДАННЫХ CLICKHOUSE ---
CREATE_DATABASE_QUERY = f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}"
# --- ЗАПРОС НА СОЗДАНИЕ ТАБЛИЦЫ CLICKHOUSE ---
CREATE_USER_ACCOUNTS_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {CH_DATABASE}.{CH_TABLENAME} (
    id UInt64,
    login String,
    type String,
    email Nullable(String),
    description String,
    firstName String,
    lastName String,
    middleName String,
    balance Float64,
    createdAt DateTime,
    export_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id
"""
# --- ЗАПРОС НА ОЧИСТКУ ТАБЛИЦЫ CLICKHOUSE ---
TRUNCATE_TABLE_QUERY = f"TRUNCATE TABLE {CH_DATABASE}.{CH_TABLENAME}"
# --- ЗАПРОС НА ВСТАВКУ ДАННЫХ В ТАБЛИЦУ CLICKHOUSE ---
INSERT_USER_ACCOUNTS_QUERY = f'INSERT INTO {CH_DATABASE}.{CH_TABLENAME} FORMAT JSONEachRow'


def get_click_ru_users_accounts_from_api() -> Optional[List[Dict[str, Any]]]:
    """
    Получает список пользовательских аккаунтов из API Click.ru.

    Args:
        Нет аргументов. Используются глобальные константы API_URL, API_TOKEN, USER_ID.

    Returns:
        Optional[List[Dict[str, Any]]]:
            Список словарей с данными пользователей, если запрос успешен.
            None — если произошла ошибка при запросе или парсинге.

    Raises:
        Не выбрасывает исключения наружу, все ошибки логируются и возвращается None.
    """
    api_request_headers: Dict[str, str] = {
        "Accept": "application/json",
        "X-Auth-Token": API_TOKEN,
        "X-Auth-UserId": USER_ID
    }
    try:
        logger.info("[CLICK_RU_API_REQUEST] Запрос к Click.ru API для получения списка пользователей")  # СЕМАНТИЧЕСКИЙ_ЯКОРЬ: Запрос к API
        api_response = requests.get(API_URL, headers=api_request_headers, timeout=15)
        api_response.raise_for_status()
        accounts_list: List[Dict[str, Any]] = api_response.json()['response']['users']
        logger.info(f"[CLICK_RU_API_RESPONSE] Получено пользователей: {len(accounts_list)}")  # СЕМАНТИЧЕСКИЙ_ЯКОРЬ: Ответ API
        return accounts_list
    except Exception as api_exception:
        raise Exception(f"Ошибка при запросе к Click.ru API: {api_exception}") from api_exception


def execute_clickhouse_query(query: str, data: Optional[str] = None) -> None:
    """
    Выполняет SQL-запрос к ClickHouse через HTTP-интерфейс.

    Args:
        query (str): SQL-запрос для выполнения.
        data (Optional[str]): Данные для отправки (например, JSON для INSERT). По умолчанию None.

    Raises:
        requests.RequestException: При ошибке HTTP-запроса.
    """
    request_params = {"query": query}
    
    try:
        if data is not None:
            logger.info(f"[CLICKHOUSE_QUERY] Выполнение запроса с данными: {query[:60]}... [data length: {len(data)}]")  # СЕМАНТИЧЕСКИЙ_ЯКОРЬ: ClickHouse Insert
            response = requests.post(
                CLICKHOUSE_URL,
                params=request_params,
                data=data,
                headers=CLICKHOUSE_HEADERS,
                timeout=CLICKHOUSE_TIMEOUT
            )
        else:
            logger.info(f"[CLICKHOUSE_QUERY] Выполнение запроса: {query[:80]}")  # СЕМАНТИЧЕСКИЙ_ЯКОРЬ: ClickHouse Query
            response = requests.post(
                CLICKHOUSE_URL,
                params=request_params,
                headers=CLICKHOUSE_HEADERS,
                timeout=CLICKHOUSE_TIMEOUT
            )
        response.raise_for_status()
        logger.info("[CLICKHOUSE_OK] Запрос успешно выполнен")  # СЕМАНТИЧЕСКИЙ_ЯКОРЬ: ClickHouse OK
    except Exception as ch_exc:
        raise


def prepare_clickhouse_user_accounts_data_in_json(accounts: List[Dict[str, Any]]) -> str:
    """
    Преобразует список аккаунтов в формат, пригодный для вставки в ClickHouse.

    Args:
        accounts (List[Dict[str, Any]]): Список аккаунтов из Click.ru API.

    Returns:
        str: Список словарей для вставки в ClickHouse в формате JSON.
    """
    prepared_data: List[Dict[str, Any]] = []
    for user in accounts:
        prepared_data.append({
            "id": user["id"],
            "login": user.get("login", ""),
            "type": user.get("type", ""),
            "email": user.get("email"),
            "description": user.get("description", ""),
            "firstName": user.get("firstName", ""),
            "lastName": user.get("lastName", ""),
            "middleName": user.get("middleName", ""),
            "balance": float(user.get("balance", 0)),
            "createdAt": datetime.strptime(
                user["createdAt"], "%Y-%m-%dT%H:%M:%S%z"
            ).strftime("%Y-%m-%d %H:%M:%S")
        })

    # Вставка данных в таблицу
    prepared_data_in_json = '\n'.join(json.dumps(row) for row in prepared_data)
    return prepared_data_in_json



def main():
    """
    Главная функция для управления таблицей user_accounts в ClickHouse.
    Создаёт таблицу, если она не существует, и загружает данные из API Click.ru.
    """
    try:
        logger.info("[START] Запуск обновления баланса аккаунтов Click.ru → ClickHouse")  
        data = get_click_ru_users_accounts_from_api()
        if not data:
            logger.error("[NO_DATA] Не удалось получить данные пользователей из Click.ru API. Список пустой или произошла ошибка.")  
            return

        logger.info("[PREPARE] Подготовка данных для ClickHouse") 
        prepared_data_in_json = prepare_clickhouse_user_accounts_data_in_json(data)

        logger.info("[CLICKHOUSE_DB] Создание базы данных, если не существует") 
        execute_clickhouse_query(CREATE_DATABASE_QUERY)

        logger.info("[CLICKHOUSE_TABLE] Создание таблицы, если не существует")  
        execute_clickhouse_query(CREATE_USER_ACCOUNTS_TABLE_QUERY)

        logger.info("[CLICKHOUSE_TRUNCATE] Очистка таблицы перед вставкой новых данных") 
        execute_clickhouse_query(TRUNCATE_TABLE_QUERY)

        logger.info(f"[CLICKHOUSE_INSERT] Вставка {len(data)} записей в ClickHouse") 
        execute_clickhouse_query(INSERT_USER_ACCOUNTS_QUERY, data=prepared_data_in_json)

        logger.info("[SUCCESS] ✅ Данные успешно обновлены")  
    except Exception as e:
        logger.critical(f"[FATAL] 🔴 Ошибка обновления: {e}")  



if __name__ == "__main__":
    main()