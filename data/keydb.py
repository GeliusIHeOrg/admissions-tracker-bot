import os
import pandas as pd
import logging
from redis.asyncio import Redis
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

redis = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
logging.basicConfig(level=logging.DEBUG)


async def clear_table(table: str):
    try:
        keys = await redis.keys(f"{table}:*")
        if keys:
            await redis.delete(*keys)
        logging.debug(f"Cleared table {table}")
    except Exception as e:
        logging.error(f"Error clearing table: {e}")
        raise


async def save_unn_cached_data(df: pd.DataFrame, batch_size: int = 500):
    try:
        df = df.where(pd.notnull(df), None)
        df['number'] = pd.to_numeric(df['number'], errors='coerce').astype('Int64', errors='ignore')
        df['snils'] = df['snils'].astype(str, errors='ignore')
        df['total_score'] = pd.to_numeric(df['total_score'], errors='coerce').astype('Int64', errors='ignore')
        df['priority'] = pd.to_numeric(df['priority'], errors='coerce').astype('Int64', errors='ignore')
        df['original_document'] = df['original_document'].astype(bool, errors='ignore')

        data_list = df.to_dict(orient='records')
        pipeline = redis.pipeline()
        for i, data in enumerate(data_list):
            data['last_updated'] = datetime.now().isoformat()
            data['original_document'] = int(data['original_document'])
            data['disciplines'] = json.dumps(data['disciplines'])

            key = f"unn:{data['snils']}:{data['number']}"
            pipeline.hset(key, mapping=data)

            if (i + 1) % batch_size == 0:
                await pipeline.execute()
                pipeline = redis.pipeline()

        if pipeline:
            await pipeline.execute()

        logging.debug("Data saved to KeyDB")

    except Exception as e:
        logging.error(f"Error saving cached data: {e}")
        raise


async def save_cached_data(city: str, program: str, df: pd.DataFrame, batch_size: int = 500):
    try:
        df = df.where(pd.notnull(df), None)
        df['Позиция'] = pd.to_numeric(df['Позиция'], errors='coerce').astype('Int64', errors='ignore')
        df['СНИЛС'] = df['СНИЛС'].astype(str, errors='ignore')
        df['Сумма_баллов'] = pd.to_numeric(df['Сумма_баллов'], errors='coerce').astype('Int64', errors='ignore')
        df['Оригинал'] = df['Оригинал'].astype(bool, errors='ignore')

        data_list = df.to_dict(orient='records')
        pipeline = redis.pipeline()
        for i, data in enumerate(data_list):
            data['city'] = city
            data['program'] = program
            data['last_updated'] = datetime.now().isoformat()
            data['position'] = data.pop('Позиция', None)
            data['snils'] = data.pop('СНИЛС', None)
            data['total_score'] = data.pop('Сумма_баллов', None)
            data['original_document'] = int(data.pop('Оригинал', None))

            if None in data.values():
                logging.error(f"Invalid data found: {data}")
                continue

            key = f"hse:{data['snils']}:{city}:{program}"
            pipeline.hset(key, mapping=data)

            if (i + 1) % batch_size == 0:
                await pipeline.execute()
                pipeline = redis.pipeline()

        if pipeline:
            await pipeline.execute()

        logging.debug("Data saved to KeyDB")

    except Exception as e:
        logging.error(f"Error saving cached data: {e}")
        raise


async def save_snils(user_id: int, snils: str):
    try:
        await redis.hset(f"user:{user_id}", "snils", snils)
        logging.debug("SNILS saved to KeyDB")
    except Exception as e:
        logging.error(f"Error saving SNILS: {e}")
        raise


async def get_last_updated(table: str):
    try:
        keys = await redis.keys(f"{table}:*")
        latest = None
        for key in keys:
            last_updated = await redis.hget(key, "last_updated")
            if last_updated and (latest is None or last_updated > latest):
                latest = last_updated
        if latest:
            return datetime.fromisoformat(latest)
        return None
    except Exception as e:
        logging.error(f"Error getting last updated: {e}")
        raise


async def get_snils(user_id: int):
    try:
        snils = await redis.hget(f"user:{user_id}", "snils")
        return snils
    except Exception as e:
        logging.error(f"Error getting SNILS: {e}")
        raise


async def get_cached_data(city: str, program: str):
    try:
        keys = await redis.keys(f"hse:*:{city}:{program}")
        if not keys:
            return None

        pipeline = redis.pipeline()
        for key in keys:
            pipeline.hgetall(key)

        results = await pipeline.execute()
        data_list = [data for data in results if data]

        if data_list:
            last_updated = datetime.fromisoformat(data_list[0]['last_updated'])
            if datetime.now(last_updated.tzinfo) - last_updated < timedelta(hours=3):
                return pd.DataFrame(data_list)
        return None
    except Exception as e:
        logging.error(f"Error getting cached data: {e}")
        raise


async def get_user_position(snils: str, table: str = 'cache'):
    try:
        logging.debug(f"Ищем СНИЛС: {snils} в таблице '{table}'")
        keys = await redis.keys(f"{table}:{snils}:*")
        data_list = []
        for key in keys:
            data = await redis.hgetall(key)
            if 'disciplines' in data:
                data['disciplines'] = json.loads(data['disciplines'])
            if data:
                data_list.append(data)
        if data_list:
            logging.debug(f"Найдены данные: {data_list}")
            return data_list
        logging.debug("Данные не найдены.")
        return None
    except Exception as e:
        logging.error(f"Ошибка при получении позиции пользователя: {e}")
        raise


def is_data_stale(last_updated_str: str, hours: int = 4) -> bool:
    try:
        last_updated = datetime.fromisoformat(last_updated_str)
        return datetime.now(last_updated.tzinfo) - last_updated > timedelta(hours=hours)
    except ValueError:
        logging.error(f"Invalid date format: {last_updated_str}")
        return True


async def get_total_rows(table: str = 'cache'):
    try:
        keys = await redis.keys(f"{table}:*")
        return len(keys)
    except Exception as e:
        logging.error(f"Error getting total rows: {e}")
        raise