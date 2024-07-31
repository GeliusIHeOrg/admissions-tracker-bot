import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Загрузка переменных окружения
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL is not set in the environment variables")
if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY is not set in the environment variables")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"Error creating Supabase client: {e}")
    raise


async def save_snils(user_id: int, snils: str):
    try:
        existing_record = supabase.table('users').select('user_id').eq('user_id', user_id).execute()

        if existing_record.data:
            data = supabase.table('users').update({"snils": snils}).eq('user_id', user_id).execute()
        else:
            data = supabase.table('users').insert({"user_id": user_id, "snils": snils}).execute()

        return data
    except Exception as e:
        print(f"Error saving SNILS: {e}")
        raise


async def get_snils(user_id: int):
    try:
        data = supabase.table('users').select('snils').eq('user_id', user_id).execute()
        if data.data:
            return data.data[0]['snils']
        return None
    except Exception as e:
        print(f"Error getting SNILS: {e}")
        raise


async def get_cached_data(city: str, program: str):
    try:
        data = supabase.table('cache').select('*').eq('city', city).eq('program', program).execute()
        if data.data:
            last_updated = datetime.fromisoformat(data.data[0]['last_updated'])
            if datetime.now(last_updated.tzinfo) - last_updated < timedelta(hours=3):
                return pd.DataFrame(data.data)
        return None
    except Exception as e:
        print(f"Error getting cached data: {e}")
        raise


async def get_user_position(snils: str):
    try:
        print(f"Ищем СНИЛС: {snils} в таблице 'cache'")
        data = supabase.table('cache').select('*').eq('snils', snils).execute()
        if data.data:
            print(f"Найдены данные: {data.data}")
            return data.data
        print("Данные не найдены.")
        return None
    except Exception as e:
        print(f"Ошибка при получении позиции пользователя: {e}")
        raise

def is_data_stale(last_updated_str: str, hours: int = 4) -> bool:
    last_updated = datetime.fromisoformat(last_updated_str)
    return datetime.now(last_updated.tzinfo) - last_updated > timedelta(hours=hours)


async def save_cached_data(city: str, program: str, df: pd.DataFrame):
    try:
        df = df.where(pd.notnull(df), None)
        df['Позиция'] = pd.to_numeric(df['Позиция'], errors='coerce').astype('Int64', errors='ignore')
        df['СНИЛС'] = df['СНИЛС'].astype(str, errors='ignore')
        df['Сумма_баллов'] = pd.to_numeric(df['Сумма_баллов'], errors='coerce').astype('Int64', errors='ignore')
        df['Оригинал'] = df['Оригинал'].astype(bool, errors='ignore')

        data_list = df.to_dict(orient='records')
        for data in data_list:
            data['city'] = city
            data['program'] = program
            data['last_updated'] = datetime.now().isoformat()
            data['position'] = data.pop('Позиция', None)
            data['snils'] = data.pop('СНИЛС', None)
            data['total_score'] = data.pop('Сумма_баллов', None)
            data['original_document'] = data.pop('Оригинал', None)

            supabase.table('cache').upsert(data).execute()
    except Exception as e:
        print(f"Error saving cached data: {e}")
        raise
