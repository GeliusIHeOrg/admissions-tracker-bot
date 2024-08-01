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

# Функция для сохранения данных для ННГУ с использованием пакетной вставки
async def clear_table(table: str):
    try:
        supabase.table(table).delete().neq('id', 0).execute()
    except Exception as e:
        print(f"Error clearing table: {e}")
        raise

# Функция для сохранения данных для ННГУ с использованием пакетной вставки
async def save_unn_cached_data(df: pd.DataFrame, batch_size: int = 500):
    try:
        df = df.where(pd.notnull(df), None)
        df['number'] = pd.to_numeric(df['number'], errors='coerce').astype('Int64', errors='ignore')
        df['snils'] = df['snils'].astype(str, errors='ignore')
        df['total_score'] = pd.to_numeric(df['total_score'], errors='coerce').astype('Int64', errors='ignore')
        df['priority'] = pd.to_numeric(df['priority'], errors='coerce').astype('Int64', errors='ignore')
        df['original_document'] = df['original_document'].astype(bool, errors='ignore')

        data_list = df.to_dict(orient='records')
        for data in data_list:
            data['last_updated'] = datetime.now().isoformat()

        # Пакетная вставка без проверки на существующие записи
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            supabase.table('cache_unn').insert(batch).execute()
            print(f"Processed batch {i // batch_size + 1}")

    except Exception as e:
        print(f"Error saving cached data: {e}")
        raise

# Функция для сохранения данных для ВШЭ с использованием пакетной вставки
async def save_cached_data(city: str, program: str, df: pd.DataFrame, batch_size: int = 500):
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

        # Пакетная вставка
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            for record in batch:
                existing_record = supabase.table('cache').select('*').eq('snils', record['snils']).eq('city', record['city']).eq('program', record['program']).execute()
                if existing_record.data:
                    # Обновляем существующую запись
                    supabase.table('cache').update(record).eq('snils', record['snils']).eq('city', record['city']).eq('program', record['program']).execute()
                else:
                    # Вставляем новую запись
                    supabase.table('cache').insert(record).execute()
            print(f"Processed batch {i // batch_size + 1}")

    except Exception as e:
        print(f"Error saving cached data: {e}")
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

async def get_user_position(snils: str, table: str = 'cache'):
    try:
        print(f"Ищем СНИЛС: {snils} в таблице '{table}'")
        data = supabase.table(table).select('*').eq('snils', snils).execute()
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

# Новая функция для получения общего количества строк
async def get_total_rows(table: str = 'cache'):
    try:
        data = supabase.table(table).select('*', count='exact').execute()
        return data.count if data else 0
    except Exception as e:
        print(f"Error getting total rows: {e}")
        raise
