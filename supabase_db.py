import os
from supabase import create_client, Client
from dotenv import load_dotenv

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
        # Проверка, существует ли запись с данным user_id
        existing_record = supabase.table('users').select('user_id').eq('user_id', user_id).execute()

        if existing_record.data:
            # Если запись существует, обновить её
            data = supabase.table('users').update({"snils": snils}).eq('user_id', user_id).execute()
        else:
            # Если записи нет, вставить новую
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
