import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from typing import Dict, List
from dask.distributed import Client
from data.supabase_db import save_cached_data, get_cached_data

async def fetch_excel_file(session, url: str, retries: int = 3, timeout: int = 10) -> bytes:
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as response:
                return await response.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt == retries - 1:
                raise

def process_dataframe(df: pd.DataFrame, snils: str) -> Dict:
    required_columns = ['Позиция', 'СНИЛС', 'Сумма_баллов', 'Оригинал']
    for col in required_columns:
        if col not in df.columns:
            print(f"Missing column: {col}")
            return None

    df['СНИЛС'] = df['СНИЛС'].astype(str).str.replace(r'\D', '', regex=True)
    mask = df['СНИЛС'] == snils
    result = df[mask]
    if not result.empty:
        row = result.iloc[0]
        try:
            return {
                'position': int(row['Позиция']) if pd.notna(row['Позиция']) else None,
                'total_score': int(row['Сумма_баллов']) if pd.notna(row['Сумма_баллов']) else None,
                'original_document': bool(row['Оригинал']) if pd.notna(row['Оригинал']) else None
            }
        except ValueError as e:
            print(f"Error converting row values: {e}")
            return None
    return None

async def process_excel_file(session, city: str, program: str, url: str, snils: str) -> Dict:
    cached_data = await get_cached_data(city, program)
    if cached_data is not None:
        df = pd.DataFrame(cached_data)
    else:
        try:
            content = await fetch_excel_file(session, url)
            df = pd.read_excel(BytesIO(content), usecols=[0, 1, 18, 22], names=['Позиция', 'СНИЛС', 'Сумма_баллов', 'Оригинал'], engine='openpyxl')
            if df is not None:
                await save_cached_data(city, program, df)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Timeout error fetching file: {url}")
            return None
        except Exception as e:
            print(f"Error processing file {url}: {str(e)}")
            return None

    return process_dataframe(df, snils)

async def process_city_programs(session, city: str, programs: Dict[str, str], snils: str) -> List[Dict]:
    tasks = [process_excel_file(session, city, program, url, snils) for program, url in programs.items()]
    results = await asyncio.gather(*tasks)
    return [{'city': city, 'program': program, 'result': result}
            for program, result in zip(programs.keys(), results) if result]

async def process_all_programs(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str) -> List[Dict]:
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
        tasks = [process_city_programs(session, city, all_programs[city.lower()], snils) for city in cities if city.lower() in all_programs]
        results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

async def process_with_timeout(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str, timeout: int = 30) -> List[Dict]:
    try:
        client = Client(n_workers=4, threads_per_worker=2)
        results = await asyncio.wait_for(process_all_programs(cities, all_programs, snils), timeout=timeout)
        client.close()
        return results
    except asyncio.TimeoutError:
        print(f"Processing took too long and was interrupted after {timeout} seconds")
        return []
    finally:
        if 'client' in locals():
            client.close()
