import os
import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from typing import Dict, List, Tuple
import time
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
from concurrent.futures import ThreadPoolExecutor

# Кэш для хранения обработанных файлов
file_cache = {}

async def fetch_excel_file(session, url: str) -> bytes:
    async with session.get(url) as response:
        return await response.read()

def process_dataframe(df: dd.DataFrame, snils: str) -> Dict:
    df['СНИЛС'] = df['СНИЛС'].astype(str).str.replace(r'\D', '', regex=True)
    mask = df['СНИЛС'] == snils.replace('-', '').replace(' ', '')
    result = df[mask].compute()
    if not result.empty:
        row = result.iloc[0]
        return {
            'position': int(row['Позиция']),
            'total_score': int(row['Сумма_баллов']),
            'original_document': bool(row['Оригинал'])
        }
    return None

async def process_excel_file(session, url: str, snils: str) -> Dict:
    if url in file_cache:
        df = file_cache[url]
    else:
        try:
            content = await asyncio.wait_for(fetch_excel_file(session, url), timeout=5)
            pdf = pd.read_excel(BytesIO(content), usecols=[0, 1, 18, 22], names=['Позиция', 'СНИЛС', 'Сумма_баллов', 'Оригинал'], engine='openpyxl')
            df = dd.from_pandas(pdf, npartitions=4)  # Convert pandas DataFrame to Dask DataFrame
            file_cache[url] = df
        except asyncio.TimeoutError:
            print(f"Timeout error fetching file: {url}")
            return None
        except Exception as e:
            print(f"Error processing file {url}: {str(e)}")
            return None

    try:
        return process_dataframe(df, snils)
    except Exception as e:
        print(f"Error processing data for {url}: {str(e)}")
    return None

async def process_city_programs(session, city: str, programs: Dict[str, str], snils: str) -> List[Dict]:
    tasks = [process_excel_file(session, url, snils) for program, url in programs.items()]
    results = await asyncio.gather(*tasks)
    return [{'city': city, 'program': program, 'result': result}
            for program, result in zip(programs.keys(), results) if result]

async def process_all_programs(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str) -> List[Dict]:
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
        tasks = [process_city_programs(session, city, all_programs[city.lower()], snils) for city in cities if city.lower() in all_programs]
        results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

async def process_with_timeout(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str, timeout: int = 3) -> List[Dict]:
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

# Пример использования:
# cities = ['москва', 'санкт-петербург', 'пермь', 'нижний новгород']
# results = await process_with_timeout(cities, hse_programs, snils, timeout=3)