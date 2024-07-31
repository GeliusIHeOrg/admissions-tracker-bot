import os
import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from typing import Dict, List, Tuple

file_cache = {}

async def fetch_excel_file(session, url: str) -> bytes:
    async with session.get(url) as response:
        return await response.read()

async def process_excel_file(session, url: str, snils: str) -> Dict:
    if url in file_cache:
        df = file_cache[url]
    else:
        try:
            content = await asyncio.wait_for(fetch_excel_file(session, url), timeout=10)
            df = pd.read_excel(BytesIO(content), usecols=[0, 1, 18, 22], names=['Позиция', 'СНИЛС', 'Сумма_баллов', 'Оригинал'])
            file_cache[url] = df
        except asyncio.TimeoutError:
            print(f"Timeout error fetching file: {url}")
            return None
        except Exception as e:
            print(f"Error processing file {url}: {str(e)}")
            return None

    try:
        df['СНИЛС'] = df['СНИЛС'].astype(str).str.replace(r'\D', '', regex=True)
        mask = df['СНИЛС'] == snils.replace('-', '').replace(' ', '')
        if mask.any():
            row = df.loc[mask].iloc[0]
            return {
                'position': int(row['Позиция']),
                'total_score': int(row['Сумма_баллов']),
                'original_document': bool(row['Оригинал'])
            }
    except Exception as e:
        print(f"Error processing data for {url}: {str(e)}")
    return None

async def process_city_programs(session, city: str, programs: Dict[str, str], snils: str) -> List[Dict]:
    tasks = [process_excel_file(session, url, snils) for program, url in programs.items()]
    results = await asyncio.gather(*tasks)
    return [{'city': city, 'program': program, 'result': result}
            for program, result in zip(programs.keys(), results) if result]

async def process_all_programs(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str) -> List[Dict]:
    async with aiohttp.ClientSession() as session:
        tasks = [process_city_programs(session, city, all_programs[city.lower()], snils) for city in cities if city.lower() in all_programs]
        results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

async def process_with_timeout(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str, timeout: int = 30) -> List[Dict]:
    try:
        return await asyncio.wait_for(process_all_programs(cities, all_programs, snils), timeout=timeout)
    except asyncio.TimeoutError:
        print(f"Processing took too long and was interrupted after {timeout} seconds")
        return []
