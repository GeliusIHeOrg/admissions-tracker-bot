import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from typing import Dict, List
import logging
from datetime import datetime

from data.keydb import save_cached_data, get_cached_data, redis, is_data_stale, get_user_position

logging.basicConfig(level=logging.DEBUG)


async def fetch_excel_file(session, url: str, retries: int = 3, timeout: int = 10) -> bytes:
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    logging.debug(f"Fetched {url} successfully.")
                    return await response.read()
                else:
                    logging.error(f"Failed to fetch {url}, status code: {response.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt == retries - 1:
                raise


async def process_all_programs(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str) -> List[Dict]:
    logging.debug('Processing all programs...')
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
        tasks = [process_city_programs(session, city, all_programs[city], snils) for city in cities if
                 city in all_programs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    processed_results = []
    for city, result in zip(cities, results):
        if isinstance(result, Exception):
            logging.error(f"Error processing city {city}: {result}")
        elif result:
            processed_results.extend(result)

    logging.debug(f"Processed {len(processed_results)} results.")
    return processed_results


async def process_city_programs(session, city: str, programs: Dict[str, str], snils: str) -> List[Dict]:
    logging.debug(f"Processing city programs for {city} with {len(programs)} programs.")
    tasks = [process_excel_file(session, city, program, url, snils) for program, url in programs.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    processed_results = []
    for program, result in zip(programs.keys(), results):
        if isinstance(result, Exception):
            logging.error(f"Error processing program {program}: {result}")
        elif result:
            processed_results.append({'city': city, 'program': program, 'result': result})

    logging.debug(f"Processed {len(processed_results)} programs for {city}")
    return processed_results

async def process_with_timeout(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str, timeout: int = 60) -> List[Dict]:
    try:
        return await asyncio.wait_for(process_all_programs(cities, all_programs, snils), timeout=timeout)
    except asyncio.TimeoutError:
        logging.error(f"Processing exceeded the time limit of {timeout} seconds")
        return []


async def process_excel_file(session, city: str, program: str, url: str, snils: str) -> Dict:
    logging.debug(f"Processing Excel file for city: {city}, program: {program}, URL: {url}")
    try:
        cached_data = await get_cached_data(city, program)
        if cached_data is not None:
            df = pd.DataFrame(cached_data)
        else:
            content = await fetch_excel_file(session, url)
            if content:
                logging.debug(f"Reading Excel content from {url}")
                try:
                    df = pd.read_excel(BytesIO(content), usecols=[0, 1, 18, 22],
                                       names=['Позиция', 'СНИЛС', 'Сумма_баллов', 'Оригинал'], engine='openpyxl')
                    logging.debug(f"Successfully read Excel content for {city} - {program}")
                    logging.debug(f"Normalized columns: {', '.join(df.columns)}")
                    await save_cached_data(city, program, df)
                except Exception as e:
                    logging.error(f"Error reading or normalizing Excel file for {city} - {program}: {e}")
                    return None
            else:
                logging.error(f"Failed to fetch content from {url}")
                return None

        return process_dataframe(df, snils)
    except Exception as e:
        logging.error(f"Error processing file for {city} - {program}: {str(e)}")
        return None


def process_dataframe(df: pd.DataFrame, snils: str) -> Dict:
    required_columns = ['Позиция', 'СНИЛС', 'Сумма_баллов', 'Оригинал']
    for col in required_columns:
        if col not in df.columns:
            logging.error(f"Missing column: {col}")
            return None

    df['СНИЛС'] = df['СНИЛС'].astype(str).str.replace(r'\D', '', regex=True)
    mask = df['СНИЛС'] == snils
    result = df[mask]

    if not result.empty:
        row = result.iloc[0]
        logging.debug(f"Found matching row: {row.to_dict()}")
        try:
            # Debugging first 5 rows
            for index, debug_row in df.head(5).iterrows():
                logging.debug(
                    f"Row {index + 1}: SNILS: {debug_row['СНИЛС']}, Position: {debug_row['Позиция']}, Original Document: {debug_row['Оригинал']}")

            return {
                'position': int(row['Позиция']) if pd.notna(row['Позиция']) else None,
                'total_score': int(row['Сумма_баллов']) if pd.notna(row['Сумма_баллов']) else None,
                'original_document': bool(row['Оригинал']) if pd.notna(row['Оригинал']) else None,
                'last_updated': datetime.now().isoformat()
            }
        except ValueError as e:
            logging.error(f"Error converting row values: {e}")
            return None
    else:
        logging.debug(f"No matching rows found for SNILS: {snils}")
    return None
