import pandas as pd
import aiohttp
import asyncio
from io import BytesIO
from typing import Dict, List
import logging
from datetime import datetime
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from data.hse_data import hse_programs
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

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    column_mapping = {
        'position': 'позиция',
        'snils': 'снилс',
        'total_score': 'сумма_баллов',
        'original_document': 'оригинал',
        'позиция': 'позиция',
        'снилс': 'снилс',
        'сумма_баллов': 'сумма_баллов',
        'оригинал': 'оригинал'
    }

    df.columns = [str(col).strip().lower() if col is not None else '' for col in df.columns]

    for i, row in df.iterrows():
        if any('снилс' in str(cell).lower() for cell in row):
            df.columns = [str(cell).strip().lower() if cell is not None else '' for cell in row]
            df = df.drop(i).reset_index(drop=True)
            break

    df.columns = [column_mapping.get(col, col) for col in df.columns]

    df = df.applymap(lambda x: str(x).strip() if x is not None else '')

    if 'позиция' not in df.columns:
        df['позиция'] = range(1, len(df) + 1)

    return df

def process_dataframe(df: pd.DataFrame, snils: str) -> Dict:
    required_columns = ['позиция', 'снилс', 'сумма_баллов', 'оригинал']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing columns: {', '.join(missing_columns)}")
        logging.debug(f"Available columns: {', '.join(df.columns)}")
        return None

    df['снилс'] = df['снилс'].str.replace(r'\D', '', regex=True)
    mask = df['снилс'] == snils
    result = df[mask]
    if not result.empty:
        row = result.iloc[0]
        logging.debug(f"Found matching row: {row.to_dict()}")
        try:
            return {
                'position': int(float(row['позиция'])) if row['позиция'] and row['позиция'] != '' else None,
                'total_score': int(float(row['сумма_баллов'])) if row['сумма_баллов'] and row['сумма_баллов'] != '' else None,
                'original_document': bool(row['оригинал']) if row['оригинал'] and row['оригинал'] != '' else None,
                'last_updated': datetime.now().isoformat()
            }
        except ValueError as e:
            logging.error(f"Error converting row values: {e}")
            return None
    else:
        logging.debug(f"No matching rows found for СНИЛС: {snils}")
    return None

async def process_excel_file(session, city: str, program: str, url: str, snils: str) -> Dict:
    logging.debug(f"Обрабатываем Excel файл для города: {city}, программы: {program}, URL: {url}")
    try:
        cached_data = await get_cached_data(city, program)
        if cached_data is not None:
            df = pd.DataFrame(cached_data)
            df = normalize_column_names(df)
        else:
            content = await fetch_excel_file(session, url)
            if content:
                logging.debug(f"Читаем содержимое Excel из {url}")
                try:
                    df = pd.read_excel(BytesIO(content), header=None)
                    df = normalize_column_names(df)
                    logging.debug(f"Содержимое Excel успешно прочитано для {city} - {program}")
                    logging.debug(f"Normalized columns: {', '.join(df.columns)}")
                    await save_cached_data(city, program, df)
                except Exception as e:
                    logging.error(f"Ошибка при чтении или нормализации Excel файла для {city} - {program}: {e}")
                    return None
            else:
                logging.error(f"Не удалось получить содержимое из {url}")
                return None

        processed_data = process_dataframe(df, snils)
        logging.debug(f"Processed data for {city} - {program}: {processed_data}")
        return processed_data
    except Exception as e:
        logging.error(f"Ошибка при обработке файла для {city} - {program}: {str(e)}")
        return None

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

async def process_all_programs(cities: List[str], all_programs: Dict[str, Dict[str, str]], snils: str) -> List[Dict]:
    logging.debug(f"Начинаем обработку для {len(cities)} городов.")
    logging.debug(f"Все программы: {all_programs}")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
        tasks = []
        for city in cities:
            if city in all_programs:
                logging.debug(f"Создаем задачу для города: {city}")
                tasks.append(process_city_programs(session, city, all_programs[city], snils))
            else:
                logging.warning(f"Город {city} не найден в списке программ")
        logging.debug(f"Создано {len(tasks)} задач")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logging.debug(f"Получено {len(results)} результатов")
        for city, result in zip(cities, results):
            if isinstance(result, Exception):
                logging.error(f"Ошибка при обработке города {city}: {result}")
    return [item for sublist in results for item in sublist if not isinstance(item, Exception)]

async def save_cached_data(city: str, program: str, df: pd.DataFrame, batch_size: int = 500):
    try:
        df = df.where(pd.notnull(df), None)
        df['позиция'] = pd.to_numeric(df['позиция'], errors='coerce').astype('Int64', errors='ignore')
        df['снилс'] = df['снилс'].astype(str, errors='ignore')
        df['сумма_баллов'] = pd.to_numeric(df['сумма_баллов'], errors='coerce').astype('Int64', errors='ignore')
        df['оригинал'] = df['оригинал'].astype(bool, errors='ignore')

        df['сумма_баллов'].fillna(0, inplace=True)
        df['позиция'].fillna(-1, inplace=True)
        df['снилс'].fillna('Неизвестно', inplace=True)

        data_list = df.to_dict(orient='records')
        for data in data_list:
            data['city'] = city
            data['program'] = program
            data['last_updated'] = datetime.now().isoformat()
            data['position'] = data.pop('позиция', None)
            data['snils'] = data.pop('снилс', None)
            data['total_score'] = data.pop('сумма_баллов', None)
            data['original_document'] = int(data.pop('оригинал', False))

            logging.debug(f"Подготовленные данные для Redis: {data}")

            if None in (data['position'], data['snils'], data['total_score']):
                logging.warning(f"Пропуск некорректных данных: {data}")
                continue

            key = f"hse:{data['snils']}:{city}:{program}"
            logging.debug(f"Сохранение данных в Redis с ключом: {key}")
            await redis.hset(key, mapping=data)

        logging.info(f"Данные сохранены в KeyDB для города {city} и программы {program}")

    except Exception as e:
        logging.error(f"Ошибка при сохранении кэшированных данных: {e}")
        raise

async def process_snils_found_hse(message: Message, state: FSMContext, snils: str):
    logging.debug(f"Проверяем СНИЛС: {snils} для ВШЭ")
    cached_results = await get_user_position(snils, 'hse')

    if cached_results:
        logging.debug(f"Данные найдены в кэше: {cached_results}")
        data_stale = False
        for result in cached_results:
            if is_data_stale(result['last_updated']):
                logging.debug(f"Данные устарели: {result['last_updated']}")
                data_stale = True
                break

        await message.answer('Ваш СНИЛС найден в кэше, и данные актуальны.')
        for result in cached_results:
            last_updated = datetime.fromisoformat(result['last_updated']).strftime('%d.%m %H:%М')
            response = (
                f"<b>Данные актуальны на {last_updated}.</b>\n"
                f"Город: <b>{result['city']}</b>\n"
                f"Программа: <b>{result['program']}</b>\n"
                f"Позиция: <b>{result['position']}</b>\n"
                f"Оригинал: {'<b>Да</b>' if result['original_document'] else '<b>Нет</b>'}"
            )
            await message.answer(response, parse_mode='HTML')

        if data_stale:
            await message.answer('Данные устарели, выполняется обновление...')
            await update_and_notify_user_hse(message, snils)
    else:
        await message.answer('Ваш СНИЛС не найден в кэше, выполняется обновление данных...')
        await update_and_notify_user_hse(message, snils)

async def update_and_notify_user_hse(message: Message, snils: str):
    logging.debug('Обрабатываем ВШЭ...')
    cities = list(hse_programs.keys())
    logging.debug(f"Города ВШЭ: {cities}")
    logging.debug(f"Содержимое hse_programs: {hse_programs}")
    try:
        results = await process_all_programs(cities, hse_programs, snils)
        logging.debug(f"Получены результаты: {results}")

        if results:
            await message.answer('Ваш СНИЛС найден в следующих программах (обновлено):\n')
            for result in results:
                logging.debug(f"Обработка результата: {result}")
                if 'result' in result and result['result']:
                    last_updated = datetime.fromisoformat(result['result']['last_updated']).strftime('%d.%m %H:%М')
                    response = (
                        f"<b>Данные актуальны на {last_updated}.</b>\n"
                        f"Город: <b>{result['city']}</b>\n"
                        f"Программа: <b>{result['program']}</b>\n"
                        f"Позиция: <b>{result['result']['position']}</b>\n"
                        f"Оригинал: {'<b>Да</b>' if result['result']['original_document'] else '<b>Нет</b>'}"
                    )
                    await message.answer(response, parse_mode='HTML')
                    await save_cached_data(result['city'], result['program'], pd.DataFrame([result['result']]))
                else:
                    logging.error(f"Некорректный формат результата: {result}")
        else:
            await message.answer('Ваш СНИЛС не найден ни в одном направлении ВШЭ.')
    except Exception as e:
        logging.error(f"Ошибка при обработке данных ВШЭ: {e}", exc_info=True)
        await message.answer('Произошла ошибка при обработке данных ВШЭ. Пожалуйста, попробуйте позже.')