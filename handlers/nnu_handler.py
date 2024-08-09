import asyncio
import re
import aiohttp
import logging
from bs4 import BeautifulSoup
import pandas as pd
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from datetime import datetime
from data.keydb import save_unn_cached_data, get_user_position, is_data_stale, get_total_rows, clear_table, get_last_updated, redis
from data.nnu_data import faculties

logging.basicConfig(level=logging.DEBUG)

updating_db = False

async def process_snils_found_nnu(message: Message, state: FSMContext, snils: str):
    global updating_db

    if updating_db:
        await message.answer('База данных обновляется. Пожалуйста, попробуйте через 15 минут.')
        return

    logging.debug(f"Проверяем СНИЛС: {snils} для ННГУ")
    cached_results = await get_user_position(snils, 'unn')

    if cached_results:
        data_stale = any(is_data_stale(result['last_updated']) for result in cached_results)
        await send_cached_results(message, cached_results)

        if data_stale:
            await message.answer('Данные устарели, выполняется обновление...')
            await update_and_notify_user_nnu(message, snils)
    else:
        await message.answer('Ваш СНИЛС не найден в кэше, выполняется обновление данных...')
        await update_and_notify_user_nnu(message, snils)

async def send_cached_results(message: Message, cached_results):
    await message.answer('Ваш СНИЛС найден в кэше, и данные актуальны.')
    for result in cached_results:
        last_updated = datetime.fromisoformat(result['last_updated']).strftime('%d.%m %H:%M')
        response = (
            f"<b>Данные актуальны на {last_updated}.</b>\n"
            f"Факультет: <b>{result['faculty']}</b>\n"
            f"Статус заявления: <b>{result['status']}</b>\n"
            f"Номер: <b>{result['number']}</b>\n"
            f"Тип конкурса: <b>{result['competition_type']}</b>"
        )
        await message.answer(response, parse_mode='HTML')

async def update_all_faculties():
    global updating_db

    try:
        updating_db = True
        await clear_table('unn')  # Очистка таблицы перед обновлением

        for fac_id in faculties.keys():
            query = f'/list/menu.php?list=1&level=1&spec=-1&fac={fac_id}&fin=-1&form=-1'
            url = f"http://abiturient.unn.ru{query}"
            logging.debug(f'Выполняется запрос: GET {url}')
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    html = await response.text()
                    logging.debug(f"Получен ответ для факультета {fac_id}: {html}")
                    specialties = parse_specialties(html)
                    logging.debug(f"Список специальностей для факультета {fac_id}: {specialties}")
                    for spec_id in specialties:
                        await process_specialty(fac_id, spec_id)
            logging.debug(f"Факультет {fac_id} обработан.")
    finally:
        updating_db = False

def parse_specialties(html):
    specialties = {}
    spec_section = re.search(r'<select class="form-control\s+selno"\s+id="spec"[^>]*>(.*?)</select>', html, re.DOTALL)
    if spec_section:
        matches = re.findall(r'<option\s+value="(\d+)">([^<]+)</option>', spec_section.group(1))
        for match in matches:
            if match[0] != "-1":  # Исключаем опцию "Специальность"
                specialties[match[0]] = match[1].strip()
    return specialties

async def process_specialty(fac_id, spec_id):
    fin_ids = ['281474976719885', '281474976719886']
    for fin_id in fin_ids:
        query = f'/list/show.php?spec={spec_id}&level=1&fac={fac_id}&fin={fin_id}&form=0&list=1'
        url = f"http://abiturient.unn.ru{query}"
        logging.debug(f'Выполняется запрос: GET {url}')
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                if len(html.splitlines()) > 400:
                    logging.debug(f"Успешно больше 400 строк для запроса: GET {url}")
                    data = parse_table(html, faculties[fac_id])
                    df = pd.DataFrame(data)
                    await save_unn_cached_data(df)
                else:
                    logging.debug(f"Запрос: GET {url} - Строк меньше 400")
                await asyncio.sleep(0.05)  # Добавляем задержку между запросами

def parse_table(html, faculty):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'jtable'})
    rows = table.find_all('tr')[2:]  # Пропускаем заголовки таблицы

    data = []

    for row in rows:
        cols = row.find_all('td')
        if len(cols) > 1:
            competition_type = cols[0].text.strip()
            number = cols[1].text.strip()
            snils = cols[2].text.strip()
            original_document = 'Да' if 'fa-check' in str(cols[3]) else 'Нет'
            total_score = cols[4].text.strip()
            priority = cols[5].text.strip()

            # Обработка дисциплин
            disciplines = []
            for i in range(6, 10):
                discipline_text = cols[i].text.strip()
                try:
                    discipline_score = int(discipline_text) if discipline_text else None
                except ValueError:
                    discipline_score = None
                disciplines.append(discipline_score)

            status = cols[10].text.strip()

            # Создание записи только если number и total_score не пусты
            entry = {
                "competition_type": competition_type,
                "number": int(number) if number.isdigit() else None,
                "snils": snils,
                "original_document": original_document == 'Да',
                "total_score": int(total_score) if total_score.isdigit() else None,
                "priority": int(priority) if priority.isdigit() else None,
                "disciplines": disciplines,
                "status": status,
                "faculty": faculty
            }

            if entry["number"] is not None and entry["total_score"] is not None:
                data.append(entry)

    return data

def extract_last_updated(html):
    soup = BeautifulSoup(html, 'html.parser')
    match = re.search(r'Время последнего обновления:\s*([\d-]+\s[\d:]+)', html)
    if match:
        return datetime.fromisoformat(match.group(1))
    return None

async def update_and_notify_user_nnu(message: Message, snils: str):
    global updating_db

    logging.debug('Обрабатываем ННГУ им. Лобачевского...')

    latest_update = None
    query = f'/list/show.php?spec=281474976710748&level=1&fac=281474976710809&fin=281474976719886&form=-1&list=1'
    url = f"http://abiturient.unn.ru{query}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
            latest_update = extract_last_updated(html)

    if latest_update is None:
        await message.answer('Не удалось получить дату последнего обновления с сервера.')
        return

    # Получаем дату последнего обновления из базы данных
    last_updated = await get_last_updated('unn')

    if last_updated is None or latest_update > last_updated:
        await message.answer('Выполняется обновление данных...')
        await update_all_faculties()
        await message.answer('Данные успешно обновлены.')

    cached_results = await get_user_position(snils, 'unn')
    if cached_results:
        await send_cached_results(message, cached_results)
    else:
        await message.answer('Ваш СНИЛС не найден.')