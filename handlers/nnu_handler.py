import asyncio
import re
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from datetime import datetime
from data.supabase_db import save_unn_cached_data, get_user_position, is_data_stale, get_total_rows
from data.nnu_data import faculties

async def process_snils_found_nnu(message: Message, state: FSMContext, snils: str):
    print(f"Проверяем СНИЛС: {snils} для ННГУ")
    cached_results = await get_user_position(snils, 'cache_unn')

    if cached_results and not any(is_data_stale(result['last_updated']) for result in cached_results):
        await send_cached_results(message, cached_results)
    else:
        total_rows = await get_total_rows('cache_unn')
        if total_rows < 5:
            await message.answer('Выполняется обновление данных...')
            await update_all_faculties()
        else:
            await update_and_notify_user_nnu(message, snils)

async def send_cached_results(message: Message, cached_results):
    await message.answer('Ваш СНИЛС найден в кэше, и данные актуальны.')
    for result in cached_results:
        last_updated = datetime.fromisoformat(result['last_updated']).strftime('%d.%м %H:%М')
        response = (
            f"<b>Данные актуальны на {last_updated}.</b>\n"
            f"Факультет: <b>{faculties[result['fac']]}</b>\n"
            f"Специальность: <b>{result['spec_name']}</b>\n"
            f"Финансирование: <b>{result['fin']}</b>\n"
            f"Форма обучения: <b>{result['form']}</b>\n"
            f"Позиция: <b>{result['position']}</b>\n"
            f"Оригинал: {'<b>Да</b>' if result['original_document'] else '<b>Нет</b>'}"
        )
        await message.answer(response, parse_mode='HTML')

async def update_all_faculties():
    for fac_id in faculties.keys():
        query = f'/list/menu.php?list=1&level=1&spec=-1&fac={fac_id}&fin=-1&form=-1'
        url = f"http://abiturient.unn.ru{query}"
        print(f'Выполняется запрос: GET {url}')
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                print(f"Получен ответ для факультета {fac_id}: {html}")
                specialties = parse_specialties(html)
                print(f"Список специальностей для факультета {fac_id}: {specialties}")
                for spec_id in specialties:
                    await process_specialty(fac_id, spec_id)
        print(f"Факультет {fac_id} обработан.")

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
        print(f'Выполняется запрос: GET {url}')
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                if len(html.splitlines()) > 400:
                    print(f"Успешно больше 400 строк для запроса: GET {url}")
                    data = parse_table(html)
                    df = pd.DataFrame(data)
                    await save_unn_cached_data(df)
                else:
                    print(f"Запрос: GET {url} - Строк меньше 400")
                await asyncio.sleep(1)  # Добавляем задержку между запросами

def parse_table(html):
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
                "status": status
            }

            if entry["number"] is not None and entry["total_score"] is not None:
                data.append(entry)

    return data

async def update_and_notify_user_nnu(message: Message, snils: str):
    print('Обрабатываем ННГУ им. Лобачевского...')
    await update_all_faculties()
    await message.answer('Данные успешно обновлены.')
    cached_results = await get_user_position(snils, 'cache_unn')
    if cached_results:
        await send_cached_results(message, cached_results)
    else:
        await message.answer('Ваш СНИЛС не найден.')
