import os
import re

import io
import openpyxl
import dotenv
from aiogram import Router
from aiogram.client.session import aiohttp
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import Message
from bs4 import BeautifulSoup
from supabase import create_client, Client

from keyboards import reply_keyboards

dotenv.load_dotenv()
router = Router()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# <-----------Пинг бд не работает----------->
# @router.message(Command("ping"))
# async def echo(message: Message):
#     try:
#         response = supabase.table('users').select("*").limit(1).execute()
#
#         if response.status_code == 200:
#             await message.reply('подключение к базе данных успешно установлено')
#
#         else:
#             await message.reply('подключение к базе данных не удалось')
#
#     except Exception as e:
#         await message.reply(f'ошибка: {e}')
# <----------------------------------------->

@router.message(CommandStart())
async def start(message: Message):
    await message.answer('Привет! Этот бот помогает найти себя в конкурсных списках по СНИЛС.',
                         reply_markup=reply_keyboards.start)


@router.message(lambda message: re.match(r'^\d{3}-\d{3}-\d{3} \d{2}$', message.text))
async def echo(message: Message):
    await message.answer('Запомнил твой СНИЛС, теперь выбери ВУЗ:', reply_markup=reply_keyboards.main)


@router.message(Command('help'))
async def help(message: Message):
    await message.answer('/ping - проверка бд\n')


router = Router()


@router.message(Command('parse2'))
async def parse_hse(message: Message, command: CommandObject):
    snils = command.args
    if not snils or not re.match(r'^\d{3}-\d{3}-\d{3} \d{2}$', snils):
        await message.answer("Пожалуйста, введите корректный СНИЛС в формате: /parse2 123-456-789 00")
        return

    url = "https://enrol.hse.ru/storage/public_report_2024/perm/Bachelors/BD_perm_Design_O.xlsx"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                await message.answer("Не удалось получить файл с сайта ВШЭ.")
                return

            content = await response.read()

    # Load the Excel file
    workbook = openpyxl.load_workbook(io.BytesIO(content))
    sheet = workbook.active

    found = False
    for row in sheet.iter_rows(min_row=16, values_only=True):
        if row[1] == snils:
            position = row[0]
            snils = row[1]
            privileged_right = row[22] if len(row) > 22 else "Нет данных"

            result = (f"Позиция: {position}\n"
                      f"СНИЛС: {snils}\n"
                      f"Преимущественное право: {privileged_right}")

            await message.answer(result)
            found = True
            break

    if not found:
        await message.answer(f"СНИЛС {snils} не найден в списке.")

    workbook.close()


@router.message(Command('parse'))
async def parse_guap(message: Message, command: CommandObject):
    snils = command.args
    if not snils or not re.match(r'^\d{3}-\d{3}-\d{3} \d{2}$', snils):
        await message.answer("Пожалуйста, введите корректный СНИЛС в формате: /parse 123-456-789 00")
        return

    url = "https://priem.guap.ru/bach/rating/list_1_20_1_1_1_f"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                await message.answer("Не удалось получить данные с сайта ГУАП.")
                return

            html = await response.text()

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'tablestat1140'})

    if not table:
        await message.answer("Не удалось найти таблицу с данными на странице.")
        return

    rows = table.find_all('tr')[1:]

    found = False
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 11 and cols[1].text.strip() == snils:
            position = cols[0].text.strip()
            priority = cols[2].text.strip()
            total_score = cols[3].text.strip()
            result = f"Позиция: {position}\nСНИЛС: {snils}\nПриоритет: {priority}\nСумма конкурсных баллов: {total_score}"
            await message.answer(result)
            found = True
            break

    if not found:
        await message.answer(f"СНИЛС {snils} не найден в списке.")
