import os
import re

from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from supabase import create_client, Client

from keyboards import reply_keyboards

router = Router()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# <-----------Пинг бд не работает----------->
@router.message(Command("ping"))
async def echo(message: Message):
    try:
        response = supabase.table('users').select("*").limit(1).execute()

        if response.status_code == 200:
            await message.reply('подключение к базе данных успешно установлено')

        else:
            await message.reply('подключение к базе данных не удалось')

    except Exception as e:
        await message.reply(f'ошибка: {e}')
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
