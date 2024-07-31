import asyncio
import re
from datetime import datetime

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from excel_parser import process_with_timeout
from hse_data import hse_programs
from keyboards import reply_keyboards
from supabase_db import save_snils, get_snils, get_user_position, is_data_stale

router = Router()

class UserState(StatesGroup):
    waiting_for_snils = State()
    waiting_for_university = State()

@router.message(CommandStart())
async def start(message: Message, state: FSMContext):
    snils = await get_snils(message.from_user.id)
    if snils:
        await message.answer('Добро пожаловать обратно! Выберите ВУЗ:', reply_markup=reply_keyboards.universities)
        await state.set_state(UserState.waiting_for_university)
        await state.update_data(snils=snils)
    else:
        await message.answer('Привет! Этот бот помогает найти себя в конкурсных списках по СНИЛС. Введите ваш СНИЛС:')
        await state.set_state(UserState.waiting_for_snils)

@router.message(UserState.waiting_for_snils)
async def process_snils(message: Message, state: FSMContext):
    if re.match(r'^\d{3}-\d{3}-\d{3} \d{2}$', message.text):
        await save_snils(message.from_user.id, message.text)
        await state.update_data(snils=message.text)
        await message.answer('СНИЛС сохранен. Выберите ВУЗ:', reply_markup=reply_keyboards.universities)
        await state.set_state(UserState.waiting_for_university)
    else:
        await message.answer('Неверный формат СНИЛС. Попробуйте еще раз (например, 123-456-789 00):')

@router.message(UserState.waiting_for_university)
async def process_university(message: Message, state: FSMContext):
    if message.text.lower() == 'вшэ':
        user_data = await state.get_data()
        snils = user_data['snils']
        print(f"Проверяем СНИЛС: {snils}")

        # Проверка данных в базе данных
        cached_results = await get_user_position(snils)
        if cached_results:
            response = 'Ваш СНИЛС найден в следующих программах:\n'
            data_stale = False
            for result in cached_results:
                if is_data_stale(result['last_updated']):
                    data_stale = True
                last_updated = datetime.fromisoformat(result['last_updated']).strftime('%d.%m %H:%M')
                response += (f"{result['city']} - {result['program']}: позиция {result['position']}, "
                             f"сумма баллов {result['total_score']}, "
                             f"оригинал документа: {'Да' if result['original_document'] else 'Нет'} "
                             f"(данные актуальны на {last_updated})\n")
            await message.answer(response)

            if data_stale:
                await message.answer('Данные устарели, выполняется обновление данных. Это может занять некоторое время...')
                # Асинхронное обновление данных в фоновом режиме
                await asyncio.create_task(update_and_notify_user(message, snils))
        else:
            await message.answer('Ваш СНИЛС не найден в кэше, выполняется обновление данных. Это может занять некоторое время...')
            # Асинхронное обновление данных в фоновом режиме
            await asyncio.create_task(update_and_notify_user(message, snils))

        await state.clear()
    else:
        await message.answer('Извините, пока доступен только ВШЭ.')

async def update_and_notify_user(message: Message, snils: str):
    cities = list(hse_programs.keys())
    results = await process_with_timeout(cities, hse_programs, snils, timeout=60)

    if results:
        response = 'Ваш СНИЛС найден в следующих программах (обновлено):\n'
        for r in results:
            last_updated = datetime.now().strftime('%d.%m %H:%M')
            response += (f"{r['city']} - {r['program']}: позиция {r['result']['position']}, "
                         f"сумма баллов {r['result']['total_score']}, "
                         f"оригинал документа: {'Да' if r['result']['original_document'] else 'Нет'} "
                         f"(данные актуальны на {last_updated})\n")
        await message.answer(response)
    else:
        await message.answer('Ваш СНИЛС не найден ни в одном направлении ВШЭ или произошла ошибка при обработке.')
