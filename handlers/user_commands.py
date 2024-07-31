import re
import asyncio
from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from keyboards import reply_keyboards
from hse_data import hse_programs
from supabase_db import save_snils, get_snils
from excel_parser import process_all_programs, process_with_timeout

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
        await message.answer('Начинаю проверку всех направлений ВШЭ. Это может занять некоторое время...')
        user_data = await state.get_data()
        snils = user_data['snils']

        cities = list(hse_programs.keys())
        results = await process_with_timeout(cities, hse_programs, snils, timeout=30)

        if results:
            response = 'Ваш СНИЛС найден в следующих программах:\n'
            for r in results:
                response += f"{r['city']} - {r['program']}: позиция {r['result']['position']}, "
                response += f"сумма баллов {r['result']['total_score']}, "
                response += f"оригинал документа: {'Да' if r['result']['original_document'] else 'Нет'}\n"
            await message.answer(response)
        else:
            await message.answer('Ваш СНИЛС не найден ни в одном направлении ВШЭ или произошла ошибка при обработке.')

        await state.clear()
    else:
        await message.answer('Извините, пока доступен только ВШЭ.')