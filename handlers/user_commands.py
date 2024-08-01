import re
from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from data.supabase_db import save_snils, get_snils
from keyboards import reply_keyboards
from handlers.nnu_handler import process_snils_found_nnu  # Убедитесь, что имя и путь правильные
from handlers.hse_handler import process_snils_found_hse

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
    user_data = await state.get_data()
    snils = user_data['snils']
    university = message.text.lower()

    if university == 'ннгу им. лобачевского':
        await process_snils_found_nnu(message, state, snils)
    elif university == 'вшэ':
        await process_snils_found_hse(message, state, snils)
    else:
        await message.answer('Извините, пока доступны только ВШЭ и ННГУ им. Лобачевского.')
