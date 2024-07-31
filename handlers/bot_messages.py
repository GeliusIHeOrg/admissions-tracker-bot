from aiogram import Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from .user_commands import UserState
from keyboards import reply_keyboards

router = Router()

@router.message()
async def echo(message: Message, state: FSMContext):
    current_state = await state.get_state()

    if current_state == UserState.waiting_for_snils:
        await message.answer("Пожалуйста, введите свой СНИЛС в формате: 123-456-789 00")
    elif current_state == UserState.waiting_for_university:
        await message.answer("Пожалуйста, выберите ВУЗ из списка:", reply_markup=reply_keyboards.universities)
    elif current_state == UserState.waiting_for_hse_city:
        await message.answer("Пожалуйста, выберите город ВШЭ из списка:", reply_markup=reply_keyboards.hse_cities)
    elif current_state == UserState.waiting_for_hse_program:
        await message.answer("Пожалуйста, выберите программу из списка.")
    else:
        await message.answer("Используйте /start для начала работы с ботом.")