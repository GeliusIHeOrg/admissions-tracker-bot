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
    else:
        await message.answer("Используйте СТАРТ для начала работы с ботом.", reply_markup=reply_keyboards.button_start)
        await state.set_state(UserState.waiting_for_START)
