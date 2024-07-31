from aiogram import Router
from aiogram.types import Message

from keyboards import reply_keyboards

router = Router()


@router.message()
async def echo(message: Message):
    msg = message.text.lower()
    # всё, с чем сравниваем msg пишем с маленькой буквы,
    # так как принимаем маленькими буквами - .lower()

    if msg == "ввести снилс":
        await message.answer("Введите свой СНИЛС в формате: 123-456-789 00")

    elif msg == "выбрать вуз":
        await message.answer("Список вузов:", reply_markup=reply_keyboards.universities)

    elif msg == 'кфу':
        pass

    elif msg == 'вшэ':
        pass

    elif msg == 'ннгу им. лобачевского':
        pass

    elif msg == 'гуап':
        pass
