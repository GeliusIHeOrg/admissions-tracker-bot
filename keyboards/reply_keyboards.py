from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

start = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text='Ввести СНИЛС'),]
    ],
    one_time_keyboard=True,
    resize_keyboard=True,
    selective=True,
)

main = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text='Выбрать ВУЗ'),]
    ],
    one_time_keyboard=True,
    resize_keyboard=True,
    selective=True,
)

universities = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text='ГУАП'), KeyboardButton(text='ННГУ им. Лобачевского')],
        [KeyboardButton(text='КФУ'), KeyboardButton(text='ВШЭ')]
    ],
    one_time_keyboard=False,
    resize_keyboard=True,
    selective=True,
)

start_button = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text='СТАРТ')],
    ],
    one_time_keyboard=False,
    resize_keyboard=True,
    selective=True,
)

def create_programs_keyboard(programs):
    keyboard = [[KeyboardButton(text=program)] for program in programs]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        one_time_keyboard=False,
        resize_keyboard=True,
        selective=True,
    )