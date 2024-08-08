import asyncio
from datetime import datetime
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from data.hse_data import hse_programs
from data.keydb import get_user_position, is_data_stale, save_cached_data
from parsers.excel_parser import process_with_timeout


async def process_snils_found_hse(message: Message, state: FSMContext, snils: str):
    print(f"Проверяем СНИЛС: {snils} для ВШЭ")
    cached_results = await get_user_position(snils, 'hse')

    if cached_results:
        print(f"Данные найдены в кэше: {cached_results}")
        data_stale = False
        for result in cached_results:
            if is_data_stale(result['last_updated']):
                print(f"Данные устарели: {result['last_updated']}")
                data_stale = True
                break

        await message.answer('Ваш СНИЛС найден в кэше, и данные актуальны.')
        for result in cached_results:
            last_updated = datetime.fromisoformat(result['last_updated']).strftime('%d.%m %H:%M')
            response = (
                f"<b>Данные актуальны на {last_updated}.</b>\n"
                f"Город: <b>{result['city']}</b>\n"
                f"Программа: <b>{result['program']}</b>\n"
                f"Позиция: <b>{result['position']}</b>\n"
                f"Оригинал: {'<b>Да</b>' if result['original_document'] else '<b>Нет</b>'}"
            )
            await message.answer(response, parse_mode='HTML')

        if data_stale:
            await message.answer('Данные устарели, выполняется обновление...')
            await update_and_notify_user_hse(message, snils)
    else:
        await message.answer('Ваш СНИЛС не найден в кэше, выполняется обновление данных...')
        await update_and_notify_user_hse(message, snils)


async def update_and_notify_user_hse(message: Message, snils: str):
    print('Обрабатываем ВШЭ...')
    cities = list(hse_programs.keys())
    results = await process_with_timeout(cities, hse_programs, snils, timeout=60)

    if results:
        await message.answer('Ваш СНИЛС найден в следующих программах (обновлено):\n')
        for result in results:
            last_updated = datetime.fromisoformat(result['last_updated']).strftime('%d.%m %H:%M')
            response = (
                f"<b>Данные актуальны на {last_updated}.</b>\n"
                f"Город: <b>{result['city']}</b>\n"
                f"Программа: <b>{result['program']}</b>\n"
                f"Позиция: <b>{result['position']}</b>\n"
                f"Оригинал: {'<b>Да</b>' if result['original_document'] else '<b>Нет</b>'}"
            )
            await message.answer(response, parse_mode='HTML')
    else:
        await message.answer('Ваш СНИЛС не найден ни в одном направлении ВШЭ или произошла ошибка при обработке.')
