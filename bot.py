import asyncio
import os

import dotenv
from aiogram import Bot, Dispatcher, Router

from handlers import user_commands, bot_messages

dotenv.load_dotenv()
router = Router()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")


async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()

    dp.include_routers(
        user_commands.router,
        bot_messages.router  # ВАЖНО, этот роутер должен стоять в конце всегда, потому что там обработчик всех сообщений
    )

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
