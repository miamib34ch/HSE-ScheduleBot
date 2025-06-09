from telegram import Bot
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL


async def send_to_telegram(message):
    """
    Отправка в Telegram.
    """
    bot = Bot(token=TELEGRAM_TOKEN)

    MAX_MESSAGE_LENGTH = 4096
    if len(message) > MAX_MESSAGE_LENGTH:
        for i in range(0, len(message), MAX_MESSAGE_LENGTH):
            await bot.send_message(chat_id=TELEGRAM_CHANNEL, text=message[i:i+MAX_MESSAGE_LENGTH], parse_mode='HTML')
    else:
        await bot.send_message(chat_id=TELEGRAM_CHANNEL, text=message, parse_mode='HTML')
