import logging
import asyncio
import pandas as pd
import hashlib


from schedule_manager import load_xls_file, get_schedule_link, find_week_sheet, get_schedule_for_all_days
from time_manager import get_next_week_date, get_current_date
from config import SCHEDULE_CHANGING_UPDATE_SECONDS
from telegram_sender import send_to_telegram


def get_hash(content):
    """
    Создание хэша для отслеживания изменений.
    """
    return hashlib.md5(content).hexdigest()


def compare_schedules(old_schedule, new_schedule):
    """
    Функция для сравнения расписаний.
    """
    changes = {}

    for day in new_schedule:
        if day not in old_schedule or old_schedule[day] != new_schedule[day]:
            changes[day] = new_schedule[day]

    return changes


async def check_for_updates():
    """
    Функция для проверки обновлений по ссылке каждый час.
    """
    last_hash = None
    old_schedules = {}

    while True:
        try:
            schedule_url = get_schedule_link()
            if not schedule_url:
                await asyncio.sleep(SCHEDULE_CHANGING_UPDATE_SECONDS)
                continue

            xls_file, content = load_xls_file(schedule_url)
            if xls_file is None or content is None:
                await asyncio.sleep(SCHEDULE_CHANGING_UPDATE_SECONDS)
                continue

            current_hash = get_hash(content)

            if last_hash is None or current_hash != last_hash:
                last_hash = current_hash

                current_week_day = get_current_date()
                next_week_day = get_next_week_date()

                current_week_sheet = find_week_sheet(xls_file, current_week_day)
                next_week_sheet = find_week_sheet(xls_file, next_week_day)

                new_schedules = {}

                if current_week_sheet:
                    df_current_week = pd.read_excel(xls_file, sheet_name=current_week_sheet)
                    new_schedules['current'] = get_schedule_for_all_days(df_current_week)

                if next_week_sheet:
                    df_next_week = pd.read_excel(xls_file, sheet_name=next_week_sheet)
                    new_schedules['next'] = get_schedule_for_all_days(df_next_week)

                for week_type, new_schedule in new_schedules.items():
                    old_schedule = old_schedules.get(week_type)
                    if old_schedule is not None:
                        changes = compare_schedules(old_schedule, new_schedule)
                        if changes:
                            for day, schedule in changes.items():
                                message = f"Обновления на {day.capitalize()} ({week_type} неделя):\n\n{schedule}"
                                await send_to_telegram(message)

                    old_schedules[week_type] = new_schedule

        except Exception as e:
            logging.error(f"Ошибка при проверке изменений: {e}")

        await asyncio.sleep(SCHEDULE_CHANGING_UPDATE_SECONDS)