from datetime import datetime, timedelta
import pytz

from config import TIMEZONE


TZ = pytz.timezone(TIMEZONE)
DAYS_TRANSLATION = {
    'monday': 'Понедельник',
    'tuesday': 'Вторник',
    'wednesday': 'Среда',
    'thursday': 'Четверг',
    'friday': 'Пятница',
    'saturday': 'Суббота',
    'sunday': 'Воскресенье'
}


def get_current_date():
    """
    Функция для получения текущей даты.
    """
    return datetime.now(TZ)


def get_next_day_date():
    """
    Функция для получения даты следующего.
    """
    return get_current_date() + timedelta(days=1)


def get_next_week_date():
    """
    Функция для получения даты дня через неделю.
    """
    return get_current_date() + timedelta(days=7)
