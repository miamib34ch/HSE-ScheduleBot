import pandas as pd
from io import BytesIO
import re
import requests
from bs4 import BeautifulSoup
import asyncio
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin

from telegram_sender import send_to_telegram
from config import HOST, TIMETABLE_ENDPOINT, SCHEDULE_FILENAME_PATTERN, GROUP_NAME, SENDING_HOUR, SENDING_MINUTES
from time_manager import TZ, DAYS_TRANSLATION, get_next_day_date, get_current_date


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def escape_html(text):
    """
    Экранирование специальных символов для HTML.
    """
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#039;')


def get_full_link(new_link):
    """
    Функция для добавления протокола и хоста к ссылке, если их нет.
    """
    parsed_url = urlparse(new_link)

    if not parsed_url.netloc:
        full_link = urljoin(HOST, new_link)
    else:
        if not parsed_url.scheme:
            full_link = f"http:{new_link}"
        else:
            full_link = new_link
    return full_link


def get_schedule_link():
    """
    Функция для получения ссылки на файл с расписанием.
    """
    try:
        response = requests.get(TIMETABLE_ENDPOINT)
        soup = BeautifulSoup(response.text, 'html.parser')
        all_links = soup.find_all('a', href=True)

        for link_tag in all_links:
            link_text = link_tag.get_text()

            if re.search(SCHEDULE_FILENAME_PATTERN, link_text, re.IGNORECASE):
                new_link = get_full_link(link_tag['href'])
                return new_link

        logging.error("Не удалось найти ссылку по заданному шаблону.")
        return None

    except Exception as e:
        logging.error(f"Ошибка при получении ссылки: {e}")
        return None


def load_xls_file(url):
    """
    Функция для загрузки файла Excel.
    """
    try:
        response = requests.get(url)
        xls_file = BytesIO(response.content)
        return pd.ExcelFile(xls_file), response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при загрузке файла: {e}")
        return None


def find_week_sheet(xls_file, today):
    """
    Функция для поиска листа текущей недели.
    """
    today_date_only = today.date()
    for sheet_name in xls_file.sheet_names:
        match = re.match(r'(\d{2}\.\d{2})-(\d{2}\.\d{2})', sheet_name)
        if match:
            start_date = datetime.strptime(match.group(1), '%d.%m').replace(year=today.year, tzinfo=TZ).date()
            end_date = datetime.strptime(match.group(2), '%d.%m').replace(year=today.year, tzinfo=TZ).date()

            if end_date < start_date:
                end_date = end_date.replace(year=today.year + 1)

            if start_date <= today_date_only <= end_date:
                return sheet_name
    logging.warning("Лист текущей недели не найден.")
    return None


def find_group_column(df):
    """
    Поиск индекса колонки с нужной группой.
    """
    for col in df.columns:
        if df[col].astype(str).str.contains(GROUP_NAME, case=False, na=False).any():
            return df.columns.get_loc(col)
    logging.warning(f"Колонка с '{GROUP_NAME}' не найдена.")
    return None


def prepare_dates_and_days(df):
    """
    Преобразование первой колонки в даты и определение дней недели.
    """
    try:
        df['Date'] = pd.to_datetime(df.iloc[:, 0], format='%d.%m.%Y', errors='coerce')
        df['Day_of_Week'] = df['Date'].dt.day_name().str.lower()
        df['Day_of_Week'] = df['Day_of_Week'].ffill()
    except Exception as e:
        logging.error(f"Ошибка при преобразовании даты: {e}")
        return None
    return df


def find_matching_rows(df, target_day):
    """
    Поиск строк для целевого дня недели.
    """
    matching_rows = df[df['Day_of_Week'] == target_day].index.tolist()
    if not matching_rows:
        logging.warning(f"Расписание на день не найдено.")
        return None
    return matching_rows


def format_schedule(df, matching_rows, group_column_index):
    """
    Форматирование расписания для дня.
    """
    schedule_info = []
    for row_index in matching_rows:
        time_info = df.iloc[row_index, 2]  # Время занятия в третьей колонке
        activity_info = df.iloc[row_index, group_column_index]

        if pd.notna(activity_info):
            formatted_time = f"<b>{str(time_info)}</b>"
            activity_with_time = f"{formatted_time}\n{escape_html(str(activity_info))}" if pd.notna(
                time_info) else escape_html(str(activity_info))
            schedule_info.append(activity_with_time)

    return schedule_info if schedule_info else None


def get_schedule_for_day(df, today):
    """
    Функция для получения расписания на текущий день.
    """
    target_day = today.strftime('%A').lower()

    group_column_index = find_group_column(df)
    if group_column_index is None:
        return None

    df = prepare_dates_and_days(df)
    if df is None:
        return None

    matching_rows = find_matching_rows(df, target_day)
    if matching_rows is None:
        return None

    schedule_info = format_schedule(df, matching_rows, group_column_index)
    if schedule_info is None:
        return None

    day_name_russian = DAYS_TRANSLATION.get(target_day, target_day).capitalize()
    formatted_date = today.strftime('%d.%m.%Y')

    message = f"<b>Расписание на {day_name_russian}, {formatted_date}</b>\n\n"
    message += "\n\n".join(schedule_info)

    return message


def get_schedule_for_all_days(df):
    """
    Функция для получения расписания все дни недели.
    """
    group_column_index = find_group_column(df)
    if group_column_index is None:
        return None

    df = prepare_dates_and_days(df)
    if df is None:
        return None

    unique_days = df['Day_of_Week'].unique()
    week_schedule = {}

    for day in unique_days:
        matching_rows = find_matching_rows(df, day)
        if matching_rows is None or len(matching_rows) == 0:
            continue

        schedule_info = format_schedule(df, matching_rows, group_column_index)
        if not schedule_info:
            continue

        day_name_russian = DAYS_TRANSLATION.get(day, day).capitalize()

        message = f"<b>Расписание на {day_name_russian}</b>\n\n"
        message += "\n\n".join(schedule_info)

        week_schedule[day_name_russian] = message

    if not week_schedule:
        logging.warning("Расписание на неделю не найдено.")
        return None

    return week_schedule


async def send_daily_schedule():
    """
    Основная функция для получения и отправки расписания.
    """
    today = get_next_day_date() # получаем расписание на следующий день

    schedule_url = get_schedule_link()
    if not schedule_url:
        return

    xls_file, _ = load_xls_file(schedule_url)
    if xls_file is None:
        return

    sheet_name = find_week_sheet(xls_file, today)
    if sheet_name is None:
        return

    df = pd.read_excel(xls_file, sheet_name=sheet_name)
    schedule = get_schedule_for_day(df, today)
    if schedule is None:
        return

    await send_to_telegram(schedule)


async def schedule_notifier():
    """
    Функция для ожидания нужного времени и отправки расписания.
    """
    while True:
        now = get_current_date()
        target_time = now.replace(hour=SENDING_HOUR, minute=SENDING_MINUTES, second=0, microsecond=0)

        if now > target_time:
            target_time += timedelta(days=1)

        wait_time = (target_time - now).total_seconds()
        logging.info(f"Ждём до {target_time}. Ожидание {wait_time} секунд.")
        await asyncio.sleep(wait_time)

        await send_daily_schedule()
