import datetime
import html
import logging
import re
from typing import Final

import aiohttp

from .const import (
    API_KEY_DATE,
    API_KEY_STATUS,
    API_KEY_TODAY,
    API_KEY_TOMORROW,
    API_STATUS_SCHEDULE_APPLIES,
)
from .models import OutageEventType
from .planned import PlannedOutagesApi

LOGGER = logging.getLogger(__name__)

CEK_TELEGRAM_URL: Final = "https://t.me/s/cek_info"

MONTHS_MAP = {
    "СІЧНЯ": 1,
    "ЛЮТОГО": 2,
    "БЕРЕЗНЯ": 3,
    "КВІТНЯ": 4,
    "ТРАВНЯ": 5,
    "ЧЕРВНЯ": 6,
    "ЛИПНЯ": 7,
    "СЕРПНЯ": 8,
    "ВЕРЕСНЯ": 9,
    "ЖОВТНЯ": 10,
    "ЛИСТОПАДА": 11,
    "ГРУДНЯ": 12,
    "січня": 1,
    "лютого": 2,
    "березня": 3,
    "квітня": 4,
    "травня": 5,
    "червня": 6,
    "липня": 7,
    "серпня": 8,
    "вересня": 9,
    "жовтня": 10,
    "листопада": 11,
    "грудня": 12,
}

IGNORED_CITIES = [
    "ЖОВТІ ВОДИ",
    "ВІЛЬНОГІРСЬК",
    "ПАВЛОГРАД",
    "ЗЕЛЕНОДОЛЬСЬК",
    "АПОСТОЛОВЕ",
    "КРИВОРІЗЬК",
]

RE_MESSAGE = re.compile(
    r'<div class="tgme_widget_message_text js-message_text" dir="auto">(.*?)</div>',
    re.DOTALL,
)

# Дата: "29 ГРУДНЯ", "02 грудня"
RE_DATE = re.compile(r"(\d{1,2})\s+([А-ЯІЇЄа-яіїє]+)", re.IGNORECASE)

# Група: 1.1, 2.1
RE_GROUP_STRICT = re.compile(r"📌\s*(\d\.\d)")

# Час: 06:00 - 11:00, 06:00 до 11:00
RE_TIME_RANGE = re.compile(
    r"(\d{1,2}:\d{2})\s*(?:до|по|-)\s*(\d{1,2}:\d{2})", re.IGNORECASE
)

# Слова-маркери повного графіку
FULL_SCHEDULE_MARKERS = [
    "зміни в гпв",
    "графік погодинних відключень",
    "застосовуватимуться відключення наступних черг",
    "графік може змінюватися",
]


class CekPlannedOutagesApi(PlannedOutagesApi):
    """Custom API for CEK provider using Telegram scraping with fallback to Yasno."""

    async def fetch_planned_outages_data(self) -> None:
        """Fetch planned outages data using custom source, fallback to Yasno."""
        try:
            custom_data = await self._fetch_telegram_data()
            if custom_data and self.group in custom_data:
                self.planned_outages_data = custom_data
                LOGGER.debug("Successfully fetched CEK data for group %s", self.group)
                return
            LOGGER.debug("No relevant CEK data found for group %s", self.group)
        except Exception as err:
            LOGGER.warning("Failed to fetch CEK Telegram: %s. Fallback.", err)

        await super().fetch_planned_outages_data()

    async def _fetch_telegram_data(self) -> dict | None:
        async with aiohttp.ClientSession() as session:
            async with session.get(CEK_TELEGRAM_URL) as response:
                if response.status != 200:
                    return None
                html_content = await response.text()

        messages = self._extract_messages(html_content)
        # Parse chronologically (old -> new) for correct state accumulation
        return self._parse_messages_to_schedule(reversed(messages))

    def _extract_messages(self, raw_html: str) -> list[str]:
        messages = []
        for match in RE_MESSAGE.finditer(raw_html):
            text = match.group(1)
            text = text.replace("<br/>", "\n").replace("<br>", "\n")
            text = re.sub(r"<[^>]+>", "", text)
            text = html.unescape(text)
            messages.append(text)
        return messages

    def _parse_messages_to_schedule(self, messages_iter) -> dict:
        schedule = {}  # {group: {day_key: {"date":..., "slots": [...]}}}
        today = datetime.datetime.now().date()
        tomorrow = today + datetime.timedelta(days=1)

        for msg in messages_iter:
            if any(city in msg.upper() for city in IGNORED_CITIES):
                continue

            date_match = RE_DATE.search(msg)
            if not date_match:
                continue

            day_num = int(date_match.group(1))
            month_name = date_match.group(2).upper()
            if month_name not in MONTHS_MAP:
                continue

            month_num = MONTHS_MAP[month_name]

            year = today.year
            if month_num == 1 and today.month == 12:
                year += 1
            elif month_num == 12 and today.month == 1:
                year -= 1

            msg_date = datetime.date(year, month_num, day_num)

            day_key = None
            if msg_date == today:
                day_key = API_KEY_TODAY
            elif msg_date == tomorrow:
                day_key = API_KEY_TOMORROW
            else:
                continue

            # Визначаємо тип повідомлення: повне оновлення чи патч?
            is_full_update = any(m in msg.lower() for m in FULL_SCHEDULE_MARKERS)

            self._parse_message_body(msg, msg_date, day_key, schedule, is_full_update)

        return schedule

    def _parse_message_body(
        self,
        text: str,
        date: datetime.date,
        day_key: str,
        schedule: dict,
        is_full_update: bool,
    ) -> None:
        parts = re.split(r"(📌\s*\d\.\d)", text)
        current_group = None

        for part in parts:
            group_match = RE_GROUP_STRICT.search(part)
            if group_match:
                current_group = group_match.group(1)
                continue

            if current_group and part.strip():
                # Якщо слоти не знайдені (наприклад текст без часу), пропускаємо
                ranges = self._extract_outage_ranges(part)
                if not ranges:
                    continue

                if current_group not in schedule:
                    schedule[current_group] = {}

                # Ініціалізація структури дня, якщо немає
                if day_key not in schedule[current_group]:
                    schedule[current_group][day_key] = {
                        "date": date.isoformat(),
                        "status": API_STATUS_SCHEDULE_APPLIES,
                        "slots": [],  # Починаємо з порожнього
                    }

                current_day_data = schedule[current_group][day_key]

                if "raw_ranges" not in current_day_data:
                    current_day_data["raw_ranges"] = []

                if is_full_update:
                    # Якщо це повний апдейт, ми перезаписуємо слоти
                    current_day_data["raw_ranges"] = ranges
                else:
                    # Patch: додаємо нові діапазони до існуючих
                    current_day_data["raw_ranges"].extend(ranges)

                # Одразу перераховуємо слоти (для сумісності)
                current_day_data["slots"] = self._ranges_to_slots(
                    current_day_data["raw_ranges"]
                )

    def _extract_outage_ranges(self, text: str) -> list[tuple[int, int]]:
        ranges = []
        for match in RE_TIME_RANGE.finditer(text):
            start_str, end_str = match.group(1), match.group(2)
            start_min = self._time_to_minutes(start_str)
            end_min = self._time_to_minutes(end_str)
            if end_min == 0:
                end_min = 1440
            ranges.append((start_min, end_min))
        return ranges

    def _ranges_to_slots(self, ranges: list[tuple[int, int]]) -> list[dict]:
        if not ranges:
            return [
                {"start": 0, "end": 1440, "type": OutageEventType.NOT_PLANNED.value}
            ]

        # Merge overlaps
        ranges.sort()
        merged = []
        if ranges:
            curr_start, curr_end = ranges[0]
            for next_start, next_end in ranges[1:]:
                if next_start <= curr_end:  # Overlap
                    curr_end = max(curr_end, next_end)
                else:
                    merged.append((curr_start, curr_end))
                    curr_start, curr_end = next_start, next_end
            merged.append((curr_start, curr_end))

        final_slots = []
        current_time = 0
        for start, end in merged:
            if start > current_time:
                final_slots.append(
                    {
                        "start": current_time,
                        "end": start,
                        "type": OutageEventType.NOT_PLANNED.value,
                    }
                )
            final_slots.append(
                {
                    "start": start,
                    "end": end,
                    "type": OutageEventType.DEFINITE.value,
                }
            )
            current_time = end

        if current_time < 1440:
            final_slots.append(
                {
                    "start": current_time,
                    "end": 1440,
                    "type": OutageEventType.NOT_PLANNED.value,
                }
            )

        return final_slots

    def _time_to_minutes(self, time_str: str) -> int:
        h, m = map(int, time_str.split(":"))
        return h * 60 + m

