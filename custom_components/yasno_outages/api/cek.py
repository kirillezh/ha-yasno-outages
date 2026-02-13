"""API for fetching planned outages from CEK Telegram channel."""

import datetime
import html
import logging
import re
from collections.abc import Iterable
from typing import Final
from zoneinfo import ZoneInfo

import aiohttp

from .const import (
    API_KEY_STATUS,
    API_KEY_TODAY,
    API_KEY_TOMORROW,
    API_STATUS_EMERGENCY_SHUTDOWNS,
    API_STATUS_SCHEDULE_APPLIES,
    API_STATUS_WAITING_FOR_SCHEDULE,
)
from .models import OutageEventType
from .planned import PlannedOutagesApi

LOGGER = logging.getLogger(__name__)

CEK_TELEGRAM_URL: Final = "https://t.me/s/cek_info"

HTTP_OK: Final = 200
MINUTES_IN_DAY: Final = 1440
MONTH_JANUARY: Final = 1
MONTH_DECEMBER: Final = 12

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

# Regex to extract publication time from <time datetime="...">
RE_TIME = re.compile(r'<time datetime="([^"]+)" class="time">')

# Date: "29 ГРУДНЯ", "02 грудня"
RE_DATE = re.compile(
    r"(\d{1,2})\s+([А-ЯІЇЄа-яіїє]+)",  # noqa: RUF001
    re.IGNORECASE,
)

# Group: 1.1, 2.1
# Supports: "📌 1.1", "🔹 Черга 1.1", "📌 Черга 1.1", "🔹 1.1"
# Flexible to handle any emoji before group number
RE_GROUP_STRICT = re.compile(
    r"[📌🔹]\s*(?:[Чч]ерга\s*)?(\d\.\d)"  # noqa: RUF001
)

# Time: 06:00 - 11:00, 06:00 to 11:00
RE_TIME_RANGE = re.compile(
    r"(\d{1,2}:\d{2})\s*(?:до|по|-)\s*(\d{1,2}:\d{2})",
    re.IGNORECASE,
)

# Full schedule markers
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
        # 1. Fetch Yasno data first (for emergency status and fallback)
        yasno_data = None
        try:
            await super().fetch_planned_outages_data()
            yasno_data = self.planned_outages_data
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to fetch Yasno data", exc_info=True)

        # 2. Fetch CEK Telegram data
        cek_data = None
        try:
            cek_data = await self._fetch_telegram_data()
        except aiohttp.ClientError as err:
            LOGGER.warning(
                "Could not fetch CEK Telegram data (network error: %s).",
                err,
            )
        except Exception:  # noqa: BLE001
            LOGGER.warning("Error processing CEK Telegram data.", exc_info=True)

        # 3. Combine logic
        if cek_data and self.group in cek_data:
            # Use CEK data as primary
            self.planned_outages_data = cek_data
            LOGGER.debug("Successfully fetched CEK data for group %s", self.group)

            # Inject Emergency status from Yasno if present
            if yasno_data and self.group in yasno_data:
                self._inject_emergency_status(yasno_data)
        elif yasno_data:
            # Fallback to Yasno
            self.planned_outages_data = yasno_data
            LOGGER.warning(
                "Fallback to Yasno data for group %s "
                "(CEK data not available or group not found)",
                self.group,
            )
        else:
            LOGGER.error("Both Yasno and CEK APIs failed to return data")

    def _inject_emergency_status(self, yasno_data: dict) -> None:
        """Inject emergency status from Yasno data into CEK data."""
        yasno_group = yasno_data[self.group]
        cek_group = self.planned_outages_data[self.group]

        for day_key in (API_KEY_TODAY, API_KEY_TOMORROW):
            if day_key not in yasno_group or day_key not in cek_group:
                continue

            yasno_status = yasno_group[day_key].get(API_KEY_STATUS)
            if yasno_status == API_STATUS_EMERGENCY_SHUTDOWNS:
                LOGGER.info(
                    "Injecting Emergency status from Yasno into CEK data for %s",
                    day_key,
                )
                cek_group[day_key][API_KEY_STATUS] = API_STATUS_EMERGENCY_SHUTDOWNS

    async def _fetch_telegram_data(self) -> dict | None:
        timeout = aiohttp.ClientTimeout(total=60)
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.get(CEK_TELEGRAM_URL) as response,
        ):
            if response.status != HTTP_OK:
                return None
            html_content = await response.text()

        messages = self._extract_messages(html_content)
        # Parse chronologically (old -> new) for correct state accumulation
        return self._parse_messages_to_schedule(reversed(messages))

    def _extract_messages(self, raw_html: str) -> list[tuple[str, str | None]]:
        """Extract messages with their publication time from HTML."""
        messages = []

        # Split by message blocks to correlate text with time
        message_blocks = raw_html.split('class="tgme_widget_message_wrap')

        for block in message_blocks[1:]:  # Skip first empty split
            # Extract text
            text_match = RE_MESSAGE.search(block)
            if not text_match:
                continue

            text = text_match.group(1)
            text = text.replace("<br/>", "\n").replace("<br>", "\n")
            text = re.sub(r"<[^>]+>", "", text)
            text = html.unescape(text)

            # Extract publication time
            time_match = RE_TIME.search(block)
            pub_time = time_match.group(1) if time_match else None

            messages.append((text, pub_time))

        return messages

    def _parse_messages_to_schedule(  # noqa: PLR0912, PLR0915
        self, messages_iter: Iterable[tuple[str, str | None]]
    ) -> dict:
        schedule = {}  # {group: {day_key: {"date":..., "slots": [...]}}}
        # Track latest update time per group
        group_update_times: dict[str, str | None] = {}
        # Use local today, but without tz info as telegram messages don't have year
        today = datetime.datetime.now().date()  # noqa: DTZ005
        tomorrow = today + datetime.timedelta(days=1)

        for msg_text, pub_time in messages_iter:
            if any(city in msg_text.upper() for city in IGNORED_CITIES):
                continue

            date_match = RE_DATE.search(msg_text)
            if not date_match:
                continue

            day_num = int(date_match.group(1))
            month_name = date_match.group(2).upper()
            if month_name not in MONTHS_MAP:
                continue

            month_num = MONTHS_MAP[month_name]

            year = today.year
            if (
                month_num == MONTH_JANUARY and today.month == MONTH_DECEMBER
            ):  # New year transition
                year += 1
            elif (
                month_num == MONTH_DECEMBER and today.month == MONTH_JANUARY
            ):  # Old messages in Jan
                year -= 1

            msg_date = datetime.date(year, month_num, day_num)

            # FIX: Force Kyiv timezone because Telegram messages use Kyiv time
            kyiv_tz = ZoneInfo("Europe/Kyiv")
            dt_aware = datetime.datetime.combine(
                msg_date, datetime.time.min, tzinfo=kyiv_tz
            )
            date_str = dt_aware.isoformat()

            day_key = None
            if msg_date == today:
                day_key = API_KEY_TODAY
            elif msg_date == tomorrow:
                day_key = API_KEY_TOMORROW
            else:
                continue

            # Determine message type: full update or patch?
            is_full_update = any(m in msg_text.lower() for m in FULL_SCHEDULE_MARKERS)

            # Track which groups are updated in this message
            groups_in_message: set[str] = set()

            self._parse_message_body(
                msg_text,
                date_str,
                day_key,
                schedule,
                metadata={"pub_time": pub_time, "is_full_update": is_full_update},
                groups_updated=groups_in_message,
            )

            # Update latest time for each group found in this message
            if pub_time:
                for group in groups_in_message:
                    # Keep the latest (most recent) publication time
                    if group not in group_update_times:
                        group_update_times[group] = pub_time
                    else:
                        # Compare timestamps - keep the later one
                        try:
                            current_time = datetime.datetime.fromisoformat(
                                group_update_times[group]
                            )
                            new_time = datetime.datetime.fromisoformat(pub_time)
                            if new_time > current_time:
                                group_update_times[group] = pub_time
                        except (ValueError, TypeError):
                            # If parsing fails, use the new one
                            group_update_times[group] = pub_time

        # Fill missing today/tomorrow with WaitingForSchedule status
        self._fill_missing_days(schedule, today, tomorrow)

        # Set updatedOn for each group using the latest time
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        for group, latest_time in group_update_times.items():
            if group in schedule:
                schedule[group]["updatedOn"] = latest_time

        # For groups found in messages but without pub_time,
        # use current time as fallback
        # This ensures updatedOn is always set if group has data
        current_time = datetime.datetime.now(tz=kyiv_tz).isoformat()
        for group_data in schedule.values():
            if "updatedOn" not in group_data:
                group_data["updatedOn"] = current_time

        return schedule

    def _fill_missing_days(
        self,
        schedule: dict,
        today: datetime.date,
        tomorrow: datetime.date,
    ) -> None:
        """Fill missing today/tomorrow with WaitingForSchedule status."""
        kyiv_tz = ZoneInfo("Europe/Kyiv")

        for group_data in schedule.values():
            # Add today if it doesn't exist
            if API_KEY_TODAY not in group_data:
                dt_today = datetime.datetime.combine(
                    today, datetime.time.min, tzinfo=kyiv_tz
                )
                group_data[API_KEY_TODAY] = {
                    "date": dt_today.isoformat(),
                    "status": API_STATUS_WAITING_FOR_SCHEDULE,
                    "slots": [],
                }

            # Add tomorrow if it doesn't exist
            if API_KEY_TOMORROW not in group_data:
                dt_tomorrow = datetime.datetime.combine(
                    tomorrow, datetime.time.min, tzinfo=kyiv_tz
                )
                group_data[API_KEY_TOMORROW] = {
                    "date": dt_tomorrow.isoformat(),
                    "status": API_STATUS_WAITING_FOR_SCHEDULE,
                    "slots": [],
                }

    def _parse_message_body(  # noqa: PLR0913
        self,
        text: str,
        date_str: str,
        day_key: str,
        schedule: dict,
        metadata: dict,
        groups_updated: set[str],
    ) -> None:
        # Split by group markers - supports both 📌 and 🔹 formats
        # Flexible pattern to handle any emoji variations
        parts = re.split(
            r"([📌🔹]\s*(?:[Чч]ерга\s*)?\d\.\d)",  # noqa: RUF001
            text,
        )
        current_group = None

        for part in parts:
            group_match = RE_GROUP_STRICT.search(part)
            if group_match:
                current_group = group_match.group(1)
                continue

            if current_group and part.strip():
                # If slots are not found (e.g. text without time), skip
                ranges = self._extract_outage_ranges(part)
                if not ranges:
                    continue

                if current_group not in schedule:
                    schedule[current_group] = {}

                # Initialize day structure if it doesn't exist
                if day_key not in schedule[current_group]:
                    schedule[current_group][day_key] = {
                        "date": date_str,
                        "status": API_STATUS_SCHEDULE_APPLIES,
                        "slots": [],  # Start with empty
                    }

                current_day_data = schedule[current_group][day_key]

                if "raw_ranges" not in current_day_data:
                    current_day_data["raw_ranges"] = []

                # Track that this group was updated in this message
                groups_updated.add(current_group)

                is_full_update = metadata.get("is_full_update", False)
                if is_full_update:
                    # If this is a full update, we overwrite the slots
                    current_day_data["raw_ranges"] = ranges
                else:
                    # Patch: add new ranges to existing ones
                    current_day_data["raw_ranges"].extend(ranges)

                # Immediately recalculate slots (for compatibility)
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
                end_min = MINUTES_IN_DAY
            ranges.append((start_min, end_min))
        return ranges

    def _ranges_to_slots(self, ranges: list[tuple[int, int]]) -> list[dict]:
        if not ranges:
            return [
                {
                    "start": 0,
                    "end": MINUTES_IN_DAY,
                    "type": OutageEventType.NOT_PLANNED.value,
                }
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

        if current_time < MINUTES_IN_DAY:
            final_slots.append(
                {
                    "start": current_time,
                    "end": MINUTES_IN_DAY,
                    "type": OutageEventType.NOT_PLANNED.value,
                }
            )

        return final_slots

    def _time_to_minutes(self, time_str: str) -> int:
        h, m = map(int, time_str.split(":"))
        return h * 60 + m
