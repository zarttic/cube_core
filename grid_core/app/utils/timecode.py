from datetime import datetime, timezone

from grid_core.app.core.enums import TimeGranularity
from grid_core.app.core.exceptions import ValidationError


TIME_FORMATS = {
    TimeGranularity.SECOND: "%Y%m%d%H%M%S",
    TimeGranularity.MINUTE: "%Y%m%d%H%M",
    TimeGranularity.HOUR: "%Y%m%d%H",
    TimeGranularity.DAY: "%Y%m%d",
    TimeGranularity.MONTH: "%Y%m",
}


def normalize_to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def to_time_code(timestamp: datetime, granularity: TimeGranularity) -> str:
    if granularity not in TIME_FORMATS:
        raise ValidationError(f"Unsupported time granularity: {granularity}")
    ts = normalize_to_utc(timestamp)
    return ts.strftime(TIME_FORMATS[granularity])
