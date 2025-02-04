import datetime
import json

from pyflink.common import Row
from pyflink.common.watermark_strategy import TimestampAssigner


class VisitTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        event = json.loads(value)
        event_time = datetime.datetime.fromisoformat(event['event_time'])
        return int(event_time.timestamp())


class AppendTimeTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        append_time: datetime.datetime = value.append_time
        event_time_for_watermark = int(append_time.timestamp()) * 1000
        return event_time_for_watermark
