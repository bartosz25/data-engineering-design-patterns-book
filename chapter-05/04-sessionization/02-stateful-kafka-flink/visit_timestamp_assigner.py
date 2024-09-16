import datetime
import json
from typing import Any

from pyflink.common.watermark_strategy import TimestampAssigner


class VisitTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        event = json.loads(value)
        event_time = datetime.datetime.fromisoformat(event['event_time'])
        return int(event_time.timestamp())*1000
