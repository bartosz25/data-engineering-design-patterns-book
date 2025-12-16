import datetime
import json
import logging
from typing import Any

from pyflink.common.watermark_strategy import TimestampAssigner


class VisitTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        event = json.loads(value)
        event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp()) * 1000
        logging.getLogger('my_code').info(f'Got {event_time} from {value}')
        return event_time