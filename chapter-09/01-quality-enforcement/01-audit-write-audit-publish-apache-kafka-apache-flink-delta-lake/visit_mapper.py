import json
from typing import Union

from pyflink.datastream import OutputTag, MapFunction


class VisitAuditor(MapFunction):

    REQUIRED_FIELDS = ['event_time', 'user_id', 'page', 'context']

    def __init__(self, late_data_output: OutputTag):
        self.invalid_data_output = late_data_output

    def map(self, json_payload: str) -> Union[str, tuple[OutputTag, str]]:
        is_valid = True
        event = json.loads(json_payload)
        for field in self.REQUIRED_FIELDS:
            if field not in event or not event[field]:
                is_valid = False
                break

        if is_valid:
            return json_payload
        else:
            return self.invalid_data_output, json_payload
