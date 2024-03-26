import datetime
import json

from pyflink.datastream import OutputTag, MapFunction


class MapJsonToReducedVisit(MapFunction):

    def __init__(self, late_data_output: OutputTag):
        self.invalid_data_output = late_data_output

    def map(self, json_payload: str) -> str:
        try:
            event = json.loads(json_payload)
            event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp())
            return (json.dumps({'visit_id': event['visit_id'], 'event_time': event_time,
                               'page': event['page']}))
        except Exception as e:
            return self.invalid_data_output, MapJsonToReducedVisit._wrap_input_with_error(json_payload, e)

    @staticmethod
    def _wrap_input_with_error(input_json: str, error: Exception):
        return json.dumps({
            'processing_time': datetime.datetime.now().isoformat(),
            'input_json': input_json,
            'error_message': str(error)
        })
