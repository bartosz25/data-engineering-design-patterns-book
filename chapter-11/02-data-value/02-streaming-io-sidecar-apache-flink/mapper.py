import datetime
import json

from visit import Visit


def map_json_to_visit(json_payload: str) -> Visit:
    event = json.loads(json_payload)
    event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp()) * 1000
    return Visit(visit_id=event['visit_id'], event_id=event['event_id'], event_time=event_time, page=event['page'])

