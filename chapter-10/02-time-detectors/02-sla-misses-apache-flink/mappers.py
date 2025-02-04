import datetime
import json

from models import ReducedVisitWrapper, ReducedVisit


def map_json_to_reduced_visit(json_payload: str) -> str:
    import time
    event = json.loads(json_payload)
    event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp())
    return json.dumps(ReducedVisitWrapper(
        start_processing_time_unix_ms=time.time_ns() // 1_000_000,
        reduced_visit=ReducedVisit(visit_id=event['visit_id'], event_time=event_time)).to_dict())
