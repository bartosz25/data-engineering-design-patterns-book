import datetime
import json
from typing import Iterable, Optional

from pyflink.datastream import ProcessWindowFunction
from pyflink.datastream.functions import IN


class VisitToSessionConverter(ProcessWindowFunction):

    def process(self, key: str, context: 'ProcessWindowFunction.Context', elements: Iterable[IN]) -> Iterable[str]:
        # the function generates the final output; it's called only once the session window closes
        user_id: Optional[str] = None
        visits_dicts = []
        for element in elements:
            visit_dict = json.loads(element)
            visits_dicts.append({'page': visit_dict['page'], 'event_time': visit_dict['event_time']})
            user_id = visit_dict['user_id']
        sorted_visits = sorted(visits_dicts, key=lambda entry: entry['event_time'])
        first_visit = sorted_visits[0]
        last_visit = sorted_visits[len(sorted_visits) - 1]

        watermark_in_epoch_seconds = context.current_watermark() / 1000

        output_session = {
            "visit_id": key,
            "start_time": first_visit['event_time'],
            "end_time": last_visit['event_time'],
            "visited_pages": [sorted_visits],
            "user_id": [user_id],
            "data_processing_context": {
                "generation_time": datetime.datetime.utcnow().isoformat(sep='T'),
                "watermark": (datetime.datetime.fromtimestamp(watermark_in_epoch_seconds, tz=datetime.timezone.utc)
                              .isoformat(sep='T'))
            }
        }
        yield json.dumps(output_session)
