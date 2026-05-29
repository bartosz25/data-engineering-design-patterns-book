import datetime
import json
import logging

from pyflink.datastream import MapFunction, RuntimeContext

from visit import Visit

class JsonToVisitMapper(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        print(
            f"Started the mapping {runtime_context.get_task_name_with_subtasks(),} — subtask {runtime_context.get_index_of_this_subtask()} of {runtime_context.get_number_of_parallel_subtasks()}"
        )

    def map(self, json_payload: str):
        event = json.loads(json_payload)
        event_time = None
        if 'event_time' in event:
            event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp()) * 1000
        return Visit(visit_id=event['visit_id'], user_id=event['user_id'], event_time=event_time, page=event['page'])
