import json
from datetime import datetime
from typing import Iterable

from pyflink.datastream import ProcessWindowFunction, OutputTag

from visit import Visit


class VisitWindowProcessor(ProcessWindowFunction):

    def __init__(self, pages_stats_output: OutputTag):
        self.pages_stats_output = pages_stats_output

    def process(self, visit_id: str, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Visit]) -> str:
        processing_time = datetime.now().isoformat()
        window_start = datetime.fromtimestamp(context.window().start / 1000).isoformat()
        window_end = datetime.fromtimestamp(context.window().end / 1000).isoformat()
        visits = len(elements)
        yield f">> VISIT -{visit_id}- [{processing_time}], range=[{window_start} to {window_end}], visits={visits}"

        visited_pages = dict(map(lambda visit: (visit.event_id, visit.page), elements))

        yield self.pages_stats_output, json.dumps({
            'processing_time': processing_time,
            'window_start': window_start, 'window_end': window_end,
            'visited_pages': visited_pages, 'visit_id': visit_id})

