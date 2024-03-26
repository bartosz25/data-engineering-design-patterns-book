from datetime import datetime
from typing import Iterable

from pyflink.datastream import ProcessWindowFunction

from visit import Visit


class VisitWindowProcessor(ProcessWindowFunction):
    def process(self, visit_id: str, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Visit]) -> Iterable[str]:
        window_start = datetime.fromtimestamp(context.window().start / 1000).isoformat()
        window_end = datetime.fromtimestamp(context.window().end / 1000).isoformat()
        visits = len(elements)
        processing_time = datetime.now().isoformat()
        return [f">> VISIT{visit_id}#{processing_time}, range=[{window_start} to {window_end}], visits={visits}"]
