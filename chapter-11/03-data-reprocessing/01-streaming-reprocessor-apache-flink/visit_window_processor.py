from datetime import datetime
from typing import Iterable

from pyflink.datastream import ProcessWindowFunction

from visit import Visit


class VisitWindowProcessor(ProcessWindowFunction):
    def process(self, visit_id: str, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Visit]) -> Iterable[str]:
        processing_time = datetime.now().isoformat()
        window_start = datetime.fromtimestamp(context.window().start / 1000).isoformat()
        window_end = datetime.fromtimestamp(context.window().end / 1000).isoformat()
        visits = len(elements)
        print(f'context={context.window().max_timestamp()}')
        print(f'context={context.current_watermark()}')
        is_partial = True
        if context.current_watermark() > context.window().max_timestamp():
            is_partial = False
        return [f">> VISIT -{visit_id}- [{processing_time}], range=[{window_start} to {window_end}], visits={visits}, partial={is_partial}"]