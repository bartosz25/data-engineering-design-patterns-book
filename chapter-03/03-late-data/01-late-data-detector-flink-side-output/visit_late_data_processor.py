import json

from pyflink.datastream import ProcessFunction, OutputTag

from reduced_visit import ReducedVisit, VisitWithStatus


class VisitLateDataProcessor(ProcessFunction):

    def __init__(self, late_data_output: OutputTag):
        self.late_data_output = late_data_output

    def process_element(self, value: ReducedVisit, ctx: 'ProcessFunction.Context'):
        current_watermark = ctx.timer_service().current_watermark()
        if current_watermark > value.event_time:
            yield self.late_data_output, json.dumps(VisitWithStatus(visit=value, is_late=True).to_dict())
        else:
            yield json.dumps(VisitWithStatus(visit=value, is_late=False).to_dict())
