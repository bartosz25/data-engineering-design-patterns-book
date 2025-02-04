import datetime
from typing import List, Any

from numpy import ndarray, dtype, floating, percentile
from pyflink.common import Row
from pyflink.datastream import AggregateFunction

from models import PercentileOutput


def extract_grouping_key(value: Row) -> datetime.datetime:
    visit_time_as_minute: datetime.datetime = value.visit_time_minute
    return visit_time_as_minute

class PercentilesAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> List[int]:
        return []

    def add(self, value: Row, slas_accumulator: List[int]) -> List[int]:
        slas_accumulator.append(value.time_difference)
        return slas_accumulator

    def get_result(self, slas_accumulator: List[int]) -> PercentileOutput:
        percentiles: ndarray[Any, dtype[floating[Any]]] = percentile(a=slas_accumulator, q=[50, 75, 90, 99])
        return PercentileOutput(
            p50=percentiles[0].item(), p75=percentiles[1].item(),
            p90=percentiles[2].item(), p99=percentiles[3].item()
        )

    def merge(self, list_1: List[int], list_2: List[int]) -> List[int]:
        merged_list = list_1 + list_2
        return merged_list
