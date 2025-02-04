import datetime
from typing import Iterable, Any

from pyflink.datastream import TimeWindow
from pyflink.datastream.functions import KEY, WindowFunction
from pytz import utc

from models import PercentileOutput


class PercentilesOutputWindowFormatter(WindowFunction):

    def apply(self, key: KEY, window: TimeWindow, inputs: Iterable[PercentileOutput]) -> Iterable[dict[str, Any]]:
        outputs = []
        for percentile in inputs:
            start = datetime.datetime.fromtimestamp(window.start / 1000, tz=utc)
            observation_dump = {
                '@timestamp': start.isoformat(),
                'p50': percentile.p50,
                'p75': percentile.p75,
                'p90': percentile.p90,
                'p99': percentile.p99
            }
            outputs.append(observation_dump)
        return outputs