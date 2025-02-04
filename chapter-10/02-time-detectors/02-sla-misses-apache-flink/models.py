import dataclasses
from typing import Dict, Any


@dataclasses.dataclass
class ReducedVisit:
    visit_id: int
    event_time: int


@dataclasses.dataclass
class ReducedVisitWrapper:
    start_processing_time_unix_ms: int
    reduced_visit: ReducedVisit


    def to_dict(self) -> Dict[str, Any]:
        import time
        time.sleep(10)
        return {
            'visit': {
                'id': self.reduced_visit.visit_id,
                'event_time': self.reduced_visit.event_time
            },
            'start_processing_time_ms': self.start_processing_time_unix_ms
        }

@dataclasses.dataclass
class PercentileOutput:
    p50: float
    p75: float
    p90: float
    p99: float