import dataclasses
from typing import Dict, Any


@dataclasses.dataclass
class ReducedVisit:
    visit_id: int
    event_time: int


@dataclasses.dataclass
class VisitWithStatus:
    visit: ReducedVisit
    is_late: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            'visit': {
                'id': self.visit.visit_id,
                'event_time': self.visit.event_time
            },
            'is_late': self.is_late
        }
