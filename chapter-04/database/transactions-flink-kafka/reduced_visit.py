import dataclasses
from typing import Dict, Any


@dataclasses.dataclass
class ReducedVisit:
    visit_id: int
    event_time: int


@dataclasses.dataclass
class VisitWithStatus:
    visit: ReducedVisit

    def to_dict(self) -> Dict[str, Any]:
        return {
            'visit': {
                'id': self.visit.visit_id,
                'event_time': self.visit.event_time
            }
        }
