import dataclasses


@dataclasses.dataclass
class Visit:
    event_id: str
    visit_id: int
    event_time: int
    page: str
