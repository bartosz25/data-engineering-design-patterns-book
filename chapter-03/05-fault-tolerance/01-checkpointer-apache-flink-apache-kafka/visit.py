import dataclasses


@dataclasses.dataclass
class Visit:
    visit_id: str
    event_time: int
    page: str
