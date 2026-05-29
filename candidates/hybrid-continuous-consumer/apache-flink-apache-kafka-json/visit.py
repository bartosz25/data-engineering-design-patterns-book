import dataclasses


@dataclasses.dataclass
class Visit:
    visit_id: int
    user_id: str
    event_time: int | None
    page: str
