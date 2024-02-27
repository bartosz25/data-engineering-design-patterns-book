import datetime
from typing import Any, Dict, Iterable, List, Tuple, Optional

import pandas
from pyspark.sql.streaming.state import GroupState


def map_visits_to_session(visit_id_tuple: Any,
                          input_rows: Iterable[pandas.DataFrame],
                          current_state: GroupState) -> Iterable[pandas.DataFrame]:
    session_expiration_time_10min_as_ms = 10 * 60 * 1000
    visit_id = visit_id_tuple[0]

    def get_session_to_return(visited_pages: List[Tuple[str, str]], user_id: str) -> Dict[str, Any]:
        sorted_visits = sorted(visited_pages,
                               key=lambda event_time_with_page: event_time_with_page.event_time_as_milliseconds)
        first_event_time_as_milliseconds = sorted_visits[0].event_time_as_milliseconds
        last_event_time_as_milliseconds = sorted_visits[len(sorted_visits) - 1].event_time_as_milliseconds

        duration_in_milliseconds = last_event_time_as_milliseconds - first_event_time_as_milliseconds
        # Using the [...] is required to avoid
        # "ValueError: If using all scalar values, you must pass an index" error
        return {
            "visit_id": [visit_id],
            "start_time": [datetime.datetime.fromtimestamp(first_event_time_as_milliseconds / 1000.0,
                                                           tz=datetime.timezone.utc)],
            "end_time": [datetime.datetime.fromtimestamp(last_event_time_as_milliseconds / 1000.0,
                                                         tz=datetime.timezone.utc)],
            "visited_pages": [sorted_visits],
            "duration_in_milliseconds": [duration_in_milliseconds],
            "user_id": [user_id]
        }

    visit_to_return = None
    if current_state.hasTimedOut:
        print(f"Session ({current_state.get}) expired for {visit_id}; let's generate the final output here")
        visits, user_id, = current_state.get
        visit_to_return = get_session_to_return(visits, user_id)
        current_state.remove()
    else:
        should_use_event_time_for_watermark = current_state.getCurrentWatermarkMs() == 0
        base_watermark = current_state.getCurrentWatermarkMs()
        new_visits = []
        user_id: Optional[str] = None
        for input_df_for_group in input_rows:
            input_df_for_group['event_time_as_milliseconds'] = input_df_for_group['event_time'] \
                .apply(lambda x: int(pandas.Timestamp(x).timestamp()) * 1000)
            if should_use_event_time_for_watermark:
                base_watermark = int(input_df_for_group['event_time_as_milliseconds'].max())

            new_visits = input_df_for_group[['event_time_as_milliseconds', 'page']].to_dict(orient='records')
            user_id = input_df_for_group.at[0, 'user_id']

        visits_so_far = []
        if current_state.exists:
            visits_so_far, user_id, = current_state.get

        print(f'visits_so_far={visits_so_far}')
        visits_for_state = visits_so_far + new_visits
        print(f'Updating {visits_for_state}')
        current_state.update((visits_for_state, user_id,))

        timeout_timestamp = base_watermark + session_expiration_time_10min_as_ms
        current_state.setTimeoutTimestamp(timeout_timestamp)

    if visit_to_return:
        print(visit_to_return)
        yield pandas.DataFrame(visit_to_return)
