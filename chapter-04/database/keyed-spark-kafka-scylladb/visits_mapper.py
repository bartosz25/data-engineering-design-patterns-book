from typing import Any, Iterable, Optional

import pandas
from pyspark.sql.streaming.state import GroupState


def map_visit_to_session(user_tuple: Any,
                         input_rows: Iterable[pandas.DataFrame],
                         current_state: GroupState) -> Iterable[pandas.DataFrame]:
    session_expiration_time_50_seconds_as_ms = 50 * 1000
    user_id = user_tuple[0]

    session_to_return = None
    if current_state.hasTimedOut:
        print(f"Session ({current_state.get}) expired for {user_id}; let's generate the final output here")
        min_append_time, pages, = current_state.get
        session_to_return = {
            'user_id': [user_id],
            'session_id': [hash(str(min_append_time))],
            'pages': [pages]
        }
        current_state.remove()
    else:
        should_use_event_time_for_watermark = current_state.getCurrentWatermarkMs() == 0
        base_watermark = current_state.getCurrentWatermarkMs()
        data_min_append_time: Optional[int] = None
        pages = []
        for input_df_for_group in input_rows:
            pages = pages + list(input_df_for_group['page'])
            if should_use_event_time_for_watermark:
                base_watermark = int(input_df_for_group['event_time'].max().timestamp()) * 1000
            if not current_state.exists:
                data_min_append_time = int(input_df_for_group['append_time'].min()) * 1000

        timeout_timestamp = base_watermark + session_expiration_time_50_seconds_as_ms
        current_state.setTimeoutTimestamp(timeout_timestamp)
        print(f'Set timeout to {timeout_timestamp}')
        if current_state.exists:
            min_append_time, current_pages, = current_state.get
            visited_pages = current_pages + pages
            current_state.update((min_append_time, visited_pages,))
        else:
            current_state.update((data_min_append_time, pages,))

    if session_to_return:
        print(f'Returning {session_to_return}')
        yield pandas.DataFrame(session_to_return)
