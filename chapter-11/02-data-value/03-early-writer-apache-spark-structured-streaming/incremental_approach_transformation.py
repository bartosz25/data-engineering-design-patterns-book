from typing import Any, Dict, Iterator

import pandas
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import TimerValues, ExpiredTimerInfo
from pyspark.sql.types import StructType, StructField, IntegerType


class VisitsDurationCalculator(StatefulProcessor):

    STATE_SCHEMA = StructType([
        StructField("visited_pages", IntegerType()),
    ])
    STATE_EXPIRATION_TIME_10_MIN_AS_MS = 10 * 60 * 1000

    def _generate_visit_counter(self, visit_id: Any, visited_pages_to_return: int, is_finished: bool) -> Dict[str, Any]:
        # Using the [...] is required to avoid
        # "ValueError: If using all scalar values, you must pass an index" error
        return {
            "visit_id": [visit_id],
            "visited_pages": [visited_pages_to_return],
            "is_finished": [is_finished]
        }

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.duration_state = handle.getValueState("durationState", self.STATE_SCHEMA)
        self.handle = handle

    def handleInputRows(self, key_tuple: Any, input_rows: Iterator["PandasDataFrameLike"], timerValues: TimerValues) -> Iterator[
        "PandasDataFrameLike"]:
        visit_id = key_tuple[0]
        visited_pages = 0
        if self.duration_state.exists():
            current_state = self.duration_state.get()
            visited_pages = current_state[0]

        last_event_timestamp_from_input = -1
        for input_df_for_group in input_rows:
            visited_pages += len(input_df_for_group.index)
            input_df_for_group['event_time_as_milliseconds'] = (input_df_for_group['event_time']
                .apply(lambda x: int(pandas.Timestamp(x).timestamp())*1000))
            last_event_timestamp_from_input = max(last_event_timestamp_from_input,
                                                  input_df_for_group['event_time_as_milliseconds'].max())
        self.duration_state.update((visited_pages,))
        print(f'Updated state to {visited_pages} and {last_event_timestamp_from_input} ')

        base_event_time_for_timeout = timerValues.getCurrentWatermarkInMs()
        if base_event_time_for_timeout == 0:
            base_event_time_for_timeout = last_event_timestamp_from_input
        print(f'watermark is {timerValues.getCurrentWatermarkInMs()}')
        for timer in self.handle.listTimers():
            self.handle.deleteTimer(timer)
        state_expiration_time = base_event_time_for_timeout + self.STATE_EXPIRATION_TIME_10_MIN_AS_MS
        self.handle.registerTimer(state_expiration_time)
        print(f'Set a new state expiration time to {state_expiration_time}')

        yield pandas.DataFrame(self._generate_visit_counter(
            visit_id=visit_id, is_finished=False, visited_pages_to_return=visited_pages
        ))


    def handleExpiredTimer(
        self, key: Any, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo
    ) -> Iterator["PandasDataFrameLike"]:
        expired_state = self.duration_state.get()
        visit_id = key[0]
        print(f'Handling expired state for {visit_id}')
        yield pandas.DataFrame(self._generate_visit_counter(
            visit_id=visit_id, is_finished=True, visited_pages_to_return=expired_state[0]
        ))


"""
def generate_visit_duration(visit_id_tuple: Any,
                            input_rows: Iterable[pandas.DataFrame],
                            currentState: GroupState) -> Iterable[pandas.DataFrame]:
    session_expiration_time_10min_as_ms = 10 * 60 * 1000
    visit_id = visit_id_tuple[0]

    def generate_visit_counter(visited_pages_to_return: int, is_finished: bool) -> Dict[str, Any]:
        # Using the [...] is required to avoid
        # "ValueError: If using all scalar values, you must pass an index" error
        return {
            "visit_id": [visit_id],
            "visited_pages": [visited_pages_to_return],
            "is_finished": [is_finished]
        }

    if currentState.hasTimedOut:
        print(f"Session ({currentState.get}) expired for {visit_id}; let's generate the final output here")
        visited_pages, _ = currentState.get
        visit_counter = generate_visit_counter(visited_pages, True)
        currentState.remove()
    else:
        visited_pages = 0
        last_event_timestamp_from_input = 0
        for input_df_for_group in input_rows:
            visited_pages += len(input_df_for_group.index)
            input_df_for_group['event_time_as_milliseconds'] = input_df_for_group['event_time']\
                .apply(lambda x: int(pandas.Timestamp(x).timestamp())*1000)
            last_event_timestamp_from_input = max(last_event_timestamp_from_input,
                                                  input_df_for_group['event_time_as_milliseconds'].max())

        latest_timestamp_for_state = int(last_event_timestamp_from_input)
        if currentState.exists:
            old_count, latest_event_timestamp = currentState.get
            visited_pages += old_count
            latest_timestamp_for_state = max(latest_event_timestamp, latest_timestamp_for_state)
        visit_counter = generate_visit_counter(visited_pages, False)

        currentState.update((visited_pages, latest_timestamp_for_state))

        timeout_timestamp = latest_timestamp_for_state + session_expiration_time_10min_as_ms
        currentState.setTimeoutTimestamp(timeout_timestamp)

    yield pandas.DataFrame(visit_counter)
"""