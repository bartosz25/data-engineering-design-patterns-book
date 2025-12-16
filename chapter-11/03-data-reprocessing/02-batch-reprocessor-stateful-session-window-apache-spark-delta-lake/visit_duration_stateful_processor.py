import datetime
from typing import Any, Dict, Iterator

import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import TimerValues, ExpiredTimerInfo
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, BooleanType


class VisitsDurationCalculator(StatefulProcessor):

    OUTPUT_SCHEMA = StructType([
        StructField('visit_id', StringType()), StructField('visited_pages', IntegerType()),
        StructField('is_finished', BooleanType())
    ])
    STATE_SCHEMA = StructType([
        StructField('visited_pages', IntegerType()),
        StructField('first_event_time', TimestampType()),
        StructField('previous_event_time', TimestampType())
    ])
    STATE_EXPIRATION_TIME_10_MIN_AS_SECONDS = 10 * 60
    STATE_EXPIRATION_TIME_10_MIN_AS_MS = STATE_EXPIRATION_TIME_10_MIN_AS_SECONDS * 1000

    def __init__(self, is_reprocessing: bool):
        self.is_reprocessing = is_reprocessing
        self.finished_sessions_only = is_reprocessing

    def _generate_visit_counter(self, user_id: Any, first_visit_time: datetime.datetime,
                                visited_pages_to_return: int, is_finished: bool) -> Dict[str, Any]:
        # Using the [...] is required to avoid
        # "ValueError: If using all scalar values, you must pass an index" error
        visit_id = f'{first_visit_time.strftime("%Y%m%d_%H%M%S")}-{user_id}'
        return {
            "visit_id": [visit_id],
            "visited_pages": [visited_pages_to_return],
            "is_finished": [is_finished]
        }

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.duration_state = handle.getValueState("durationState", self.STATE_SCHEMA)
        self.handle = handle

    def _get_value_state(self) -> (int, datetime.datetime, datetime.datetime):
        current_state = self.duration_state.get()
        visited_pages = current_state[0]
        first_event_time = current_state[1]
        previous_event_time = current_state[2]
        return visited_pages, first_event_time, previous_event_time

    def handleInputRows(self, key_tuple: Any, input_rows: Iterator["PandasDataFrameLike"], timerValues: TimerValues) -> Iterator[
        "PandasDataFrameLike"]:
        user_id = key_tuple[0]
        visited_pages = 0
        first_event_time: datetime.datetime | None = None
        previous_event_time: pd.Timedelta | None = None
        if self.duration_state.exists():
            print(f'Retrieving the state for {user_id}')
            visited_pages, first_event_time, previous_event_time = self._get_value_state()

        last_event_timestamp_from_input = 0
        for input_df_for_group in input_rows:
            visited_pages_sorted = input_df_for_group.sort_values(by='event_time', ascending=True)
            for visit_event in visited_pages_sorted.to_dict(orient='records'):
                current_event_time = visit_event['event_time']
                if not previous_event_time:
                    previous_event_time: pd.Timedelta = current_event_time
                    first_event_time = current_event_time

                events_time_difference = current_event_time - previous_event_time
                if events_time_difference.total_seconds() > self.STATE_EXPIRATION_TIME_10_MIN_AS_SECONDS:
                    print(f'Starting new session. The difference between the event times are {events_time_difference}')
                    yield pd.DataFrame(self._generate_visit_counter(
                        user_id=user_id, first_visit_time=first_event_time, is_finished=True,
                        visited_pages_to_return=visited_pages
                    ))
                    visited_pages = 1
                    previous_event_time = current_event_time
                    first_event_time = current_event_time
                else:
                    visited_pages += 1
                    previous_event_time: pd.Timedelta = current_event_time
                last_event_timestamp_from_input = current_event_time

        if not self.is_reprocessing:
            # We don't need to deal with the stateful API for the reprocessing job as it leverages the batch
            # semantics only and this batch semantics doesn't implement any stateful logic
            self.duration_state.update((visited_pages, first_event_time, previous_event_time,))
            print(f'Updated state to {visited_pages} and {last_event_timestamp_from_input} ')

            base_event_time_for_timeout = timerValues.getCurrentWatermarkInMs()
            if base_event_time_for_timeout == 0:
                base_event_time_for_timeout = int(pd.Timestamp(last_event_timestamp_from_input).timestamp())*1000
                print(f'Setting watermark from the event times ot {base_event_time_for_timeout}')

            print(f'watermark is {timerValues.getCurrentWatermarkInMs()}')
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            state_expiration_time = base_event_time_for_timeout + self.STATE_EXPIRATION_TIME_10_MIN_AS_MS
            self.handle.registerTimer(state_expiration_time)
            print(f'Set a new state expiration time to {state_expiration_time}')


        yield pd.DataFrame(self._generate_visit_counter(
            user_id=user_id, first_visit_time=first_event_time, is_finished=self.finished_sessions_only,
            visited_pages_to_return=visited_pages
        ))


    def handleExpiredTimer(
        self, key: Any, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo
    ) -> Iterator["PandasDataFrameLike"]:
        visited_pages, first_event_time, previous_event_time = self._get_value_state()
        user_id = key[0]
        print(f'Handling expired state for {user_id}')
        yield pd.DataFrame(self._generate_visit_counter(
            user_id=user_id, first_visit_time=first_event_time, is_finished=True, visited_pages_to_return=visited_pages
        ))
