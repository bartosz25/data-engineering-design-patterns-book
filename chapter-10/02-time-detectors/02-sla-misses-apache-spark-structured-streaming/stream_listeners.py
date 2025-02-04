from pyspark.sql.streaming.listener import QueryProgressEvent, QueryIdleEvent, QueryStartedEvent, QueryTerminatedEvent, \
    StreamingQueryListener
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

class BatchCompletionSlaListener(StreamingQueryListener):

    def __init__(self):
        pass

    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        try:
            registry = CollectorRegistry()
            metrics_gauge = Gauge('visits_sync_duration',
                                  'Micro-batch duration for the visits synchronization job', registry=registry)
            metrics_gauge.set_to_current_time()
            metrics_gauge.set(event.progress.batchDuration)
            push_to_gateway('localhost:9091', job='visits_to_delta_synchronizer', registry=registry)
        except Exception as error:
            print(f'An error occurred for sending the gauge: {error}')

    def onQueryIdle(self, event: "QueryIdleEvent") -> None:
        pass

    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        pass

    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        pass