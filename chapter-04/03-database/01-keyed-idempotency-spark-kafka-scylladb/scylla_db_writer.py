import datetime

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory, BatchStatement


class ScyllaDbWriter:

    def __init__(self):
        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
            retry_policy=DowngradingConsistencyRetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=15,
            row_factory=tuple_factory
        )
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        self.session = cluster.connect('dedp')
        self.rows_to_send = []

    def process(self, row):
        if len(self.rows_to_send) == 10:
            self._flush_buffer()
        self.rows_to_send.append(row)

    def close(self):
        self._flush_buffer()
        self.session.cluster.shutdown()

    def _flush_buffer(self):
        ingestion_time = datetime.datetime.utcnow()
        query = self.session.prepare(
            "INSERT INTO sessions (session_id, user_id, pages, ingestion_time) VALUES (?, ?, ?, ?)")
        batch_statement = BatchStatement()
        for row_to_insert in self.rows_to_send:
            print(row_to_insert)
            print(row_to_insert.session_id)
            batch_statement.add(query, (row_to_insert.session_id, row_to_insert.user_id,
                                                             row_to_insert.pages, ingestion_time))

        self.session.execute(batch_statement)
        self.rows_to_send = []
