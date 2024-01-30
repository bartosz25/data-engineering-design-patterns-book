from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


class PostgresViewManagerOperator(PostgresOperator):
    def __init__(self, view_name, postgres_conn_id, database, schema, *args, **kwargs):
        super(PostgresViewManagerOperator, self).__init__(*args, **kwargs)
        self.view_name = view_name
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.database = database

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        view_tables = hook.get_records(self.sql)
        if view_tables:
            view_table_names = map(lambda tuple: 'SELECT * FROM {}.{}'.format(self.schema, tuple[0]), view_tables)
            view_tables_union = ' UNION ALL '.join(view_table_names)
            view_query = 'CREATE OR REPLACE VIEW {schema}.{view_name} AS ({tables})'.format(
                schema=self.schema, view_name=self.view_name, tables=view_tables_union)
            hook.run(view_query)