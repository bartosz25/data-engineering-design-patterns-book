from spark_session_factory import create_spark_session_with_delta_lake_and_open_lineage

if __name__ == "__main__":
    spark_session = create_spark_session_with_delta_lake_and_open_lineage('silver_enriched_visits_table')

    spark_session.sql('DROP TABLE IF EXISTS default.enriched_visits')
    spark_session.sql('''
    CREATE TABLE default.enriched_visits (
        visit_id STRING NOT NULL, 
        event_time TIMESTAMP NOT NULL, 
        user_id STRING NOT NULL, 
        page STRING NOT NULL,
        user_login STRING NOT NULL
    ) USING delta  
    ''')

    spark_session.sql('''
    INSERT INTO default.enriched_visits (
        SELECT v.*, u.login AS user_login FROM default.visits v 
        JOIN default.users u ON u.user_id = v.user_id 
    )
    ''')

    spark_session.sql('SELECT * FROM default.enriched_visits').show(truncate=False)
