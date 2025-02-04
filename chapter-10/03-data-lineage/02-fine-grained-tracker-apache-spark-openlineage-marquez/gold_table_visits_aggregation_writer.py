from spark_session_factory import create_spark_session_with_delta_lake_and_open_lineage

if __name__ == "__main__":
    spark_session = create_spark_session_with_delta_lake_and_open_lineage('gold_visits_aggregates')

    spark_session.sql('DROP TABLE IF EXISTS default.visits_aggregates')
    spark_session.sql('''
    CREATE TABLE default.visits_aggregates (
        visit_id STRING NOT NULL, 
        user_id STRING NOT NULL, 
        visits_count INT
    ) USING delta  
    ''')

    spark_session.sql('''
    INSERT INTO default.visits_aggregates (
        SELECT visit_id, user_id, COUNT(visit_id) AS visits_count FROM default.enriched_visits GROUP BY visit_id, user_id
    )
    ''')

    spark_session.sql('SELECT * FROM default.visits_aggregates').show(truncate=False)
