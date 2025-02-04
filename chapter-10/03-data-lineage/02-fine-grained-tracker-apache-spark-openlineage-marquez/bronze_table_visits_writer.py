from spark_session_factory import create_spark_session_with_delta_lake_and_open_lineage

if __name__ == "__main__":
    spark_session = create_spark_session_with_delta_lake_and_open_lineage('bronze_visits_table')

    spark_session.sql('DROP TABLE IF EXISTS default.visits')
    spark_session.sql('''
    CREATE TABLE default.visits (
        visit_id STRING NOT NULL, 
        event_time TIMESTAMP NOT NULL, 
        user_id STRING NOT NULL, 
        page STRING NOT NULL
    ) USING delta  
    ''')


    spark_session.sql('''
    INSERT INTO default.visits (visit_id, event_time, user_id, page) VALUES 
    ("visit 100", TIMESTAMP "2024-07-02T10:00:00.840Z", "user_a", "contact.html"),
    ("visit 100", TIMESTAMP "2024-07-02T10:02:00.840Z", "user_a", "home.html"),
    ("visit 200", TIMESTAMP "2024-07-02T11:00:00.840Z", "user_b", "about.html")
    ''')