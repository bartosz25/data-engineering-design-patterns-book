from spark_session_factory import create_spark_session_with_delta_lake_and_open_lineage

if __name__ == "__main__":
    spark_session = create_spark_session_with_delta_lake_and_open_lineage('bronze_users_table')

    spark_session.sql('DROP TABLE IF EXISTS default.users')
    spark_session.sql('''
    CREATE TABLE default.users (
        user_id STRING NOT NULL, 
        login STRING NOT NULL
    ) USING delta  
    ''')

    spark_session.sql('''
    INSERT INTO default.users(user_id, login) VALUES 
    ("user_a", "User a login"),
    ("user_b", "User b login")
    ''')