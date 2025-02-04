from pyspark import pandas
from pyspark.sql import SparkSession, functions

from config import get_base_input_dir

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]').getOrCreate())

    users = (spark.read.schema('user_id INT, country STRING, ssn STRING, salary INT').json(get_base_input_dir()))


    def pseudo_anonymize_users(input_pandas: pandas.DataFrame) -> pandas.DataFrame:
        def pseudo_anonymize_country(country: str) -> str:
            countries_area_mapping = {
                'Poland': 'eu', 'France': 'eu', 'Spain': 'eu', 'the USA': 'na'
            }
            return countries_area_mapping[country]

        def pseudo_anonymize_ssn(ssn: str) -> str:
            return f'{ssn[0]}***-{ssn[5]}***-{ssn[10]}***'

        for rows in input_pandas:
            rows['country'] = rows['country'].apply(lambda country: pseudo_anonymize_country(country))
            rows['ssn'] = rows['ssn'].apply(lambda ssn: pseudo_anonymize_ssn(ssn))
            yield rows


    pseud_anonymized_users = (users.mapInPandas(pseudo_anonymize_users, users.schema)
                              .withColumn('salary', functions.expr('''
                              CASE 
                                WHEN salary BETWEEN 0 AND 50000 THEN "0-50000"
                                WHEN salary BETWEEN 50000 AND 60000 THEN "50000-60000"
                                ELSE "60000+"
                              END
                              ''')))

    users.show()
    pseud_anonymized_users.show()
