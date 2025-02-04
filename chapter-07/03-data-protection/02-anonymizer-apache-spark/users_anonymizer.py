import pandas
from faker import Faker
from pyspark.sql import SparkSession, functions
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StringType

from config import get_base_input_dir

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]').getOrCreate())

    users = (spark.read.schema('user_id INT, birthday STRING, email STRING').json(get_base_input_dir()))


    @pandas_udf(StringType())
    def replace_email(emails: pandas.Series) -> pandas.Series:
        faker_generator = Faker()
        return emails.apply(lambda email: faker_generator.email())


    anonymized_users = (users.drop('birthday')
                        .withColumn('email', replace_email(users.email)))

    users.show(truncate=False)
    anonymized_users.show(truncate=False)
