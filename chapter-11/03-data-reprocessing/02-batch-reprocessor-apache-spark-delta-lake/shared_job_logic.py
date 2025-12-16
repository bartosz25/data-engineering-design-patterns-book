from pyspark.sql import DataFrame


def rename_columns(input_dataframe: DataFrame) -> DataFrame:
    return input_dataframe.selectExpr("""
        FROM_JSON(CAST(value AS STRING), 'event_id STRING, visit_id LONG, event_time TIMESTAMP, page STRING ') AS visit_with_page
    """).selectExpr(
        'visit_with_page.event_id AS event_identifier', 'visit_with_page.visit_id AS visit_identifier',
        'visit_with_page.event_time', 'visit_with_page.page AS visited_page'
    ).selectExpr('TO_JSON(STRUCT(*)) AS value')
