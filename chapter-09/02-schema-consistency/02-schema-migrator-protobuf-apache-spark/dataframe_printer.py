from pyspark.sql import DataFrame


def printer_sink(visits: DataFrame, batch_number: int):
    visits.cache()
    connected_users_visits = visits.filter('visit.is_connected = true').filter('visit.from_page IS NOT NULL')
    print('-- connected visitors --')
    connected_users_visits.selectExpr('visit.*').show(truncate=False)

    print('-- all visits --')
    visits.selectExpr('visit.*').show(truncate=False)
    visits.unpersist()


def printer_sink_v2(visits: DataFrame, batch_number: int):
    visits.cache()
    connected_users_visits = visits.filter('visit.user_details.is_connected = true').filter(
        'visit.referral IS NOT NULL')
    print('-- connected visitors --')
    connected_users_visits.selectExpr('visit.*').show(truncate=False)

    print('-- all visits --')
    visits.selectExpr('visit.*').show(truncate=False)
    visits.unpersist()
