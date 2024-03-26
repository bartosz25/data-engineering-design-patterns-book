INSERT INTO dedp.visits_cube (current_execution_time_id, page, user_id, visits_number, is_approximate)
SELECT
    '{{ ds }}',
    page,
    user_id,
    COUNT(*),
    (SELECT CASE WHEN hours_subquery.all_hours = 24 THEN false ELSE true END FROM
        (SELECT COUNT(DISTINCT execution_time_hour_id) AS all_hours FROM dedp.visits_raw WHERE execution_time_id = '{{ ds }}')
        AS hours_subquery)
FROM dedp.visits_raw GROUP BY CUBE(user_id, page);