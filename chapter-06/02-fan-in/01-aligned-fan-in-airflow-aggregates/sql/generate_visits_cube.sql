INSERT INTO dedp.visits_cube (current_execution_time_id, page, user_id, visits_number)
SELECT '{{ ds }}', page, user_id, COUNT(*) FROM dedp.visits_raw GROUP BY CUBE(user_id, page);