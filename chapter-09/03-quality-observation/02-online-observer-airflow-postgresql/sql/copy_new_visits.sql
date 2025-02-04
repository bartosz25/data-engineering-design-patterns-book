INSERT INTO dedp.visits_output
SELECT * FROM dedp.visits_input
WHERE id BETWEEN
    (SELECT first_row_id FROM dedp.visits_state WHERE execution_time = '{{ execution_date }}')
    AND
    (SELECT last_row_id FROM dedp.visits_state WHERE execution_time = '{{ execution_date }}');