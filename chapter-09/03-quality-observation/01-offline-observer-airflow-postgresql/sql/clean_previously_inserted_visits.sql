DELETE FROM dedp.visits_output
WHERE id BETWEEN
    (SELECT first_row_id FROM dedp.visits_state WHERE execution_time = '{{ execution_date }}')
    AND
    (SELECT last_row_id FROM dedp.visits_state WHERE execution_time = '{{ execution_date }}');