SELECT COUNT(*) FROM dedp.visits_input WHERE id >
    COALESCE((SELECT last_row_id FROM dedp.visits_state WHERE execution_time = '{{ prev_execution_date }}'), 0)
;