SELECT COUNT(*) FROM dedp.visits_output WHERE id >
    COALESCE((SELECT last_row_id FROM dedp.visits_monitoring_state WHERE execution_time = '{{ prev_execution_date }}'), 0)
;