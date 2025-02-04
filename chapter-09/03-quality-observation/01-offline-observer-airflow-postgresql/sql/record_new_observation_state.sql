DELETE FROM dedp.visits_monitoring_state WHERE execution_time = '{{ execution_date }}';

INSERT INTO dedp.visits_monitoring_state (execution_time, first_row_id, last_row_id)
    SELECT
        '{{ execution_date }}' AS execution_time,
        MIN(id) AS first_row_id,
        MAX(id) AS last_row_id
    FROM dedp.visits_output
    WHERE id > COALESCE((SELECT last_row_id FROM dedp.visits_monitoring_state WHERE execution_time = '{{ prev_execution_date }}'::TIMESTAMP), 0)
;