
INSERT INTO dedp.visits_monitoring(execution_time, all_rows, invalid_event_time, invalid_user_id, invalid_page, invalid_context)
    SELECT
        '{{ execution_date }}' AS execution_time,
        COUNT(*) AS all_rows,
        SUM(CASE WHEN event_time IS NULL THEN 1 ELSE 0 END) AS invalid_event_time,
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS invalid_user_id,
        SUM(CASE WHEN page IS NULL THEN 1 ELSE 0 END) AS invalid_page,
        SUM(CASE WHEN context IS NULL THEN 1 ELSE 0 END) AS invalid_context
    FROM dedp.visits_output
    WHERE
        id BETWEEN
            (SELECT first_row_id FROM dedp.visits_monitoring_state WHERE execution_time = '{{ execution_date }}')
            AND
            (SELECT last_row_id FROM dedp.visits_monitoring_state WHERE execution_time = '{{ execution_date }}');
;