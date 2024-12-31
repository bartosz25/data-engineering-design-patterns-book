SELECT
    CASE
        WHEN COUNT(*) > 0 THEN true
        ELSE false
    END
FROM dedp.devices_history WHERE execution_time > '{{ ts }}'