-- Average number of days to resolve elevator complaints in 2024
SELECT
    ROUND(AVG(closed_in_days), 2) AS avg_days_to_close
FROM clean_elevator_2024
WHERE closed_in_days IS NOT NULL;