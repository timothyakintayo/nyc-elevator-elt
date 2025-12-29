-- Average time taken to update the complaint created
SELECT
ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600),2) AS hours_to_update_status
FROM clean_elevator_2024