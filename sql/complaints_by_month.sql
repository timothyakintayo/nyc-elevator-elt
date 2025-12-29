-- Monthly elevator complaints
SELECT 
      strftime(created_date, '%B') AS month,
      COUNT(*) AS total_complaints,
      ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (), 2) AS pct_complaints
FROM clean_elevator_2024
GROUP BY strftime(created_date, '%B')
ORDER BY total_complaints DESC;