-- Quarterly elevator complaints
SELECT
      'Q' || EXTRACT(QUARTER FROM created_date) AS quarter_name,
      COUNT(*) AS total_complaints,
      ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (), 2) AS pct_complaints
FROM clean_elevator_2024
GROUP BY 'Q' || EXTRACT(QUARTER FROM created_date)
ORDER BY total_complaints DESC;