-- Total Complaints made per borough in number and percentage
SELECT
      borough,
      COUNT(*) AS total_complaints,
      ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (), 2) AS pct_complaints
FROM clean_elevator_2024
GROUP BY borough
ORDER BY total_complaints DESC;