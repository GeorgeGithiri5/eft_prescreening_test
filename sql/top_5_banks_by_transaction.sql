-- Top 5 banks by transaction volume in the last 7 days
SELECT
    bank_id,
    SUM(total_volume) AS total_transaction_volume
FROM daily_transaction_summary
WHERE 
    transaction_date >= CURDATE() - INTERVAL 7 DAY
    GROUP BY bank_id
ORDER BY total_transaction_volume DESC
LIMIT 5;
