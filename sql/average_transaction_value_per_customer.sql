-- Average transaction value per customer for a given month
SELECT 
    customer_id,
    AVG(transaction_amount) AS avg_transaction_value
FROM raw_transactions
WHERE DATE_FORMAT(transaction_date, '%Y-%m') = '2025-09'
GROUP BY customer_id
ORDER BY avg_transaction_value DESC;
