SELECT
    created_date,
    currency,
    status,
    count(*) AS transaction_count,
    money(sum(amount)) AS volume_amount,
    money(round(avg(amount), 2)) AS average_ticket
FROM fact_transaction
WHERE NOT is_deleted
GROUP BY created_date, currency, status
