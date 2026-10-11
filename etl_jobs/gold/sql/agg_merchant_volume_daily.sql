WITH volume AS (
    SELECT
        created_date,
        merchant_id,
        currency,
        count(*) AS transaction_count,
        money(sum(amount)) AS volume_amount
    FROM fact_transaction
    WHERE NOT is_deleted AND status = 'COMPLETED'
    GROUP BY created_date, merchant_id, currency
)
SELECT
    created_date,
    merchant_id,
    currency,
    transaction_count,
    volume_amount,
    safe_rate(volume_amount, sum(volume_amount) OVER (PARTITION BY created_date, currency)) AS share_of_daily_volume
FROM volume
