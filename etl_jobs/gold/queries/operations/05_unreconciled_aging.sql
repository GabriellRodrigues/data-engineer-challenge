SELECT
    CASE
        WHEN days_unreconciled <= 7 THEN '0-7 days'
        WHEN days_unreconciled <= 14 THEN '8-14 days'
        WHEN days_unreconciled <= 30 THEN '15-30 days'
        ELSE 'over 30 days'
    END AS aging_bucket,
    currency,
    count(*) AS transactions,
    sum(exposure_amount) AS exposure_amount,
    max(days_unreconciled) AS oldest_days
FROM fact_transaction
WHERE reconciliation_status = 'UNRECONCILED_INTERNAL' AND NOT is_deleted
GROUP BY ALL
ORDER BY min(days_unreconciled), currency
