SELECT
    d.year_month,
    v.currency,
    v.status,
    sum(v.transaction_count) AS transactions,
    sum(v.volume_amount) AS volume_amount,
    round(100.0 * sum(v.volume_amount) / sum(sum(v.volume_amount)) OVER (PARTITION BY d.year_month, v.currency), 2)
        AS share_pct
FROM agg_volume_daily AS v
JOIN dim_date AS d ON d.date_key = v.created_date
GROUP BY d.year_month, v.currency, v.status
ORDER BY d.year_month, v.currency, volume_amount DESC
