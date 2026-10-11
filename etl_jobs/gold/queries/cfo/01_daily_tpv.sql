SELECT
    created_date,
    currency,
    transaction_count,
    volume_amount AS tpv,
    average_ticket,
    round(100.0 * (volume_amount - lag(volume_amount) OVER w) / lag(volume_amount) OVER w, 2) AS change_vs_previous_day_pct
FROM agg_volume_daily
WHERE status = 'COMPLETED'
WINDOW w AS (PARTITION BY currency ORDER BY created_date)
ORDER BY currency, created_date
