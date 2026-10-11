SELECT
    settled_date,
    processor,
    currency,
    settled_count,
    settled_amount,
    reversed_count,
    reversed_amount,
    net_amount,
    round(100 * reversal_rate, 2) AS reversal_pct,
    round(100 * reversal_amount_rate, 2) AS reversal_amount_pct,
    without_internal_count
FROM agg_settlement_daily
ORDER BY settled_date, processor, currency
