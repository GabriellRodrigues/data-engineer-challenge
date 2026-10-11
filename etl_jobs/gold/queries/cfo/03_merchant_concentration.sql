WITH by_merchant AS (
    SELECT merchant_id, currency, sum(volume_amount) AS tpv, sum(transaction_count) AS transactions
    FROM agg_merchant_volume_daily
    GROUP BY merchant_id, currency
),
ranked AS (
    SELECT
        b.*,
        row_number() OVER (PARTITION BY b.currency ORDER BY b.tpv DESC, b.merchant_id) AS rank,
        b.tpv / sum(b.tpv) OVER (PARTITION BY b.currency) AS share
    FROM by_merchant AS b
)
SELECT
    r.currency,
    r.rank,
    r.merchant_id,
    m.trade_name,
    m.primary_cnae,
    r.transactions,
    r.tpv,
    round(100 * r.share, 2) AS share_pct,
    round(100 * sum(r.share) OVER (PARTITION BY r.currency ORDER BY r.rank), 2) AS cumulative_share_pct
FROM ranked AS r
LEFT JOIN dim_merchant AS m ON m.merchant_id = r.merchant_id AND m.is_current
WHERE r.rank <= 10
ORDER BY r.currency, r.rank
