SELECT
    settled_date,
    processor,
    currency,
    count(*) FILTER (WHERE NOT is_reversed) AS settled_count,
    money(coalesce(sum(amount) FILTER (WHERE NOT is_reversed), 0)) AS settled_amount,
    count(*) FILTER (WHERE is_reversed) AS reversed_count,
    money(coalesce(sum(amount) FILTER (WHERE is_reversed), 0)) AS reversed_amount,
    money(sum(signed_amount)) AS net_amount,
    safe_rate(count(*) FILTER (WHERE is_reversed), count(*)) AS reversal_rate,
    safe_rate(sum(amount) FILTER (WHERE is_reversed), sum(amount)) AS reversal_amount_rate,
    count(*) FILTER (WHERE NOT has_internal_transaction) AS without_internal_count
FROM fact_settlement
GROUP BY settled_date, processor, currency
