SELECT
    reference_date,
    currency,
    discrepancy_count,
    mismatched_amount,
    unreconciled_processor_amount,
    unreconciled_internal_amount,
    amount_at_risk
FROM agg_amount_at_risk_daily
WHERE reference_date = (SELECT max(reference_date) FROM agg_amount_at_risk_daily)
ORDER BY amount_at_risk DESC
