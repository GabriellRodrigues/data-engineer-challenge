SELECT
    reference_date,
    coalesce(currency, 'UNKNOWN') AS currency,
    count(*) AS result_count,
    count(*) FILTER (WHERE is_discrepancy) AS discrepancy_count,
    money(coalesce(sum(amount_at_risk) FILTER (WHERE category = 'MISMATCHED'), 0)) AS mismatched_amount,
    money(coalesce(sum(amount_at_risk) FILTER (WHERE category = 'UNRECONCILED_PROCESSOR'), 0))
        AS unreconciled_processor_amount,
    money(coalesce(sum(amount_at_risk) FILTER (WHERE category = 'UNRECONCILED_INTERNAL'), 0))
        AS unreconciled_internal_amount,
    money(coalesce(sum(amount_at_risk), 0)) AS amount_at_risk
FROM fact_reconciliation_result
WHERE is_reference_run
GROUP BY reference_date, coalesce(currency, 'UNKNOWN')
