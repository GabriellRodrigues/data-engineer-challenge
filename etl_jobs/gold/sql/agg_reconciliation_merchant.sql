SELECT
    f.reference_date,
    coalesce(f.merchant_id, 'UNKNOWN') AS merchant_id,
    arg_max(m.legal_name, m.valid_from) AS legal_name,
    arg_max(m.primary_cnae, m.valid_from) AS primary_cnae,
    count(*) AS result_count,
    count(*) FILTER (WHERE f.category = 'MATCHED') AS matched_count,
    count(*) FILTER (WHERE f.category = 'MISMATCHED') AS mismatched_count,
    count(*) FILTER (WHERE f.category = 'UNRECONCILED_PROCESSOR') AS unreconciled_processor_count,
    count(*) FILTER (WHERE f.category = 'UNRECONCILED_INTERNAL') AS unreconciled_internal_count,
    count(*) FILTER (WHERE f.is_discrepancy) AS discrepancy_count,
    safe_rate(count(*) FILTER (WHERE f.is_discrepancy), count(*)) AS discrepancy_rate
FROM fact_reconciliation_result AS f
LEFT JOIN dim_merchant AS m ON m.merchant_sk = f.merchant_sk
WHERE f.is_reference_run
GROUP BY f.reference_date, coalesce(f.merchant_id, 'UNKNOWN')
