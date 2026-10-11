SELECT
    reference_date,
    run_id,
    outcome,
    service_category,
    recomputed_category,
    count(*) AS transactions
FROM audit_reconciliation_recheck
GROUP BY ALL
ORDER BY reference_date, run_id, transactions DESC
