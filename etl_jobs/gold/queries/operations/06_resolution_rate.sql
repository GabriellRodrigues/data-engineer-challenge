WITH judged AS (
    SELECT
        transaction_id,
        reconciliation_status,
        is_resolved_discrepancy,
        runs_count
    FROM fact_transaction
    WHERE runs_count > 1
)
SELECT
    count(*) AS transactions_in_several_runs,
    count(*) FILTER (WHERE is_resolved_discrepancy) AS resolved_after_discrepancy,
    count(*) FILTER (WHERE reconciliation_status <> 'MATCHED') AS still_open,
    round(100.0 * count(*) FILTER (WHERE is_resolved_discrepancy)
        / nullif(count(*) FILTER (WHERE is_resolved_discrepancy OR reconciliation_status <> 'MATCHED'), 0), 2)
        AS resolution_pct
FROM judged
