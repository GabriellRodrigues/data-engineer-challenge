WITH recent AS (
    SELECT *
    FROM agg_reconciliation_merchant
    WHERE reference_date > (SELECT max(reference_date) FROM agg_reconciliation_merchant) - INTERVAL 30 DAY
)
SELECT
    merchant_id,
    any_value(legal_name) AS legal_name,
    any_value(primary_cnae) AS primary_cnae,
    sum(result_count) AS results,
    sum(discrepancy_count) AS discrepancies,
    round(100.0 * sum(discrepancy_count) / sum(result_count), 2) AS discrepancy_pct,
    sum(mismatched_count) AS mismatched,
    sum(unreconciled_processor_count) AS unreconciled_processor,
    sum(unreconciled_internal_count) AS unreconciled_internal
FROM recent
GROUP BY merchant_id
HAVING sum(result_count) >= 5
ORDER BY discrepancies DESC, discrepancy_pct DESC, merchant_id
LIMIT 10
