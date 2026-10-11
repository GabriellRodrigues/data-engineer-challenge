SELECT
    reference_date,
    reference_run_id,
    result_count,
    discrepancy_count,
    round(discrepancy_rate * 100, 2) AS discrepancy_pct,
    round(discrepancy_rate_baseline * 100, 2) AS baseline_pct,
    round(discrepancy_rate_deviation * 100, 2) AS deviation_pp,
    CASE
        WHEN NOT is_comparable_volume THEN 'low volume, not compared'
        WHEN discrepancy_rate_deviation > 0.05 THEN 'spike'
        WHEN discrepancy_rate_deviation > 0 THEN 'above baseline'
        WHEN discrepancy_rate_deviation <= 0 THEN 'at or below baseline'
        ELSE 'no baseline yet'
    END AS trend
FROM agg_reconciliation_daily
WHERE discrepancy_rate IS NOT NULL
ORDER BY reference_date
