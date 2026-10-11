SELECT
    d.reference_date,
    d.is_expected_run_date,
    d.runs_count,
    d.failed_runs,
    d.stuck_runs,
    d.is_missing_run,
    r.run_id,
    r.status,
    r.file_name,
    round(r.duration_s / 60, 1) AS duration_min,
    r.total_transactions,
    r.result_count,
    r.total_gap
FROM agg_reconciliation_daily AS d
LEFT JOIN fact_reconciliation_run AS r ON r.reference_date = d.reference_date
WHERE d.is_expected_run_date OR d.runs_count > 0
ORDER BY d.reference_date, r.run_id
