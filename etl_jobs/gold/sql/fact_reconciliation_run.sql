WITH results AS (
    SELECT
        run_id,
        count(*) AS result_count,
        count(*) FILTER (WHERE category = 'MATCHED') AS matched_count,
        count(*) FILTER (WHERE category = 'MISMATCHED') AS mismatched_count,
        count(*) FILTER (WHERE category = 'UNRECONCILED_PROCESSOR') AS unreconciled_processor_count,
        count(*) FILTER (WHERE category = 'UNRECONCILED_INTERNAL') AS unreconciled_internal_count
    FROM silver_reconciliation_results
    GROUP BY run_id
),
reference_runs AS (
    SELECT run_id
    FROM silver_reconciliation_runs
    WHERE status = 'COMPLETED'
    QUALIFY row_number() OVER (PARTITION BY reference_date ORDER BY started_at DESC, run_id DESC) = 1
)
SELECT
    r.reference_date,
    r.run_id,
    r.file_name,
    r.status,
    k.run_id IS NOT NULL AS is_reference_run,
    r.status = 'IN_PROGRESS' AND r.started_at < now() - INTERVAL 24 HOUR AS is_stuck,
    r.started_at,
    r.completed_at,
    r.duration_s,
    r.total_transactions,
    coalesce(x.result_count, 0) AS result_count,
    coalesce(x.matched_count, 0) AS matched_count,
    coalesce(x.mismatched_count, 0) AS mismatched_count,
    coalesce(x.unreconciled_processor_count, 0) AS unreconciled_processor_count,
    coalesce(x.unreconciled_internal_count, 0) AS unreconciled_internal_count,
    coalesce(x.result_count - x.matched_count, 0) AS discrepancy_count,
    safe_rate(x.result_count - x.matched_count, x.result_count) AS discrepancy_rate,
    r.total_transactions - coalesce(x.result_count, 0) AS total_gap,
    r.cdc_event_count,
    r._source_file,
    r._bronze_ingested_at,
    r._silver_run_id
FROM silver_reconciliation_runs AS r
LEFT JOIN results AS x ON x.run_id = r.run_id
LEFT JOIN reference_runs AS k ON k.run_id = r.run_id
