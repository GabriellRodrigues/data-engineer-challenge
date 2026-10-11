SELECT
    r.run_id,
    r.reference_date,
    r.status,
    r.file_name AS processed_file,
    r.started_at,
    r.completed_at,
    r.result_count,
    r._source_file AS run_extract,
    r._bronze_ingested_at AS run_loaded_at,
    list(DISTINCT f._source_file) AS result_extracts,
    max(f._bronze_ingested_at) AS results_loaded_at,
    any_value(r._gold_run_id) AS gold_run_id
FROM fact_reconciliation_run AS r
LEFT JOIN fact_reconciliation_result AS f ON f.run_id = r.run_id
GROUP BY ALL
ORDER BY r.reference_date, r.run_id
