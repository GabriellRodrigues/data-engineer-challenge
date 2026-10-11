SELECT
    d.run_id,
    d.transaction_id,
    d.result_id,
    d.merchant_id,
    d.merchant_id_origin,
    d.category,
    d.internal_amount,
    d.processor_amount,
    d.difference,
    d.created_at,
    d.source_row_count,
    'run_not_found' AS _quarantine_reason,
    d._source_file,
    d._bronze_run_id,
    d._bronze_ingested_at
FROM reconciliation_results_deduped AS d
ANTI JOIN reconciliation_runs AS r USING (run_id)
