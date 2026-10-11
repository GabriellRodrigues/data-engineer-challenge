SELECT
    r.reference_date,
    d.run_id,
    d.transaction_id,
    d.result_id,
    r.file_name,
    d.merchant_id,
    d.merchant_id_origin,
    d.category,
    d.internal_amount,
    d.processor_amount,
    d.difference,
    d.created_at,
    d.source_row_count,
    d._source_file,
    d._bronze_run_id,
    d._bronze_ingested_at
FROM reconciliation_results_deduped AS d
JOIN reconciliation_runs AS r USING (run_id)
