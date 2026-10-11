WITH target AS (
    SELECT transaction_id
    FROM fact_transaction
    ORDER BY changes_after_reconciliation DESC, runs_count DESC, transaction_id
    LIMIT 1
),
trail AS (
    SELECT
        e.transaction_id,
        e.cdc_timestamp AS happened_at,
        'cdc ' || e.op AS event,
        e.status || ' ' || e.amount::VARCHAR || ' ' || e.currency AS detail,
        e._source_file AS evidence
    FROM fact_transaction_event AS e
    JOIN target USING (transaction_id)
    UNION ALL
    SELECT
        r.transaction_id,
        r.result_created_at,
        'verdict run ' || r.run_id::VARCHAR,
        r.category || coalesce(' internal ' || r.internal_amount::VARCHAR, '')
            || coalesce(' processor ' || r.processor_amount::VARCHAR, ''),
        r.file_name
    FROM fact_reconciliation_result AS r
    JOIN target USING (transaction_id)
)
SELECT * FROM trail ORDER BY happened_at, event
