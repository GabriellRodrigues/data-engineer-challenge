SELECT
    e.transaction_id,
    e.first_reconciled_at,
    e.cdc_timestamp AS changed_at,
    e.op,
    e.previous_status,
    e.status,
    e.previous_amount,
    e.amount,
    t.reconciliation_status AS current_reconciliation_status,
    e._source_file AS evidence
FROM fact_transaction_event AS e
JOIN fact_transaction AS t USING (transaction_id)
WHERE e.is_after_reconciliation AND (e.amount_changed OR e.status_changed OR e.op = 'D')
ORDER BY e.cdc_timestamp, e.transaction_id
