WITH first_reconciliation AS (
    SELECT transaction_id, min(created_at) AS first_reconciled_at
    FROM silver_reconciliation_results
    GROUP BY transaction_id
)
SELECT
    h.created_date,
    h.transaction_id,
    h.cdc_timestamp,
    h.op,
    row_number() OVER w AS event_seq,
    h.source_id,
    h.merchant_id,
    h.amount,
    h.currency,
    h.status,
    h.payment_method,
    lag(h.amount) OVER w AS previous_amount,
    lag(h.status) OVER w AS previous_status,
    coalesce(h.amount <> lag(h.amount) OVER w, false) AS amount_changed,
    coalesce(h.status <> lag(h.status) OVER w, false) AS status_changed,
    h.created_at,
    h.updated_at,
    f.first_reconciled_at,
    coalesce(h.cdc_timestamp > f.first_reconciled_at, false) AS is_after_reconciliation,
    h._source_file,
    h._bronze_ingested_at,
    h._silver_run_id
FROM silver_transactions_history AS h
LEFT JOIN first_reconciliation AS f ON f.transaction_id = h.transaction_id
WINDOW w AS (PARTITION BY h.transaction_id ORDER BY h.cdc_timestamp, op_rank(h.op))
