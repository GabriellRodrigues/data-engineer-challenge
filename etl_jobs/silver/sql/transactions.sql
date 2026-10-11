SELECT
    transaction_id,
    source_id,
    merchant_id,
    amount,
    currency,
    status,
    description,
    payment_method,
    created_at,
    updated_at,
    created_date,
    op = 'D' AS is_deleted,
    cdc_timestamp AS last_cdc_timestamp,
    count(*) OVER (PARTITION BY transaction_id) AS cdc_event_count,
    _source_file,
    _bronze_run_id,
    _bronze_ingested_at
FROM transactions_history
QUALIFY row_number() OVER (
    PARTITION BY transaction_id
    ORDER BY cdc_timestamp DESC, op_rank(op) DESC, _source_file DESC, source_id DESC
) = 1
