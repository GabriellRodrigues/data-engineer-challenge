SELECT
    s.reference_date,
    s.processor,
    s.transaction_id,
    s.processor_reference,
    s.merchant_id,
    coalesce(m.merchant_sk, 'UNKNOWN') AS merchant_sk,
    s.amount,
    s.currency,
    s.status,
    s.is_reversed,
    money(CASE WHEN s.is_reversed THEN -s.amount ELSE s.amount END) AS signed_amount,
    s.settled_at,
    s.settled_date,
    t.transaction_id IS NOT NULL AS has_internal_transaction,
    t.amount AS internal_amount,
    s._source_file,
    s._bronze_ingested_at,
    s._silver_run_id
FROM silver_settlements AS s
LEFT JOIN silver_transactions AS t ON t.transaction_id = s.transaction_id
LEFT JOIN dim_merchant AS m ON m.merchant_id = s.merchant_id AND NOT m.is_unknown
QUALIFY row_number() OVER (
    PARTITION BY s.processor, s.reference_date, s.transaction_id, s.processor_reference
    ORDER BY valid_at(s.settled_at, m.valid_from, m.valid_to) DESC NULLS LAST, m.valid_from
) = 1
