WITH latest_settlement AS (
    SELECT transaction_id, currency
    FROM silver_settlements
    QUALIFY row_number() OVER (
        PARTITION BY transaction_id ORDER BY reference_date DESC, settled_at DESC, processor_reference DESC
    ) = 1
),
base AS (
    SELECT
        r.reference_date,
        r.run_id,
        r.transaction_id,
        r.result_id,
        r.file_name,
        run.status AS run_status,
        run.is_reference_run,
        r.merchant_id,
        r.merchant_id_origin,
        r.category,
        coalesce(t.currency, s.currency) AS currency,
        r.internal_amount,
        r.processor_amount,
        r.difference,
        t.created_at AS transaction_created_at,
        r.created_at AS result_created_at,
        coalesce(t.created_at, r.created_at) AS merchant_as_of,
        r.source_row_count,
        r._source_file,
        r._bronze_ingested_at,
        r._silver_run_id
    FROM silver_reconciliation_results AS r
    JOIN fact_reconciliation_run AS run ON run.run_id = r.run_id
    LEFT JOIN silver_transactions AS t ON t.transaction_id = r.transaction_id
    LEFT JOIN latest_settlement AS s ON s.transaction_id = r.transaction_id
)
SELECT
    b.reference_date,
    b.run_id,
    b.transaction_id,
    b.result_id,
    b.file_name,
    b.run_status,
    b.is_reference_run,
    b.merchant_id,
    coalesce(m.merchant_sk, 'UNKNOWN') AS merchant_sk,
    b.merchant_id_origin,
    b.category,
    coalesce(c.is_discrepancy, b.category <> 'MATCHED') AS is_discrepancy,
    b.currency,
    b.internal_amount,
    b.processor_amount,
    b.difference,
    money(CASE b.category
        WHEN 'MISMATCHED' THEN b.difference
        WHEN 'UNRECONCILED_PROCESSOR' THEN b.processor_amount
        WHEN 'UNRECONCILED_INTERNAL' THEN b.internal_amount
        ELSE 0
    END) AS amount_at_risk,
    b.transaction_created_at,
    b.result_created_at,
    b.source_row_count,
    b._source_file,
    b._bronze_ingested_at,
    b._silver_run_id
FROM base AS b
LEFT JOIN dim_category AS c ON c.category = b.category
LEFT JOIN dim_merchant AS m ON m.merchant_id = b.merchant_id AND NOT m.is_unknown
QUALIFY row_number() OVER (
    PARTITION BY b.run_id, b.transaction_id
    ORDER BY valid_at(b.merchant_as_of, m.valid_from, m.valid_to) DESC NULLS LAST, m.valid_from
) = 1
