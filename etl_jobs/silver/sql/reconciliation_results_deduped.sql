WITH events AS (
    SELECT
        id AS result_id,
        run_id,
        transaction_id,
        norm_merchant(merchant_id) AS merchant_id,
        norm_code(category) AS category,
        to_money(internal_amount) AS internal_amount,
        to_money(processor_amount) AS processor_amount,
        to_money(difference) AS difference,
        parse_ts_utc(created_at) AS created_at,
        Op AS op,
        parse_ts_utc(_timestamp) AS cdc_timestamp,
        _source_file,
        _run_id AS _bronze_run_id,
        _ingested_at AS _bronze_ingested_at
    FROM bronze_reconciliation_results
),
deduped AS (
    SELECT
        *,
        count(*) OVER (PARTITION BY run_id, transaction_id) AS source_row_count
    FROM events
    QUALIFY row_number() OVER (
        PARTITION BY run_id, transaction_id
        ORDER BY cdc_timestamp DESC, op_rank(op) DESC, _source_file DESC, result_id DESC
    ) = 1
),
settlement_merchants AS (
    SELECT transaction_id, min(merchant_id) AS merchant_id
    FROM settlements
    GROUP BY transaction_id
)
SELECT
    d.result_id,
    d.run_id,
    d.transaction_id,
    coalesce(d.merchant_id, t.merchant_id, s.merchant_id) AS merchant_id,
    CASE
        WHEN d.merchant_id IS NOT NULL THEN 'result'
        WHEN t.merchant_id IS NOT NULL THEN 'transaction'
        WHEN s.merchant_id IS NOT NULL THEN 'settlement'
    END AS merchant_id_origin,
    d.category,
    d.internal_amount,
    d.processor_amount,
    d.difference,
    d.created_at,
    d.source_row_count,
    d._source_file,
    d._bronze_run_id,
    d._bronze_ingested_at
FROM deduped AS d
LEFT JOIN transactions AS t USING (transaction_id)
LEFT JOIN settlement_merchants AS s USING (transaction_id)
