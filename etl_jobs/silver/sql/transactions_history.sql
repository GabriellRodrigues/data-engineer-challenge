WITH events AS (
    SELECT
        transaction_id,
        id AS source_id,
        norm_merchant(merchant_id) AS merchant_id,
        to_money(amount) AS amount,
        norm_code(currency) AS currency,
        norm_code(status) AS status,
        description,
        nullif(lower(trim(payment_method)), '') AS payment_method,
        parse_ts_utc(created_at) AS created_at,
        parse_ts_utc(updated_at) AS updated_at,
        CAST(parse_ts_utc(created_at) AS DATE) AS created_date,
        Op AS op,
        parse_ts_utc(_timestamp) AS cdc_timestamp,
        _source_file,
        _run_id AS _bronze_run_id,
        _ingested_at AS _bronze_ingested_at
    FROM bronze_transactions
)
SELECT *
FROM events
QUALIFY row_number() OVER (
    PARTITION BY transaction_id, cdc_timestamp, op
    ORDER BY _source_file DESC, source_id DESC
) = 1
