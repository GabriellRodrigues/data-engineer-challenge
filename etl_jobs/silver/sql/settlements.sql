WITH parsed AS (
    SELECT
        'paysettler' AS processor,
        transaction_id,
        norm_merchant(merchant_id) AS merchant_id,
        parse_amount(amount) AS amount,
        amount AS amount_raw,
        norm_code(currency) AS currency,
        parse_ts_utc(settled_at) AS settled_at,
        processor_reference,
        norm_code(status) AS status,
        file_date(_source_file) AS file_reference_date,
        _source_file,
        _run_id AS _bronze_run_id,
        _ingested_at AS _bronze_ingested_at
    FROM bronze_settlement_paysettler
),
dated AS (
    SELECT
        *,
        coalesce(
            file_reference_date,
            CAST(max(settled_at) OVER (PARTITION BY _source_file) AS DATE)
        ) AS reference_date
    FROM parsed
)
SELECT
    processor,
    reference_date,
    transaction_id,
    processor_reference,
    merchant_id,
    amount,
    amount_raw,
    currency,
    status,
    settled_at,
    CAST(settled_at AS DATE) AS settled_date,
    status = 'REVERSED' AS is_reversed,
    _source_file,
    _bronze_run_id,
    _bronze_ingested_at
FROM dated
QUALIFY row_number() OVER (
    PARTITION BY processor, reference_date, transaction_id, processor_reference
    ORDER BY _source_file DESC, settled_at DESC
) = 1
