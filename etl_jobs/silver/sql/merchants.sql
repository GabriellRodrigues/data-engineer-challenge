WITH events AS (
    SELECT
        norm_merchant(merchant_id) AS merchant_id,
        id AS source_id,
        trim(legal_name) AS legal_name,
        trim(trade_name) AS trade_name,
        regexp_replace(document, '[^0-9]', '', 'g') AS document,
        trim(primary_cnae) AS primary_cnae,
        parse_ts_utc(created_at) AS created_at,
        parse_ts_utc(updated_at) AS updated_at,
        Op AS op,
        parse_ts_utc(_timestamp) AS valid_from,
        _source_file,
        _run_id AS _bronze_run_id,
        _ingested_at AS _bronze_ingested_at
    FROM bronze_enterprise_company
    QUALIFY row_number() OVER (
        PARTITION BY norm_merchant(merchant_id), parse_ts_utc(_timestamp)
        ORDER BY op_rank(Op) DESC, _source_file DESC, id DESC
    ) = 1
),
changes AS (
    SELECT
        *,
        hash(legal_name, trade_name, document, primary_cnae, op = 'D') AS version_hash,
        lag(hash(legal_name, trade_name, document, primary_cnae, op = 'D')) OVER (
            PARTITION BY merchant_id ORDER BY valid_from
        ) AS previous_hash
    FROM events
),
versions AS (
    SELECT * FROM changes
    WHERE previous_hash IS NULL OR previous_hash <> version_hash
)
SELECT
    merchant_id,
    valid_from,
    lead(valid_from) OVER (PARTITION BY merchant_id ORDER BY valid_from) AS valid_to,
    lead(valid_from) OVER (PARTITION BY merchant_id ORDER BY valid_from) IS NULL
        AND op <> 'D' AS is_current,
    op = 'D' AS is_deleted,
    source_id,
    legal_name,
    trade_name,
    document,
    primary_cnae,
    created_at,
    updated_at,
    _source_file,
    _bronze_run_id,
    _bronze_ingested_at
FROM versions
