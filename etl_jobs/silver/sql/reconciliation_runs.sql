WITH events AS (
    SELECT
        id AS run_id,
        TRY_CAST(reference_date AS DATE) AS reference_date,
        file_name,
        norm_code(status) AS status,
        CAST(total_transactions AS BIGINT) AS total_transactions,
        parse_ts_utc(started_at) AS started_at,
        parse_ts_utc(completed_at) AS completed_at,
        parse_ts_utc(created_at) AS created_at,
        Op AS op,
        parse_ts_utc(_timestamp) AS cdc_timestamp,
        _source_file,
        _run_id AS _bronze_run_id,
        _ingested_at AS _bronze_ingested_at
    FROM bronze_reconciliation_runs
)
SELECT
    run_id,
    reference_date,
    file_name,
    status,
    total_transactions,
    started_at,
    completed_at,
    created_at,
    CAST(epoch(completed_at) - epoch(started_at) AS DOUBLE) AS duration_s,
    cdc_timestamp AS last_cdc_timestamp,
    count(*) OVER (PARTITION BY run_id) AS cdc_event_count,
    _source_file,
    _bronze_run_id,
    _bronze_ingested_at
FROM events
QUALIFY row_number() OVER (
    PARTITION BY run_id
    ORDER BY cdc_timestamp DESC, op_rank(op) DESC, _source_file DESC
) = 1
