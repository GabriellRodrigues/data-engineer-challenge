WITH verdicts AS (
    SELECT
        r.transaction_id,
        r.run_id,
        r.reference_date,
        r.category,
        r.difference,
        r.created_at,
        row_number() OVER (
            PARTITION BY r.transaction_id ORDER BY r.reference_date DESC, run.started_at DESC, r.run_id DESC
        ) AS recency
    FROM silver_reconciliation_results AS r
    JOIN silver_reconciliation_runs AS run ON run.run_id = r.run_id
),
summary AS (
    SELECT
        transaction_id,
        count(*) AS runs_count,
        min(reference_date) AS first_reference_date,
        max(reference_date) AS last_reference_date,
        min(created_at) AS first_reconciled_at,
        any_value(run_id) FILTER (WHERE recency = 1) AS latest_run_id,
        any_value(category) FILTER (WHERE recency = 1) AS latest_category,
        any_value(difference) FILTER (WHERE recency = 1) AS latest_difference,
        coalesce(bool_or(category <> 'MATCHED') FILTER (WHERE recency > 1), false) AS had_earlier_discrepancy
    FROM verdicts
    GROUP BY transaction_id
),
changes AS (
    SELECT
        transaction_id,
        count(*) AS changes_after_reconciliation,
        max(cdc_timestamp) AS last_change_after_reconciliation_at
    FROM fact_transaction_event
    WHERE is_after_reconciliation AND (amount_changed OR status_changed OR op = 'D')
    GROUP BY transaction_id
)
SELECT
    t.created_date,
    t.transaction_id,
    t.merchant_id,
    coalesce(m.merchant_sk, 'UNKNOWN') AS merchant_sk,
    t.amount,
    t.currency,
    t.status,
    t.payment_method,
    t.is_deleted,
    t.created_at,
    t.updated_at,
    coalesce(s.runs_count, 0) AS runs_count,
    s.first_reference_date,
    s.last_reference_date,
    s.latest_run_id,
    coalesce(s.latest_category, 'NOT_RECONCILED') AS reconciliation_status,
    coalesce(s.latest_category = 'MATCHED' AND s.had_earlier_discrepancy, false) AS is_resolved_discrepancy,
    money(CASE
        WHEN t.is_deleted THEN 0
        WHEN s.latest_category = 'UNRECONCILED_INTERNAL' THEN t.amount
        WHEN s.latest_category = 'MISMATCHED' THEN s.latest_difference
        ELSE 0
    END) AS exposure_amount,
    CASE
        WHEN s.latest_category = 'UNRECONCILED_INTERNAL' THEN s.last_reference_date - t.created_date
    END::BIGINT AS days_unreconciled,
    s.first_reconciled_at,
    coalesce(c.changes_after_reconciliation, 0) AS changes_after_reconciliation,
    c.last_change_after_reconciliation_at,
    t.cdc_event_count,
    t.last_cdc_timestamp,
    t._source_file,
    t._bronze_ingested_at,
    t._silver_run_id
FROM silver_transactions AS t
LEFT JOIN summary AS s ON s.transaction_id = t.transaction_id
LEFT JOIN changes AS c ON c.transaction_id = t.transaction_id
LEFT JOIN dim_merchant AS m ON m.merchant_id = t.merchant_id AND NOT m.is_unknown
QUALIFY row_number() OVER (
    PARTITION BY t.transaction_id
    ORDER BY valid_at(t.created_at, m.valid_from, m.valid_to) DESC NULLS LAST, m.valid_from
) = 1
