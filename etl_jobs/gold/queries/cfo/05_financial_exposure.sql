WITH latest_verdict AS (
    SELECT transaction_id, category, currency, amount_at_risk
    FROM fact_reconciliation_result
    QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY reference_date DESC, run_id DESC) = 1
),
internal_side AS (
    SELECT currency, reconciliation_status AS category, count(*) AS transactions, sum(exposure_amount) AS amount
    FROM fact_transaction
    WHERE exposure_amount > 0
    GROUP BY ALL
),
processor_side AS (
    SELECT coalesce(currency, 'UNKNOWN') AS currency, category, count(*) AS transactions, sum(amount_at_risk) AS amount
    FROM latest_verdict
    WHERE category = 'UNRECONCILED_PROCESSOR'
    GROUP BY ALL
)
SELECT currency, category, transactions, amount
FROM (SELECT * FROM internal_side UNION ALL SELECT * FROM processor_side)
ORDER BY currency, amount DESC
