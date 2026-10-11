WITH reference_runs AS (
    SELECT run_id, reference_date
    FROM fact_reconciliation_run
    WHERE is_reference_run
      AND reference_date IN (SELECT reference_date FROM silver_settlements)
),
processor_side AS (
    SELECT r.run_id, r.reference_date, s.transaction_id, s.amount AS processor_amount
    FROM reference_runs AS r
    JOIN silver_settlements AS s ON s.reference_date = r.reference_date
    QUALIFY row_number() OVER (
        PARTITION BY r.run_id, s.transaction_id ORDER BY s.settled_at DESC, s.processor_reference DESC
    ) = 1
),
service AS (
    SELECT r.run_id, r.reference_date, r.transaction_id, r.category, r.internal_amount, r.processor_amount
    FROM silver_reconciliation_results AS r
    JOIN reference_runs AS k ON k.run_id = r.run_id
),
candidates AS (
    SELECT run_id, reference_date, transaction_id FROM processor_side
    UNION
    SELECT run_id, reference_date, transaction_id FROM service
),
internal_side AS (
    SELECT c.run_id, c.reference_date, c.transaction_id, t.amount AS internal_amount
    FROM candidates AS c
    JOIN silver_transactions AS t
      ON t.transaction_id = c.transaction_id
     AND t.created_date BETWEEN c.reference_date - 7 AND c.reference_date
     AND NOT t.is_deleted
),
recomputed AS (
    SELECT
        coalesce(p.run_id, i.run_id) AS run_id,
        coalesce(p.reference_date, i.reference_date) AS reference_date,
        coalesce(p.transaction_id, i.transaction_id) AS transaction_id,
        i.internal_amount,
        p.processor_amount,
        CASE
            WHEN i.transaction_id IS NULL THEN 'UNRECONCILED_PROCESSOR'
            WHEN p.transaction_id IS NULL THEN 'UNRECONCILED_INTERNAL'
            WHEN abs(i.internal_amount - p.processor_amount) <= 0.01 THEN 'MATCHED'
            ELSE 'MISMATCHED'
        END AS category
    FROM processor_side AS p
    FULL OUTER JOIN internal_side AS i ON i.run_id = p.run_id AND i.transaction_id = p.transaction_id
)
SELECT
    coalesce(s.reference_date, c.reference_date) AS reference_date,
    coalesce(s.run_id, c.run_id) AS run_id,
    coalesce(s.transaction_id, c.transaction_id) AS transaction_id,
    s.category AS service_category,
    c.category AS recomputed_category,
    s.internal_amount AS service_internal_amount,
    s.processor_amount AS service_processor_amount,
    c.internal_amount AS recomputed_internal_amount,
    c.processor_amount AS recomputed_processor_amount,
    CASE
        WHEN s.category IS NULL THEN 'missing_in_service'
        WHEN c.category IS NULL THEN 'missing_in_recompute'
        WHEN s.category = c.category THEN 'agrees'
        ELSE 'category_differs'
    END AS outcome
FROM service AS s
FULL OUTER JOIN recomputed AS c ON c.run_id = s.run_id AND c.transaction_id = s.transaction_id
