WITH bounds AS (
    SELECT min(day) AS first_day, max(day) AS last_day
    FROM (
        SELECT created_date AS day FROM silver_transactions
        UNION ALL SELECT reference_date FROM silver_reconciliation_runs
        UNION ALL SELECT reference_date FROM silver_settlements
        UNION ALL SELECT settled_date FROM silver_settlements
    )
),
days AS (
    SELECT CAST(unnest(generate_series(first_day::TIMESTAMP, last_day::TIMESTAMP, INTERVAL 1 DAY)) AS DATE) AS date_key
    FROM bounds
    WHERE first_day IS NOT NULL
)
SELECT
    date_key,
    year(date_key)::BIGINT AS year,
    quarter(date_key)::BIGINT AS quarter,
    month(date_key)::BIGINT AS month,
    strftime(date_key, '%Y-%m') AS year_month,
    weekofyear(date_key)::BIGINT AS iso_week,
    isodow(date_key)::BIGINT AS day_of_week,
    dayname(date_key) AS day_name,
    isodow(date_key) >= 6 AS is_weekend
FROM days
