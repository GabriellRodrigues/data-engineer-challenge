WITH runs AS (
    SELECT
        reference_date,
        count(*) AS runs_count,
        count(*) FILTER (WHERE status = 'COMPLETED') AS completed_runs,
        count(*) FILTER (WHERE status = 'FAILED') AS failed_runs,
        count(*) FILTER (WHERE status = 'IN_PROGRESS') AS in_progress_runs,
        count(*) FILTER (WHERE is_stuck) AS stuck_runs,
        any_value(run_id) FILTER (WHERE is_reference_run) AS reference_run_id
    FROM fact_reconciliation_run
    GROUP BY reference_date
),
verdicts AS (
    SELECT
        reference_date,
        result_count,
        matched_count,
        mismatched_count,
        unreconciled_processor_count,
        unreconciled_internal_count,
        discrepancy_count,
        discrepancy_rate,
        duration_s
    FROM fact_reconciliation_run
    WHERE is_reference_run
),
gaps AS (
    SELECT reference_date - lag(reference_date) OVER (ORDER BY reference_date) AS gap
    FROM runs
),
cadence AS (
    SELECT coalesce(mode(gap), 1) AS cadence_days FROM gaps WHERE gap IS NOT NULL
),
bounds AS (
    SELECT min(reference_date) AS first_date, max(reference_date) AS last_date FROM runs
),
calendar AS (
    SELECT
        d.date_key AS reference_date,
        (d.date_key - b.first_date) % c.cadence_days = 0 AS is_expected_run_date,
        c.cadence_days
    FROM dim_date AS d
    CROSS JOIN bounds AS b
    CROSS JOIN cadence AS c
    WHERE d.date_key BETWEEN b.first_date AND b.last_date
),
volume AS (
    SELECT
        reference_date,
        median(result_count) OVER (ORDER BY reference_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING)
            AS result_count_baseline
    FROM verdicts
    WHERE discrepancy_rate IS NOT NULL
),
comparable AS (
    SELECT
        v.reference_date,
        v.discrepancy_rate,
        w.result_count_baseline,
        w.result_count_baseline IS NULL OR v.result_count >= 0.5 * w.result_count_baseline AS is_comparable_volume
    FROM verdicts AS v
    JOIN volume AS w ON w.reference_date = v.reference_date
),
trend AS (
    SELECT
        reference_date,
        avg(discrepancy_rate) OVER (ORDER BY reference_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
            AS discrepancy_rate_baseline
    FROM comparable
    WHERE is_comparable_volume
)
SELECT
    k.reference_date,
    k.cadence_days::BIGINT AS cadence_days,
    k.is_expected_run_date,
    coalesce(r.runs_count, 0) AS runs_count,
    coalesce(r.completed_runs, 0) AS completed_runs,
    coalesce(r.failed_runs, 0) AS failed_runs,
    coalesce(r.in_progress_runs, 0) AS in_progress_runs,
    coalesce(r.stuck_runs, 0) AS stuck_runs,
    k.is_expected_run_date AND coalesce(r.completed_runs, 0) = 0 AS is_missing_run,
    r.reference_run_id,
    v.duration_s,
    coalesce(v.result_count, 0) AS result_count,
    coalesce(v.matched_count, 0) AS matched_count,
    coalesce(v.mismatched_count, 0) AS mismatched_count,
    coalesce(v.unreconciled_processor_count, 0) AS unreconciled_processor_count,
    coalesce(v.unreconciled_internal_count, 0) AS unreconciled_internal_count,
    coalesce(v.discrepancy_count, 0) AS discrepancy_count,
    v.discrepancy_rate,
    safe_rate(v.mismatched_count, v.result_count) AS mismatch_rate,
    safe_rate(v.unreconciled_processor_count, v.result_count) AS unreconciled_processor_rate,
    safe_rate(v.unreconciled_internal_count, v.result_count) AS unreconciled_internal_rate,
    t.discrepancy_rate_baseline,
    v.discrepancy_rate - t.discrepancy_rate_baseline AS discrepancy_rate_deviation,
    c.result_count_baseline::DOUBLE AS result_count_baseline,
    c.is_comparable_volume
FROM calendar AS k
LEFT JOIN runs AS r ON r.reference_date = k.reference_date
LEFT JOIN verdicts AS v ON v.reference_date = k.reference_date
LEFT JOIN trend AS t ON t.reference_date = k.reference_date
LEFT JOIN comparable AS c ON c.reference_date = k.reference_date
