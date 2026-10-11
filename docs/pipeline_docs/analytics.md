# Analytics: questions, queries and answers

What each consumer can ask the Gold layer, the query that answers it and what it returns on the
challenge sample (`docs/sample-data`). The KPI definitions and priorities are in
[`data-product.md`](data-product.md); the tables and rules in [`gold-layer.md`](gold-layer.md).
Every query is a file in `etl_jobs/gold/queries/` and runs with `make queries`.

- [1. How to run](#1-how-to-run)
- [2. Operations](#2-operations)
- [3. CFO](#3-cfo)
- [4. Compliance](#4-compliance)
- [5. Why these questions and not others](#5-why-these-questions-and-not-others)

## 1. How to run

```bash
rm -rf data/bronze data/silver data/gold data/observability
docker compose exec -e DATA_PATH=/app/docs/sample-data pipeline python -m etl_jobs.bronze.ingest_to_bronze
make silver
make gold
make queries                 # writes data/reports/gold_queries.md
make ui                      # or explore the gold schema in the DuckDB UI
```

The numbers below are from that sequence. Amounts are BRL (the sample has only BRL; with the
generator every money query returns one row per currency).

## 2. Operations

Pain: *"is the discrepancy rate going up?"* — day-to-day health of the reconciliation.

| # | Question | Query | Tables | Answer on the sample |
|---|---|---|---|---|
| O1–O3 | Is the discrepancy rate rising compared with the recent runs? | `operations/01_discrepancy_trend.sql` | `agg_reconciliation_daily` | stable between 11.4% and 14.6%; the latest run (2025-03-31) is 11.36%, 2.08 p.p. **below** the baseline of the previous 5 runs; the highest deviation was 2025-03-19 (+1.51 p.p.), under the 5 p.p. alert |
| O5 | Which runs failed, are stuck, or never happened? | `operations/02_run_health.sql` | `agg_reconciliation_daily`, `fact_reconciliation_run` | cadence 3 days; 2025-03-22 has only a `FAILED` run (run 7, 116 transactions in the file, 0 results), flagged `is_missing_run`; no stuck runs |
| O10 | Does the service's own total match what it stored? | same query, column `total_gap` | `fact_reconciliation_run` | every completed run has fewer results than `total_transactions` (run 1: 119 vs 106 after deduplication) |
| O4 | How much money is in discrepancy in the latest run? | `operations/03_amount_at_risk_latest.sql` | `agg_amount_at_risk_daily` | 2025-03-31: R$ 45,056.12 at risk, of which R$ 38,301.79 settled without internal record, R$ 5,798.68 internal not settled, R$ 955.65 of value differences |
| O9 | Which merchants concentrate the problems? | `operations/04_merchant_hotspots.sql` | `agg_reconciliation_merchant` | last 30 days, merchants with at least 5 results: MERCH_039 leads with 5 discrepancies in 19 results (26%); among the top 10, MERCH_019 and MERCH_036 have the highest rate (36%) |
| O7 | How long have unmatched internal transactions been open? | `operations/05_unreconciled_aging.sql` | `fact_transaction` | 12 transactions still `UNRECONCILED_INTERNAL`, R$ 334,575.42: 4 up to 7 days, 4 at 8–14, 4 at 15–21 days |
| O8 | Do discrepancies resolve themselves in later runs? | `operations/06_resolution_rate.sql` | `fact_transaction` | 181 transactions were judged more than once; 29 became `MATCHED` after a discrepancy, 20 are still open (59% resolution) |

**What Operations does not get, on purpose:** the root cause of a mismatch and the manual workflow
to treat it (no resolution data exists; O8 is a proxy), and real-time alerts (the cadence is the
batch of each `reference_date`).

## 3. CFO

Pain: *"volume transacted per day"* — consolidated financial view.

| # | Question | Query | Tables | Answer on the sample |
|---|---|---|---|---|
| F1, F2, F8 | How much was transacted per day, with how many transactions and what ticket, and how it changed day over day? | `cfo/01_daily_tpv.sql` | `agg_volume_daily` | 30 days in March 2025; e.g. 2025-03-01: R$ 656,062.67 in 26 transactions (ticket R$ 25,233.18); 2025-03-11 grew 74% over the previous day |
| F3 | How much is completed, pending or failed? | `cfo/02_volume_by_status.sql` | `agg_volume_daily`, `dim_date` | March: R$ 21.12M completed (84%), R$ 2.78M pending (11%), R$ 1.24M failed (5%) |
| F4 | Who are the largest merchants and how concentrated is the volume? | `cfo/03_merchant_concentration.sql` | `agg_merchant_volume_daily`, `dim_merchant` | top 10 merchants hold 27% of completed volume; the largest (MERCH_002) 3% — low concentration |
| F5, F6 | How much did the processor settle and reverse? | `cfo/04_settled_vs_reversed.sql` | `agg_settlement_daily` | 2025-03-16: R$ 11.75M settled, R$ 510k reversed (27 lines, 5.4%), net R$ 11.24M |
| F7 | How much money is exposed by unresolved reconciliation? | `cfo/05_financial_exposure.sql` | `fact_transaction`, `fact_reconciliation_result` | R$ 412,933.80 settled without internal record (16), R$ 334,575.42 internal not settled (12), R$ 11,721.49 of value differences (45) |

**What the CFO does not get, on purpose:** revenue, fees and MDR (no source carries them),
conversion between currencies (no FX source; USD and EUR are reported in their own currency, never
added to BRL), settlement lag (computable, but the sample has one file; P2), and a breakdown by
payment method (80% nulls).

## 4. Compliance

Pain: *"audit history"* — traceability of what was decided, when, and from which data.

| # | Question | Query | Tables | Answer on the sample |
|---|---|---|---|---|
| C1 | What happened to a transaction, in order? | `compliance/01_transaction_trail.sql` (replace the `target` CTE with the id) | `fact_transaction_event`, `fact_reconciliation_result` | the example transaction was `MATCHED` by run 1 with 2,805.58, then appears in CDC as created `FAILED` 40,419.11, updated to `COMPLETED`, and `MATCHED` again by run 6 with 28,516.52 — an inconsistent history worth an audit question |
| C2 | Which file produced which results, loaded when, by which pipeline run? | `compliance/02_run_lineage.sql` | `fact_reconciliation_run`, `fact_reconciliation_result` | 10 runs, each with its processed file, the Bronze extracts and load times, and the Gold run that published them |
| C3 | Which transactions changed after being reconciled? | `compliance/03_changes_after_reconciliation.sql` | `fact_transaction_event`, `fact_transaction` | 6: 3 status changes (`PENDING`/`FAILED` → `COMPLETED`) and 3 deletes, all after a `MATCHED` verdict |
| C4 | Who was the merchant at the time of the transaction? | `compliance/04_merchant_as_of.sql` | `fact_transaction`, `dim_merchant` | 81 transactions were made while the merchant was `... Ltda` and the registry now says `... S.A.` (the query shows the first 20); CNPJ shown masked |
| C6 | Which transactions were deleted and which settlements reversed? | `compliance/05_deletes_and_reversals.sql` | `fact_transaction_event`, `fact_settlement` | 6 deleted transactions and 27 reversed settlements, each with its source file |
| — | Do the service verdicts follow the business rules? | `compliance/06_reconciliation_recheck.sql` | `audit_reconciliation_recheck` | no: for 2025-03-16 the recomputation disagrees everywhere, because the sample CSV is not the file of run 5 and most verdicts are outside the 7-day window ([`gold-layer.md`](gold-layer.md), section 3) |

C5 (referential integrity) is a data quality check, not a query: `dq_results` holds it for every
run (orphan results, transactions without merchant, unknown currency).

**What Compliance does not get, on purpose:** the full CNPJ (masked in Gold; Silver keeps it for a
restricted audience), the state of a table at any past instant through SQL (Delta time travel
exists, `VERSION AS OF`, but no Gold table snapshots history per version), and the reason a
transaction was deleted (not in the source).

## 5. Why these questions and not others

- **The P0 questions are the literal pains.** "Is the discrepancy rate going up" (O1–O3), "volume
  per day" (F1–F3) and "audit history" (C1, C2) were built first, together with what makes them
  trustworthy: deduplication, the reference run per date and conservation checks between layers.
- **Money and rates are separated by grain.** The window overlap makes the same transaction appear
  in up to four runs. Rates are per run (Operations), money is per transaction (CFO). Mixing them
  is the most likely wrong number in this domain, so the model makes it hard: there is no table
  that sums amounts across runs.
- **P1 questions reuse the same facts.** Hotspots, aging, resolution, concentration, reversals and
  changes after reconciliation are columns or small aggregates on the P0 facts; they cost little
  and answer the next question each consumer would ask.
- **What the data cannot support is left out, not approximated.** Fees, FX, payment method and
  settlement lag would produce numbers that look precise and are not. They are listed above and in
  `data-product.md` section 7.
- **Scope vs time.** Gold is batch, per `reference_date`, on Delta read by DuckDB. A semantic layer,
  dashboards and per-consumer access control are the next step, not part of this delivery.
