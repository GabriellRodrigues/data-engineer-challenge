# Data product: KPIs per consumer

What the analytical layer must answer, for whom, and why. This document is the input for the
Silver and Gold models: every table built there should trace back to a KPI or a capability listed
here. For the challenge statement see the root `README.md`; for the business rules (categories,
tolerance, 7-day window) see `docs/domain-glossary.md`.

- [1. The problem](#1-the-problem)
- [2. Sources and their role](#2-sources-and-their-role)
- [3. Findings from the sample data](#3-findings-from-the-sample-data)
- [4. Operations: reconciliation health](#4-operations-reconciliation-health)
- [5. CFO: financial volume](#5-cfo-financial-volume)
- [6. Compliance: traceability and audit](#6-compliance-traceability-and-audit)
- [7. Deliberately out of scope](#7-deliberately-out-of-scope)
- [8. What this requires from Silver](#8-what-this-requires-from-silver)
- [9. Where each KPI lives in Gold](#9-where-each-kpi-lives-in-gold)

## 1. The problem

The Settlement Reconciliation Service works, but its data lives only in the transactional
PostgreSQL (`settlement_db`). Nobody has structured access to it. Three consumers have explicit
pains:

| Consumer | Stated pain | What the model must deliver |
|---|---|---|
| **Operations** | "Is the discrepancy rate going up?" | Day-to-day reconciliation health: discrepancy trend, failed or stuck runs, items to investigate |
| **CFO** | "Volume transacted per day" | Consolidated financial view: by day, merchant, status, internal vs settled |
| **Compliance** | "Audit history" | Traceability: what was reconciled, when, against which file, and how a record changed over time |

Priorities used below:

| Priority | Meaning |
|---|---|
| **P0** | Answers a stated pain directly; built first |
| **P1** | High value, built once P0 is stable |
| **P2** | Derivable by query or limited by the available data; documented, not modeled |

## 2. Sources and their role

| Source | Represents | Analytical role | Caveats |
|---|---|---|---|
| `transactions` | Internal record ("our side") | Base of transacted volume (CFO); internal side of reconciliation (window applies to `created_at`) | CDC (`Op`, `_timestamp`) across two batches with schema drift (`payment_method`); needs latest state per `transaction_id` and kept history |
| `settlement_paysettler.csv` | What the processor says it settled in the last 24h | Settled volume, reversals (`REVERSED`), external side of reconciliation | External dirty data; extension point for a second processor |
| `reconciliation_runs` | The process | Operational health (failed, stuck, duration, missing dates); audit anchor (file → results) | CDC `I`/`U` per run; `reference_date` is business time, `started_at`/`completed_at` system time |
| `reconciliation_results` | The verdict per transaction per run | Central fact for Operations (rates, amounts at risk) and audit trail for Compliance | Largest table (~1.8B rows/year at 5M/day); null amounts expected per category; FK to `run_id` |
| `enterprise_company` | The merchant | Merchant dimension (names, CNAE segment); CNPJ as regulatory identifier | Has `updated_at` and CDC updates, so SCD2 matters for audit; CNPJ is sensitive |

```
transactions ──(transaction_id)──┐
                                 ├──► reconciliation_results ──(run_id)──► reconciliation_runs
settlement_paysettler ──(transaction_id)┘          │
                                                   └──(merchant_id)──► enterprise_company
```

`transactions` plus `settlement_paysettler` allow recomputing the reconciliation independently and
comparing it with what the service stored in `reconciliation_results`.

## 3. Findings from the sample data

Profiled on `docs/sample-data/`. Each finding changes how a KPI must be defined.

| # | Finding | Impact on KPIs |
|---|---|---|
| 1 | The same transaction appears in several runs (135 in 2 runs, 38 in 3, 9 in 4): runs every 3 days with a 7-day window overlap | Summing amounts across runs double counts money. KPIs split into **per run** (snapshot of that reconciliation, Operations) and **per transaction, current state** (latest category, finance) |
| 2 | Duplicates inside a run (run 1: 119 rows, 106 distinct `transaction_id`) | Rates are inflated unless Silver deduplicates |
| 3 | Orphan results: `run_id = 99` (5 rows) has no run | Referential check; orphans excluded from KPIs and counted |
| 4 | Runs are not daily (every 3 days); run 7 (2025-03-22) `FAILED` with no results | "Missing run" needs an expected cadence, not a calendar day |
| 5 | Tolerance holds: every `MATCHED` has `difference <= 0.01`, every `MISMATCHED` has `> 0.01` | A data quality check, not a KPI |
| 6 | The CSV covers a single day (2025-03-16): 510 rows, 27 `REVERSED`; only 289 of its ids are in the results | Settled-volume KPIs are modeled but only demonstrable for one day |
| 7 | `enterprise_company`: 70 rows for 50 merchants (20 updates) | SCD2 is relevant for audit |
| 8 | `payment_method` is null in ~80% of transactions | A breakdown by payment method would mislead |
| 9 | The sample is all BRL, but the synthetic generator (`make generate`) produces ~95% BRL, ~4% USD and ~1% EUR | Amounts cannot be summed across currencies: volume KPIs are reported per `currency`, with BRL as the headline figure |
| 10 | 16 results have a null `merchant_id` while their internal transaction has one; the CSV repeats 10 lines; 8 merchant updates change no attribute | Silver fills the merchant from the transaction (`merchant_id_origin`), collapses the repeated lines and the no-op versions |

## 4. Operations: reconciliation health

| # | KPI | Definition | Grain | Priority |
|---|---|---|---|---|
| O1 | **Discrepancy rate** | (MISMATCHED + UNRECONCILED_*) / total deduplicated results | run / `reference_date` | P0 |
| O2 | Rate per category | % of each category in the run (mismatch, unreconciled processor, unreconciled internal) | run | P0 |
| O3 | **Discrepancy trend** | Moving average of O1 over the last N runs, and deviation from it | `reference_date` | P0 (the literal Ops question) |
| O4 | Amount at risk | Σ `difference` of MISMATCHED; Σ amount of unreconciled on each side | run | P0 |
| O5 | Run status | Runs COMPLETED / FAILED / stuck IN_PROGRESS; expected `reference_date` with no run | `reference_date` | P0 (covers troubleshooting, Part 3.2) |
| O6 | Run duration | `completed_at - started_at`, p50/p95 | run | P1 |
| O7 | Unreconciled internal aging | Days since `created_at` of transactions still unmatched in the latest run | transaction | P1 |
| O8 | Resolution rate | % of a run's unreconciled that became MATCHED in a later run | run | P1 (uses the window overlap) |
| O9 | Hotspots by merchant / CNAE | O1 and O4 broken down by merchant and CNAE | merchant × run | P1 |
| O10 | Run coherence | `total_transactions` vs result count; orphan results | run | P0, as a data quality check |

**Questions answered:** is the discrepancy rate rising; which run failed or never ran; how much
money is in discrepancy today; which merchants concentrate the problems; how long unmatched items
stay open.

## 5. CFO: financial volume

| # | KPI | Definition | Grain | Priority |
|---|---|---|---|---|
| F1 | **Daily transacted volume (TPV)** | Σ `amount` with status COMPLETED and not deleted, by `created_at` date, on the latest CDC state, per `currency` (BRL headline) | day × currency | P0 (the literal CFO question) |
| F2 | Count and average ticket | count and TPV / count | day | P0 |
| F3 | Volume by status | TPV for COMPLETED, PENDING, FAILED (open pipeline and lost volume) | day × status | P0 |
| F4 | TPV by merchant / CNAE | F1 by dimension, plus concentration (top 10 merchants as % of total) | day × merchant | P1 |
| F5 | Settled volume | Σ SETTLED, Σ REVERSED, net (settled − reversed), by `settled_at` | day | P1 (one file in the sample) |
| F6 | Reversal rate | REVERSED / total settled, in amount and count | day / merchant | P1 |
| F7 | Financial exposure | Current-state amount not MATCHED: expected and not seen by the processor, plus settled without internal record | snapshot | P0 (links reconciliation to money) |
| F8 | Growth | Change of F1 vs previous day / week / month | day | P2 (a query over F1) |

**Questions answered:** how much was transacted per day and by whom; how much is still pending or
failed; how much was actually settled and reversed; how much money is exposed by unresolved
reconciliation.

## 6. Compliance: traceability and audit

The product here is mainly the ability to answer questions; indicators come second.

| # | KPI / capability | Definition | Priority |
|---|---|---|---|
| C1 | **Transaction trail** | For a `transaction_id`: every CDC version, every run it appeared in, its category in each, and the source file | P0 |
| C2 | Run lineage | run → `file_name` → results → Bronze file and load (`_source_file`, `_ingested_at`) | P0 |
| C3 | **Changes after reconciliation** | Transactions whose `amount` or `status` changed in CDC after being reconciled | P1 (strong risk signal) |
| C4 | Registry history | Legal name and CNAE valid at the transaction date (SCD2 of `enterprise_company`) | P1 |
| C5 | Referential integrity | % of results with a valid run, % of transactions with a registered merchant, orphan count | P0, as a data quality check |
| C6 | Deletes and reversals | Transactions with `Op = D` and `REVERSED` settlements, listed for audit | P1 |

**Questions answered:** what happened to a given transaction and when; which file produced which
result; what changed after a reconciliation was closed; who the merchant was at the time.

## 7. Deliberately out of scope

| Not answered | Why |
|---|---|
| Revenue, fees, MDR | No source carries fees |
| FX conversion | No exchange-rate source; USD/EUR volumes are reported in their own currency, never converted or added to BRL |
| Settlement lag (`settled_at − created_at`) | Computable, but the sample has a single CSV; P2 / production idea |
| Breakdown by `payment_method` | ~80% nulls due to schema drift; the metric would mislead |
| Chargeback reasons, exception-handling workflow | No manual resolution data; O8 is only a proxy |
| Real time | Batch cadence by `reference_date` |

## 8. What this requires from Silver

For the P0 KPIs, Silver must deliver:

| Silver output | Needed by |
|---|---|
| `transactions` latest state per `transaction_id`, plus the full CDC history | F1–F3, F7, C1, C3, C6 |
| `reconciliation_results` deduplicated; orphans moved to `reconciliation_results_quarantine` | O1–O4, O10, C1, C5 |
| `reconciliation_runs` deduplicated (`I`/`U` → latest version) | O5, O6, C2 |
| PaySettler CSV typed and stamped with `reference_date` | F5, F6, F7 |
| `enterprise_company` as SCD2 | O9, F4, C4 |

"Current state per transaction" metrics (F7, O7, O8) are computed in Gold. How each output is
built is in [`silver-layer.md`](silver-layer.md).

## 9. Where each KPI lives in Gold

The tables and rules are in [`gold-layer.md`](gold-layer.md); the queries and their answers on the
sample in [`analytics.md`](analytics.md).

| KPI | Gold table.column | Query |
|---|---|---|
| O1, O2 | `agg_reconciliation_daily.discrepancy_rate`, `mismatch_rate`, `unreconciled_*_rate` (reference run); `fact_reconciliation_run` per run | `operations/01` |
| O3 | `agg_reconciliation_daily.discrepancy_rate_baseline`, `discrepancy_rate_deviation`; alert `discrepancy_rate_spike` | `operations/01` |
| O4 | `agg_amount_at_risk_daily` (per currency); `fact_reconciliation_result.amount_at_risk` | `operations/03` |
| O5 | `agg_reconciliation_daily.is_missing_run`, `failed_runs`, `stuck_runs`; alert `reconciliation_runs_missing` | `operations/02` |
| O6 | `fact_reconciliation_run.duration_s` | `operations/02` |
| O7 | `fact_transaction.days_unreconciled` | `operations/05` |
| O8 | `fact_transaction.is_resolved_discrepancy` | `operations/06` |
| O9 | `agg_reconciliation_merchant` | `operations/04` |
| O10 | `fact_reconciliation_run.total_gap` | `operations/02` |
| F1, F2 | `agg_volume_daily` (`status = 'COMPLETED'`) | `cfo/01` |
| F3 | `agg_volume_daily` by `status` | `cfo/02` |
| F4 | `agg_merchant_volume_daily.share_of_daily_volume` | `cfo/03` |
| F5, F6 | `agg_settlement_daily` | `cfo/04` |
| F7 | `fact_transaction.exposure_amount` + latest `UNRECONCILED_PROCESSOR` verdicts | `cfo/05` |
| F8 | window over `agg_volume_daily` | `cfo/01` |
| C1 | `fact_transaction_event` + `fact_reconciliation_result` | `compliance/01` |
| C2 | lineage columns of every fact + `fact_reconciliation_run.file_name` | `compliance/02` |
| C3 | `fact_transaction_event.is_after_reconciliation`, `fact_transaction.changes_after_reconciliation` | `compliance/03` |
| C4 | `merchant_sk` (version valid at the event) → `dim_merchant` | `compliance/04` |
| C5 | Silver and Gold checks in `dq_results` | — |
| C6 | `fact_transaction_event` (`op = 'D'`), `fact_settlement.is_reversed` | `compliance/05` |
