# Delta table maintenance

`make optimize` (`etl_jobs/maintenance/optimize_tables.py`) runs `maintain_table` from
`src/common/delta_maintenance.py` on every Bronze table listed in `BRONZE_ZORDER`, every Silver table in
`SILVER_ZORDER`, every Gold table in `GOLD_ZORDER` and on the observability tables
(`OBSERVABILITY_ZORDER`, compaction only). Gold facts are Z-ordered by `transaction_id`, the key of
the audit lookups; dimensions and aggregates are only compacted. Each table is a step of the
`maintenance.optimize` run in `pipeline_run_step` (see `observability.md`), and a failure on one
table does not stop the others. Each table goes through:

1. Applies the table properties (`delta.deletedFileRetentionDuration` and
   `delta.logRetentionDuration`, both `RETENTION_DAYS` = 30 days).
2. Optimizes the table: Z-order on the listed columns, or compaction when no columns are listed,
   **only if the data changed since the last `OPTIMIZE`**.
3. Vacuums files older than the retention, writes a checkpoint and cleans up expired log entries.

## Why optimize is skipped when nothing changed

Z-order in delta-rs is not incremental: every call sorts and rewrites all files of the table, even
when they are already ordered. Running maintenance after each ingestion would add a new `OPTIMIZE`
version with the same data every time, growing the log and the files that `VACUUM` has to remove.
Compaction does not have this problem (with nothing to merge it commits nothing), but goes through
the same check for consistency.

`needs_optimize` reads the table history from the newest commit backwards:

- `OPTIMIZE` found first → the table is already optimized, the step is skipped and logged.
- A data change found first (`WRITE`, `DELETE`, `MERGE`, `UPDATE` or any operation not listed
  below) → optimize runs.
- Maintenance commits (`SET TBLPROPERTIES`, `VACUUM START`, `VACUUM END`) are ignored, because they
  do not change the data.

Unknown operations count as data changes, so a new kind of write never leaves a table unoptimized.
A `--reprocess` of Bronze (`DELETE` + `WRITE`) therefore triggers a new optimize, as it should.

## Retention and auditing

`VACUUM` permanently removes files that are no longer part of the table after the retention period,
which also ends time travel to those versions. 30 days is the local default; in production the
retention has to follow the regulatory audit policy.
