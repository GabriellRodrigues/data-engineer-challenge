# Data contracts on Bronze ingestion

Each source dataset has an ODCS-style YAML contract in `etl_jobs/bronze/contracts/<dataset>.yml`.

## Sources and dataset names

The job reads every `.parquet` and `.csv` file under `DATA_PATH`, **including subfolders**
(sorted by path); other files are ignored. Each file is identified by its path relative to
`DATA_PATH` (e.g. `paysettler/settlement_2025-03-01.csv`, or just `enterprise_company.parquet` at the
top level). That identifier is stored in `_source_file`, names the observability step and is what
`--reprocess` expects.

The dataset (contract and Bronze table) comes from the file name, using `DATASET_PATTERNS` in
`etl_jobs/bronze/ingest_to_bronze.py`:

| File name | Dataset |
|---|---|
| `<name>_batch_<N>.<ext>` (e.g. `transactions_batch_1.parquet`) | `<name>` |
| `settlement_<YYYY-MM-DD or YYYYMMDD>*.csv` (daily PaySettler files) | `settlement_paysettler` |
| anything else | the file stem (e.g. `settlement_paysettler.csv` → `settlement_paysettler`) |

All files of a dataset land in the same table (`data/bronze/<dataset>`), and `_source_file` tells
them apart.

Parquet keeps the types written by the producer. CSV is read with an explicit dialect (UTF-8, `,`
separator, `"` quote, header on the first line) and **every column as `varchar`**: Bronze stores the
file exactly as received, and type parsing happens in Silver. Because of that, CSV contracts declare
`physicalType: varchar` and use `logicalType` plus `logicalTypeOptions.pattern` to describe and
check the value format. The PaySettler `amount` accepts `152.30`, `152,30` and `R$ 1.152,30`, all
of which appear in real files; any other format is refused. Empty CSV fields are read as null.

A second processor plugs in with a new contract, a new entry in `DATASET_PATTERNS` (only if its
file names need mapping) and, if it uses another format, a new reader in `READERS`.

## Enforcement

Enforcement runs in three steps, in this order, before anything is written.

### 1. Contract file validation (`src/data_contract/contract_file_validator.py`)

The YAML must have the mandatory ODCS fields, a known `logicalType` and a supported `physicalType`
(`bigint`, `integer`, `smallint`, `double`, `float`, `varchar`, `boolean`, `date`).
An invalid contract stops the inge| Required column absent | removed column | **pipeline fails** |
| Contract column absent and a new column has the same name ignoring case, `_`, `-` and spaces (`amount` → `Amount`) | renamed column | **pipeline fails** (also for optional columns) |
| Column type differs from `physicalType` | type change | **pipeline fails** |
| Column not declared in the contract | new column | kept and written (`schema_mode="merge"`), `WARNING` logged |
| Optional (`required: false`) column absent | older producer version | filled with nulls, `WARNING` logged |

Optional columns may be absent because the source has schema drift between batches: `payment_method`
only exists from `transactions_batch_2` on. A rename to a completely different name (for example
`amount` → `value`) cannot be told apart from removal + addition; it fails as a removed column if
the column is required, and the new name shows up in the error log.

A schema failure raises `SchemaContractError`. The job keeps processing the other files, but the run
ends with exit code 1, so the orchestrator (or `make bronze`) sees the failure.

### 3. Record validation (`src/data_contract/record_validator.py`)

Every row is checked by one vectorized DuckDB query built from the contract:

| Contract attribute | Rule |
|---|---|
| `required: true` | value is not null |
| `logicalType: date` stored as `varchar` | value parses as an ISO 8601 date/timestamp |
| `logicalTypeOptions.pattern` | value fully matches the regex |
| `logicalTypeOptions.minimum` / `maximum` | numeric bounds |
| `logicalTypeOptions.minLength` / `maxLength` | string length bounds |
| `quality: [{type: library, rule: validValues, validValues: [...]}]` | value is in the accepted list |

A row that breaks at least one rule does not go to the main table. It goes to the dead letter table
`data/bronze/<dataset>_refused`, with all its original columns, the Bronze metadata (`_run_id`,
`_source_file`, `_source_file_hash`, `_ingested_at`) and `_refused_reasons` (a list with every
violated rule). The count of each violation is logged as a `WARNING`.

Bronze stays tolerant on purpose: bad rows are quarantined with their reason instead of stopping
the pipeline, while schema changes stop it because they affect every downstream table.

## Idempotency and reprocessing

A file is skipped when its SHA-256 is already in the main table. Otherwise, the refused rows of
that hash are deleted from `<dataset>_refused` before the new refused rows are appended, so a
re-run after a partial failure does not duplicate rows in the dead letter table. The main table is
written last, so a failure never leaves a file marked as ingested with rows missing. A file with no
valid rows is never marked as ingested in the main table and is reprocessed on every run; this is
safe because the refused rows are replaced, not appended twice.

The hash check alone cannot handle two cases, which need an explicit reprocess:

- **The contract was fixed** (for example, a valid value was missing from `validValues`): the file
  is already ingested, so its refused rows would stay in the dead letter table forever.
- **The producer resent a corrected file with the same name**: the new hash would be appended next
  to the old version, duplicating the file in Bronze.

```bash
make reprocess FILES="transactions_batch_1.parquet transactions_batch_2.parquet" REASON="status CANCELLED added to the contract"
# or
python -m etl_jobs.bronze.ingest_to_bronze --reprocess transactions_batch_1.parquet --reason "..."
```

With `--reprocess`, only the listed files run. Each one is validated against the current contract
first; only then are all rows with that `_source_file` (every previous version) deleted from both
the main and the refused table, and the file is written again under a new `_run_id`. If the schema
check fails, nothing is deleted. Unknown file names stop the run before any change. The deletes are
Delta commits, so the replaced rows remain reachable through time travel until `VACUUM`.
`make reprocess` also runs table maintenance afterwards, like `make bronze`.

## Load traceability

Every load is traceable at two levels:

- **Row:** besides `_run_id`, `_source_file`, `_source_file_hash` and `_ingested_at`, each row has
  `_ingestion_mode` = `ingest` (regular run) or `reprocess`. It answers "which rows came from a
  reprocess?" with a simple query, but only describes what is in the table now.
- **Commit:** every `WRITE` and `DELETE` made by the job carries custom metadata in the Delta log:
  `run_id`, `ingestion_mode`, `source_file`, `source_file_hash`, `contract_version` (the `version`
  of the contract the file was validated with) and, for reprocessing, the optional
  `reason` (`--reason` / `REASON=`). This is the record of what was **removed**: the `DELETE` commit
  says which run removed the rows of which file, why and how many (`num_deleted_rows`), and time
  travel to the version before it shows the replaced rows.

```python
from deltalake import DeltaTable

for commit in DeltaTable("data/bronze/transactions").history():
    print(commit["version"], commit["operation"], commit.get("ingestion_mode"), commit.get("reason"))
```

`--reason` is optional so that the command stays simple locally, but for audit purposes it should
always be filled when reprocessing; it is rejected without `--reprocess`. Rows written before the
column existed have `_ingestion_mode` null. When the run registry (`pipeline_run_step`) exists,
these fields belong there as well.

Concurrent runs of the job are not locked: two runs over the same new file could both append it.
Running one Bronze job at a time is the orchestrator's responsibility.

## Uniqueness

`primaryKey` is not enforced in Bronze: the source is CDC, so the same `transaction_id` appears
once per event. Deduplication by the latest `_timestamp` belongs to Silver.
