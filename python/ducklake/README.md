# DuckLake over QuestDB cold storage

A DuckLake metadata layer on top of the **same untouched QuestDB Parquet** that
`../questdb_to_iceberg.py` registers into Iceberg. Nothing here copies data:
`ducklake_add_data_files` only records references to the existing files in cold
storage.

The point: QuestDB writes plain hive-partitioned Parquet
(`year=/month=/day=/hour=/data.parquet`) and does not bind it to any one table
format. The same bytes can be consumed three ways:

| Consumer | How | Timestamps |
|----------|-----|------------|
| **Iceberg** | `../questdb_to_iceberg.py` (or the Java tool) | native **nanoseconds** (v3) |
| **DuckLake** | `ducklake_register.py` (this folder) | **microseconds** (see caveat) |
| **Raw DuckDB** | `read_parquet('s3://.../**/*.parquet', hive_partitioning=true)` | microseconds |

## Setup

```bash
pip install -r requirements.txt
```

The `aws`, `httpfs`, and `ducklake` DuckDB extensions auto-install on first run
(needs network to the DuckDB extension repository).

## AWS credentials

Auth is handled outside Python. Source the wrapper to put temporary creds for
the `questdb-dev` account (337384507863) into your shell; DuckDB reads them via
its `credential_chain`:

```bash
source duck_with_aws_credentials.sh
```

It does the SSO login (`sso-main`) if needed, then
`aws configure export-credentials --profile questdb-dev --format env`. After
that, both the CLI and the scripts here can read S3 with no further auth.

## Register

Flags mirror the Iceberg tools; nothing is hardcoded. `--bucket` and `--prefix`
are required, and the table name is inferred from the prefix
(`cold_storage/market_data~699` → `market_data`), exactly like
`../questdb_to_iceberg.py`. One prefix per run.

```bash
python ducklake_register.py --bucket YOUR_BUCKET --prefix cold_storage/YOUR_TABLE~VER
python ducklake_register.py --bucket ... --prefix ... --prune   # also deregister vanished files
python ducklake_register.py --bucket ... --prefix ... --fresh   # recreate the catalog file first
```

Schema is derived from the Parquet itself: DuckDB infers `UUID`, `DOUBLE[][]`
(the nested order book), `VARCHAR`, etc. Timestamps are declared `TIMESTAMPTZ`
and ns columns are flagged in the output (see below). `--region` is optional (it
falls back to `AWS_DEFAULT_REGION`, which the wrapper sets); `--catalog` defaults
to `questdb_lake.ducklake` in the working directory.

### Keeping in sync: incremental and `--prune`

Runs are **incremental**: the script lists what is under the prefix in S3 and
registers only files not already in the catalog (`add_data_files` is not
idempotent, so re-adding would double-count). If QuestDB has also **dropped**
partitions, add **`--prune`**: it reconciles both sides, adding new files and
**deregistering** the ones no longer in S3. DuckLake has no per-file deregister
(unlike Iceberg's `DeleteFiles`), so `--prune` re-registers the surviving set to
drop the stale references. It is cheap when nothing was dropped (pure incremental
add) and only pays a re-register when a drop is detected. Nothing in S3 is ever
touched.

The catalog is written to `questdb_lake.ducklake` in the working directory by
default (git-ignored). Point any DuckDB session at it:

```sql
ATTACH 'ducklake:questdb_lake.ducklake' AS lake;
SELECT * FROM lake.trades LIMIT 5;
```

## Query

`ducklake_query.py` is the reader (the DuckLake counterpart of
`../iceberg_reader.py`):

```bash
python ducklake_query.py --list                        # tables in the catalog
python ducklake_query.py --table-details trades        # schema + row count (no S3)
python ducklake_query.py --table trades --sample-rows 5
python ducklake_query.py --table fx_trades --day 2026-06-19 --sample-rows 5
```

The reader takes table **names** (not prefixes) since it reads the catalog, and
uses the same `--catalog` default (`questdb_lake.ducklake` in the working
directory; pass `--catalog` to point elsewhere). `--list` and `--table-details`
are answered from catalog metadata (no S3 read); `--sample-rows` and `--day` read
the Parquet from cold storage. `--day` filters one UTC day on the `timestamp`
column (override with `--ts-col`).

## Iceberg vs DuckLake

For how this DuckLake path compares to the Iceberg one over the same QuestDB
Parquet, see [ICEBERG_VS_DUCKLAKE.md](ICEBERG_VS_DUCKLAKE.md).

## The nanosecond caveat

`fx_trades` and `trades` are **nanosecond** in cold storage; `market_data` is
microsecond. DuckLake stores all three at **microsecond**, and this is not a
choice the script makes -- it is forced:

- DuckDB has no nanosecond-*with-timezone* type. Its ns type (`TIMESTAMP_NS`) is
  timezone-naive.
- QuestDB writes the timestamp column `isAdjustedToUTC=1`, so DuckDB routes it to
  the microsecond-only `TIMESTAMP WITH TIME ZONE` and truncates ns **at read**.
- `ducklake_add_data_files` therefore only accepts a `TIMESTAMPTZ` column; a
  `TIMESTAMP_NS` column is rejected.

The nanoseconds are **not lost from storage** -- the Parquet is untouched, and
Iceberg v3 or pyarrow (`timestamp[ns, tz=UTC]`) still surface them. Only DuckDB's
reader truncates. So for full-fidelity ns, use the Iceberg path; for fast,
simple local querying at microsecond resolution, use DuckLake.
