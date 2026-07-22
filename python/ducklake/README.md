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

```bash
python ducklake_register.py                 # all known tables
python ducklake_register.py --table trades  # just one
python ducklake_register.py --fresh         # recreate the catalog file first
```

Schema is derived from the Parquet itself: DuckDB infers `UUID`, `DOUBLE[][]`
(the nested order book), `VARCHAR`, etc. Timestamps are declared `TIMESTAMPTZ`
and ns columns are flagged in the output (see below). Table -> S3 prefix mapping
lives in the `TABLES` dict at the top of the script; add a new table's `~<id>`
prefix there to register it.

The catalog is written to `questdb_lake.ducklake` in this folder by default
(git-ignored). Point any DuckDB session at it:

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

`--list` and `--table-details` are answered from catalog metadata (no S3 read);
`--sample-rows` and `--day` read the Parquet from cold storage. `--day` filters
one UTC day on the `timestamp` column (override with `--ts-col`).

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
