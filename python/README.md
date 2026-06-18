# QuestDB cold storage → Apache Iceberg (zero-copy)

Register QuestDB's S3 cold-storage Parquet as an Apache Iceberg table **without
copying or rewriting any data**. PyIceberg's `add_files` writes only Iceberg
metadata (manifests) that point at the Parquet files already in your bucket, and
partitions the table by `hour(timestamp)`.

The dataset is an FX order book: `timestamp`, `symbol`, `bids` and `asks` as
`list<list<double>>`, plus `best_bid` / `best_ask`, Hive-partitioned to the hour
(`year=/month=/day=/hour=`).

## Requirements

- Python 3.9+
- `pip install -r requirements.txt`
- AWS credentials that can read the data bucket and write the warehouse bucket
  (see **AWS authentication** below)

## Usage

Nothing site-specific is hardcoded: `--bucket`, `--prefix`, `--region` and
`--warehouse` are required, so the script works against any QuestDB cold-storage
layout. Keep `--warehouse` (where Iceberg writes table metadata) in a bucket
**separate from your data bucket** — the script never assumes one. Fill in your
own values:

```bash
# first run: create the table and register every file
python questdb_to_iceberg.py \
  --bucket    YOUR_DATA_BUCKET \
  --prefix    cold_storage/YOUR_TABLE~VERSION \
  --region    YOUR_REGION \
  --warehouse s3://YOUR_ICEBERG_BUCKET/warehouse

# re-run later to pick up new partitions (incremental, see below)
python questdb_to_iceberg.py --bucket ... --prefix ... --region ... --warehouse ...

# drop and re-register from scratch (catalog only; S3 data untouched)
python questdb_to_iceberg.py --bucket ... --prefix ... --region ... --warehouse ... --rebuild

# override the namespace (default "questdb")
python questdb_to_iceberg.py --bucket ... --prefix ... --region ... --warehouse ... --namespace analytics
```

Key flags (`--help` for all): `--bucket`, `--prefix`, `--region`, `--warehouse`
(required), plus `--profile`, `--sso-profile`, `--namespace`, `--catalog-db`,
`--ts-col`, `--rebuild`, `--sample-rows`.

The Iceberg **table name is inferred from the QuestDB prefix** — QuestDB
cold-storage prefixes end in `table_name~version`, so
`cold_storage/market_data~699` yields table `market_data`. `--namespace` (default
`questdb`) is prepended, giving `questdb.market_data`. The table name itself is
not configurable; it always tracks QuestDB's.

## Incremental registration vs. Athena partition projection

This is the most important operational point.

Athena over a Hive layout can use `projection.enabled = true` to **auto-discover
partitions at query time** from a path template, so new hours appear with zero
maintenance. **Iceberg has no equivalent.** Iceberg is a manifest-based table
format: the metadata holds an explicit list of every data file. That is what
gives it snapshot isolation, time travel, and fast planning with no S3 listing
at query time, but the trade-off is that **new files never appear on their own** —
something has to commit them.

So when QuestDB writes a new hourly partition, you must run a registration step.
You do **not** need `--rebuild` for that. By default this script is
**incremental**:

1. List every `data.parquet` under the prefix.
2. Diff against the files already registered in the table.
3. `add_files` only the new ones, in a new snapshot (metadata-only, zero-copy).

A run with nothing new is cheap (one S3 listing + a set diff) and prints
`nothing to do; table is up to date`. To keep the table current hands-off,
**schedule the incremental run** (cron / Lambda / Airflow) on roughly the cadence
QuestDB writes partitions (hourly). `--rebuild` is only for a clean slate or a
schema change; it drops the catalog entry and re-registers all files (the S3 data
is never touched).

| | Athena + projection | Iceberg (this script) |
|---|---|---|
| New partition visibility | automatic at query time | after an `add_files` run |
| Per-query S3 listing | yes | no (manifest is authoritative) |
| Snapshot isolation / time travel | no | yes |
| Maintenance to stay current | none | schedule incremental run |

## AWS authentication

All AWS credential handling is isolated in one function, `get_aws_credentials()`.
The default uses an AWS SSO profile (refreshing the session if expired). If you
authenticate differently, replace just that function body:

```python
# static access keys
return AwsCredentials("AKIA...", "secret...", None)

# environment vars / shared config / instance or role profile
c = boto3.Session().get_credentials().get_frozen_credentials()
return AwsCredentials(c.access_key, c.secret_key, c.token)
```

Everything downstream (boto3 client, pyarrow filesystem, catalog properties)
derives from the returned `AwsCredentials`; nothing else needs to change. The
`token` is `None` for static (non-STS) keys, and the catalog props omit the
session-token key in that case.

## Two QuestDB ↔ PyIceberg `add_files` compatibility shims

QuestDB's Parquet is valid and reads fine in Athena, DuckDB, Spark and
PyIceberg's own reader. Two quirks bite **only** PyIceberg's `add_files` path, so
the script patches PyIceberg internals **scoped to the `add_files` call** via the
`questdb_add_files_compat()` context manager (originals restored on exit).

1. **Nested-list element naming.** QuestDB serializes `list<list<double>>` with
   element nodes named `list`, so the leaf path is `bids.list.list.list.element`.
   `add_files` builds a name→field-id map that hardcodes the Parquet-canonical
   `list.element`, so the lookup misses. This is a PyIceberg `add_files`
   limitation, not a QuestDB defect — Iceberg's real identity mechanism is field
   IDs, not element names, and every reader matches the list structurally. The
   shim routes the unmapped leaf to its column's field id. (Iceberg discards
   min/max for nested types anyway and keeps only counts.)

2. **No min/max column statistics.** QuestDB writes Parquet with
   `has_min_max=False` on every column, including the designated timestamp. So
   `add_files` cannot infer the `hour(timestamp)` partition value from the footer
   and emits a null partition, which crashes the Avro manifest writer. Since the
   data is already Hive-partitioned to the hour and each file holds exactly one
   hour, the shim reads the partition value straight from the `year=/month=/day=/
   hour=` path. (This missing-stats gap also costs Athena/DuckDB row-group
   skipping on data-column predicates.)

The clean long-term fix for #1 is upstream: PyIceberg `add_files` matching nested
columns by field ID (as its read path does) instead of by hardcoded name. Until
then, the shims are the price of doing this zero-copy, which is a hard
requirement here — the only patch-free alternative is to rewrite all the data
through Iceberg's writer, which abandons the whole "cold storage stays put" goal.
