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

## QuestDB Parquet is Iceberg-compatible out of the box

QuestDB's cold-storage Parquet registers zero-copy with **stock PyIceberg — no
monkeypatching**. Three things that used to need workarounds are now correct at
the source:

- **List elements are canonically named.** Nested `list<list<double>>` columns
  serialize as `bids.list.element.list.element`, the Parquet-canonical layout
  PyIceberg's `add_files` expects, so nested columns map cleanly.
- **No embedded Parquet field IDs.** Files carry no `PARQUET:field_id`, so
  PyIceberg assigns its own (1-based) IDs and `add_files` accepts the files.
- **min/max column statistics are present.** The designated timestamp has
  footer stats, so `add_files` infers the `hour(timestamp)` partition value
  directly — no need to parse the partition out of the Hive path.

## Nanosecond timestamps (the one adaptation)

QuestDB can store `TIMESTAMP_NS`. Iceberg's support for nanoseconds is worth being
precise about, because the limitation lives at two different layers:

- **The Iceberg spec.** Format versions **v1 and v2 are microsecond-only** —
  `timestamp` / `timestamptz` are defined at µs precision, so nanoseconds are
  genuinely unrepresentable there. Format **v3 added nanosecond types**
  (`timestamp_ns` / `timestamptz_ns`), so the spec itself now supports ns.
- **PyIceberg.** It already defines the type classes (`TimestampNanoType`,
  `TimestamptzNanoType`) but **cannot _write_ v3 metadata yet**
  ([apache/iceberg-python#1551](https://github.com/apache/iceberg-python/issues/1551));
  creating a `format-version=3` table raises `NotImplementedError: Writing V3 is
  not yet supported`.

So the thing blocking a native-ns table **today is PyIceberg, not the spec**: the
spec allows ns as of v3, but this PyIceberg release can't write v3. (PyIceberg's
runtime message *"Iceberg does not yet support 'ns' timestamp precision"* is a bit
misleading — read it as "this v2 write path doesn't support ns.") Even once
PyIceberg ships v3 writes, you'd also want your query engines
(Trino / Spark / Athena / DuckDB) to support v3 ns reads before relying on it.

That makes downcasting to microseconds the pragmatic choice for now, on the
broadly supported v2 format. When the source has ns timestamps, the script
**downcasts them to microseconds**:
the Iceberg column becomes `timestamptz` (µs) while the underlying Parquet is left
untouched (still zero-copy), and reads downcast on the fly. The only cost is
sub-microsecond precision. The script detects this from the file schema, enables
`PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE` (so `create_table` downcasts),
and — because `add_files` ignores that flag in its schema-compatibility check —
forces the downcast there via the small `ns_downcast_compat()` context manager,
scoped to the `add_files` call (original restored on exit). Microsecond tables
take this path as a no-op.

## UUID columns land as `fixed[16]` (for now)

QuestDB stores UUIDs (e.g. `trade_id`, `order_id`) as a 16-byte fixed column and
writes the Parquet **`UUID` logical type** on it — which is exactly how Iceberg
encodes its own `uuid` type (`fixed_len_byte_array(16)` + UUID logical type). So
the bytes on disk are already a valid Iceberg `uuid` column; QuestDB is doing the
right thing.

The catch is the read toolchain, not the data. pyarrow (tested on 20.0.0) reads
the column back as plain `fixed_size_binary[16]` and does not promote the UUID
logical type to its `pa.uuid()` extension, so PyIceberg infers Iceberg `fixed[16]`
and the values display as binary. Forcing the table schema to `uuid` does not help
yet: `create_table` accepts it, but `add_files` then fails with
`ArrowNotImplementedError: extension` because the UUID extension is not wired
through PyIceberg's add_files path in this release.

This is harmless and lossless — the 16 bytes *are* the UUID, the column stays
zero-copy and queryable, and you can format it to the canonical
`8-4-4-4-12` string in the query engine. No workaround is added here on purpose:
because QuestDB already writes the correct UUID logical type, the column will map
to Iceberg `uuid` automatically, with no script changes, once pyarrow surfaces the
logical type as `pa.uuid()` on read (or PyIceberg matures its extension handling).
