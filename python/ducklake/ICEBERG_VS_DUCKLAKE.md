# Iceberg vs DuckLake over QuestDB cold storage

Both are open table formats that turn a pile of Parquet files into a queryable
table with a schema, snapshots, and statistics. This repo puts **both** on top of
the **same untouched QuestDB Parquet** in cold storage, zero-copy. This note is a
high-level comparison, then the specifics of how each behaves with QuestDB data.

## High-level

| | **Apache Iceberg** | **DuckLake** |
|---|---|---|
| Where metadata lives | JSON + Avro manifest files in object storage, plus a catalog pointer | **All** metadata in a SQL database (here a local `.ducklake`/SQLite file; can be Postgres/MySQL) |
| Catalog | Pluggable (SQLite/JDBC, Glue, REST, Nessie, ...) | The SQL database itself |
| Engines that read it | Broad: Spark, Trino, Flink, Snowflake, BigQuery, Athena, DuckDB (iceberg ext), ... | DuckDB-centric today (can mirror out to Iceberg) |
| Write model | append + delete/overwrite, each a new snapshot (manifest set) | append + delete/overwrite, each a SQL transaction |
| Snapshots / time travel | Yes, via metadata files | Yes, via catalog rows |
| Ops complexity | More moving parts (manifest files, catalog, compaction) | Simpler: one SQL DB holds everything |
| Maturity / reach | Battle-tested, wide ecosystem | Newer, simpler, fast to stand up |

Rule of thumb: **Iceberg** when you want the data readable by the whole
lakehouse ecosystem; **DuckLake** when you want fast, low-friction local
analytics and simpler operations.

## Working with QuestDB data: where they differ

QuestDB writes plain hive-partitioned Parquet
(`year=/month=/day=/hour=/data.parquet`) with a few type quirks. The two formats
handle those quirks differently.

### Timestamps (the big one)

QuestDB stores designated timestamps as UTC instants and writes the Parquet
column `isAdjustedToUTC=1`. `fx_trades` and `trades` are **nanosecond**;
`market_data` is **microsecond**.

- **Iceberg v3** (the Java tool) keeps native **nanoseconds** — lossless.
  Iceberg v2 downcasts ns to microseconds.
- **DuckLake** stores them at **microseconds**. This is forced, not chosen:
  DuckDB has no nanosecond-*with-timezone* type, so it reads the UTC-adjusted ns
  column as microsecond `TIMESTAMPTZ` and `add_data_files` only accepts that
  type. The ns detail is real (most `fx_trades` rows carry sub-µs digits) and is
  **not lost from storage** — Iceberg v3 and pyarrow still surface it; only
  DuckDB's reader truncates.

### UUID

QuestDB writes UUID columns (`fx_trades.trade_id`, `order_id`) with the Parquet
UUID logical type.

- **Iceberg via the Java tool (v3)**: native `uuid`, zero-copy.
- **Iceberg via PyIceberg**: lands as `fixed[16]` (a pyarrow/PyIceberg mapping
  gap).
- **DuckLake / DuckDB**: native `UUID` — inferred automatically.

### Nested arrays (order book)

`market_data.bids` / `asks` are `list<list<double>>`.

- **Iceberg**: `list<list<double>>` (name-mapping must bind the nested element
  fields correctly).
- **DuckLake / DuckDB**: `DOUBLE[][]` — inferred automatically.

### Partition churn (drop / add)

QuestDB cold storage gains and loses hourly partitions over time. **Both formats
support removing data files** (this is not an Iceberg limitation); the difference
is the mechanism:

- **Iceberg**: a file is deregistered with a `DeleteFiles` snapshot (or an engine
  `DELETE FROM` / `ALTER TABLE ... DROP PARTITION`), which drops the manifest
  reference. Metadata only; the S3 object is not deleted. Surgical and cheap.
- **DuckLake**: file references are catalog rows, but its supported removal paths
  are snapshot expiry and orphan/compaction cleanup, with no per-file deregister.
  So dropping a vanished reference is done by re-registering the surviving set.

The two registrars in this repo both reconcile drops with **`--prune`**: a run
lists S3, adds new files, and removes references to files no longer present -- the
Java tool via a surgical `DeleteFiles`, the DuckLake tool by re-registering the
survivors. `--prune` never deletes anything from S3; the vanished objects are
already gone. `--rebuild` (Java) and `--fresh` (DuckLake catalog) remain the blunt
full-reset options.

### Auth / tooling

- **Iceberg (Python)**: PyIceberg + boto3; credentials injected into the FileIO.
  The Java tool uses the AWS SDK with a named profile.
- **DuckLake**: credentials read from the environment via DuckDB's
  `credential_chain` (see `duck_with_aws_credentials.sh`); no boto3 in the tool.

## What's the same: both are native over the Parquet

The important part: **neither format copies or rewrites the data.** Both point at
the exact same Parquet files in cold storage and record only metadata —
Iceberg as manifests, DuckLake as catalog rows. QuestDB's plain hive-partitioned
layout is what makes this possible, and it is not bound to either format.

So the same bytes can be, simultaneously:

- an Iceberg table (native ns via v3, broad engine reach), **and**
- a DuckLake table (microsecond, simple local querying), **and**
- just Parquet (`read_parquet('s3://.../**/*.parquet', hive_partitioning=true)`,
  no catalog at all).

Pick the layer per use case; the cold storage underneath is one copy, untouched.
