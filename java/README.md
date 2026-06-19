# QuestDB cold storage → Apache Iceberg (zero-copy, Java)

Java port of the Python cookbook. Same job: register QuestDB's S3 cold-storage
Parquet as an Iceberg table **without copying or rewriting any data**, partitioned
by `hour(timestamp)`, incremental on re-run. The only writes are Iceberg metadata
(manifests / metadata JSON) to the warehouse bucket; the Parquet files in cold
storage are only ever read.

The reason to reach for Java is **native nanosecond timestamps and native UUIDs** -
the two types PyIceberg cannot keep today (it downcasts ns→µs and falls back to
`fixed[16]`). Java is the Iceberg reference implementation and handles both.

## Requirements

- JDK 17+ (built/tested on JDK 25)
- Maven 3.9+
- AWS credentials that can read the data bucket and write the warehouse bucket

## Build

```bash
mvn -f java/pom.xml package -DskipTests
# produces java/target/questdb-to-iceberg.jar (a shaded, runnable fat jar)
```

## Usage

```bash
java -jar java/target/questdb-to-iceberg.jar \
  --bucket    YOUR_DATA_BUCKET \
  --prefix    cold_storage/YOUR_TABLE~VERSION \
  --region    YOUR_REGION \
  --warehouse s3://YOUR_ICEBERG_BUCKET/warehouse \
  --timestamp-mode v3
```

Flags mirror the Python tool: `--bucket`, `--prefix`, `--region`, `--warehouse`
(required), plus `--namespace` (default `questdb`), `--catalog-db` (default
`iceberg_catalog.db`, a local SQLite file; accepts a full `jdbc:` URI too),
`--ts-col` (default `timestamp`), `--profile`, `--sample-rows`, `--rebuild`, and
the Java-only `--timestamp-mode` (`v2` default, or `v3`).

The table name is inferred from the prefix (`market_data~699` → `market_data`),
prefixed by `--namespace`. Re-runs are incremental: only files not already in the
table are registered.

## AWS authentication

Credentials come from the AWS SDK v2 **default provider chain** (env vars, shared
config, SSO, instance/role). `--profile NAME` sets the AWS profile so the chain
(including SSO) uses it; run `aws sso login --profile NAME` first. To authenticate
differently, change `applyProfile`/the S3 client setup - that is the only
AWS-specific boundary, mirroring the Python `get_aws_credentials`.

## Timestamp mode: v2 vs v3

QuestDB can store `TIMESTAMP_NS`. Iceberg's nanosecond support is a two-layer story
(see the Python README for the full version detail): the **spec** added
`timestamp_ns`/`timestamptz_ns` in **format-version 3**, and the **Java**
implementation can both write v3 metadata and read/write int64-nanosecond Parquet
natively (Iceberg ≥ 1.10.1; v3 GA in 1.10, hardened in 1.11).

- **`--timestamp-mode v3`** (recommended for ns sources): creates a format-version-3
  table and keeps the column as native `timestamp_ns`. Lossless and zero-copy -
  reads return full nanosecond precision (e.g. `2026-02-10T12:00:00.000296250Z`).
  Caveat: the reader must support Iceberg v3 nanosecond timestamps (recent
  Trino/Spark 4; many engines are still catching up), so only choose v3 once your
  consumers can read it.

- **`--timestamp-mode v2`** (default, broadest compatibility): creates a
  format-version-2 table with microsecond `timestamptz`. This is fully correct and
  zero-copy for sources that are **already microsecond** (e.g. the order-book
  `market_data` table). For an **ns source** it is a compromise: the column is
  labelled microsecond while the Parquet bytes stay nanosecond, so row reads become
  engine-dependent and footer min/max would be written ~1000x too large - the tool
  prints a warning and **drops the ns-timestamp bounds** from the Iceberg manifest
  to avoid corrupting predicate pushdown (the data file is untouched; its own
  Parquet footer stats remain). For ns sources, prefer v3.

**UUID is native in both modes** and needs no v3: QuestDB writes UUID columns as
`fixed_len_byte_array(16)` + the Parquet `UUID` logical type, which maps straight to
Iceberg `uuid` and reads back as a real UUID - no `fixed[16]` fallback.

How the schema is built: `ParquetSchemaUtil.convert` flattens both of these
(it yields `timestamptz`/`fixed[16]`), so the tool inspects the Parquet logical
types itself and overrides the affected columns to `uuid` and (in v3)
`timestamp_ns`. QuestDB files carry no Iceberg field IDs, so the table is created
with a `schema.name-mapping.default` so reads bind by name.

## Register the same data twice (v2 and v3 side by side)

Because registration only writes metadata pointing at the same untouched files, you
can expose one dataset as **two** Iceberg tables for mixed-capability readers - at
zero extra storage. Just run the tool twice with different namespaces and modes:

```bash
# native ns + uuid for v3-capable engines
java -jar .../questdb-to-iceberg.jar --namespace questdb_v3 --timestamp-mode v3 \
  --bucket ... --prefix cold_storage/fx_trades~701 --region ... --warehouse ...

# microsecond, broadly readable
java -jar .../questdb-to-iceberg.jar --namespace questdb_v2 --timestamp-mode v2 \
  --bucket ... --prefix cold_storage/fx_trades~701 --region ... --warehouse ...
```

Both `questdb_v3.fx_trades` and `questdb_v2.fx_trades` point at the identical
Parquet in `cold_storage/`; a reader picks whichever table its engine supports.

## Catalog

Uses Iceberg's `JdbcCatalog` over a local SQLite file - the direct analogue of
PyIceberg's `SqlCatalog`. Swap `--catalog-db` for a `jdbc:postgresql:...` URI, or
change the catalog setup to REST/Glue, without touching the warehouse layout.

## Notes vs the Python version

- No PyIceberg-style `add_files` helper exists in core Java, so the tool assembles
  it from the same primitives: `ParquetUtil.fileMetrics` (with a `NameMapping` for
  the id-less files) + `DataFiles.builder(...).withPartition(...)` +
  `newAppend().commit()`. The `hour(timestamp)` partition value is read from the
  Hive path (`year=/month=/day=/hour=`).
- The shaded jar bundles Hadoop + the AWS SDK (it is large, ~150 MB) because
  parquet-hadoop's footer reader and `S3FileIO` pull them in. This is runtime
  packaging only; nothing Hadoop-related runs against a cluster.
