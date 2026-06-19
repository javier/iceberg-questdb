# iceberg-questdb

Register QuestDB's S3 cold-storage Parquet as an [Apache Iceberg](https://iceberg.apache.org/)
table **without copying or rewriting any data** (zero-copy). Iceberg metadata is
written to point at the Parquet files QuestDB already exported, so the same data
becomes queryable across the Iceberg ecosystem (Spark, Trino, Athena, DuckDB,
PyIceberg, …) while it stays exactly where QuestDB put it.

The table is partitioned by `hour(timestamp)`, and re-runs are incremental, so it
keeps up as QuestDB writes new hourly partitions to cold storage.

## Choosing a version

There are two implementations, Python and Java. They do the same zero-copy
registration; the difference is data-type fidelity:

- For **microsecond** timestamps, either version works.
- For **nanosecond** timestamps (Iceberg v3 `timestamp_ns`), use **Java**. Iceberg
  added nanoseconds in format-version 3, and only the Java reference implementation
  can currently write v3 — PyIceberg cannot, so the Python version downcasts ns to
  microseconds. If you need lossless nanoseconds, Java is the one that works.
- UUIDs are native in **Java** (Iceberg `uuid`); Python registers them as
  `fixed[16]`.

## Implementations

- **Python** — see [`python/`](python/) ([python/README.md](python/README.md))
  for setup, usage, and the QuestDB ↔ PyIceberg compatibility notes. Downcasts
  nanosecond timestamps to microseconds and registers UUIDs as `fixed[16]`.
- **Java** — see [`java/`](java/) ([java/README.md](java/README.md)). The Iceberg
  reference implementation, so it keeps **native nanosecond timestamps** (v3) and
  **native UUIDs**. Choose `--timestamp-mode v2` (broad compatibility) or `v3`
  (lossless ns); the same data can be registered as both at once for
  mixed-capability readers.
