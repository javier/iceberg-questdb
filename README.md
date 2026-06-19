# iceberg-questdb

> [!NOTE]
> This repository is an **example of manual QuestDB to Apache Iceberg
> integration**, not a production-hardened tool. For full context and the
> recommended workflow, see the QuestDB guide:
> https://questdb.com/docs/tutorials/questdb-to-iceberg/

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
- For **nanosecond** timestamps (Iceberg v3 `timestamp_ns`), **register with Java**.
  Iceberg added nanoseconds in format-version 3, and only the Java reference
  implementation can currently *write* v3 — PyIceberg cannot, so the Python
  *registration* tool downcasts ns to microseconds.
- UUIDs are native in **Java** (Iceberg `uuid`); the Python registration tool
  records them as `fixed[16]`.

**Reading is not limited the same way.** PyIceberg's v3 gap is write-only: it
*reads* a Java-created v3 table with full fidelity — `timestamp_ns` comes back as
nanosecond-precise timestamps and `uuid` as a real UUID type. So a good pattern is
**register once with Java (v3), then query from anywhere**, Python included. The
minimal read-only helper `python/iceberg_reader.py` (`--list` / `--table`) does
exactly that against any catalog, registering nothing.

## Implementations

- **Python** — see [`python/`](python/) ([python/README.md](python/README.md))
  for setup, usage, and the QuestDB ↔ PyIceberg compatibility notes. Downcasts
  nanosecond timestamps to microseconds and registers UUIDs as `fixed[16]`. The
  catalog is pluggable (SQLite by default, or REST/Glue/Hive) via
  `--catalog-type`/`--catalog-prop`, and there's a read-only
  [`iceberg_reader.py`](python/iceberg_reader.py) (`--list` / `--table` /
  `--table-details`) for inspecting any catalog.
- **Java** — see [`java/`](java/) ([java/README.md](java/README.md)). The Iceberg
  reference implementation, so it keeps **native nanosecond timestamps** (v3) and
  **native UUIDs**. Choose `--timestamp-mode v2` (broad compatibility) or `v3`
  (lossless ns); the same data can be registered as both at once for
  mixed-capability readers.
