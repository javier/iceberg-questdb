# iceberg-questdb

Register QuestDB's S3 cold-storage Parquet as an [Apache Iceberg](https://iceberg.apache.org/)
table **without copying or rewriting any data** (zero-copy). Iceberg metadata is
written to point at the Parquet files QuestDB already exported, so the same data
becomes queryable across the Iceberg ecosystem (Spark, Trino, Athena, DuckDB,
PyIceberg, …) while it stays exactly where QuestDB put it.

The table is partitioned by `hour(timestamp)`, and re-runs are incremental, so it
keeps up as QuestDB writes new hourly partitions to cold storage.

## Implementations

- **Python** — see [`python/`](python/) ([python/README.md](python/README.md))
  for setup, usage, and the QuestDB ↔ PyIceberg compatibility notes.
