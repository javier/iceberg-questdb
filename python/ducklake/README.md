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

Use **DuckDB 1.5 or newer** (Python 3.10+). The register script switches off
DuckLake compaction on the catalog with the `auto_compact` option, which exists
from DuckLake 0.4 / DuckDB 1.5 onwards; see
[Keeping the source Parquet read-only](#keeping-the-source-parquet-read-only).
On an older build the script still runs and simply skips that step with a note,
but the catalog it writes with 1.5+ is DuckLake format version 1.0.

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

### How long registration takes, and recovering from an interrupted run

`ducklake_add_data_files` reads **every Parquet footer over S3** to record
per-file row counts and column stats. Budget roughly **1 second per file**: a
few hundred partitions is a few **minutes**, and the script prints nothing until
a table finishes, so a first-time register looks like it hangs when it is just
grinding through footer reads. This is the DuckLake counterpart to the metrics
scan the Iceberg tools do. An Iceberg `--prune` that finds nothing new is instant
only because it adds zero files; a from-scratch DuckLake register of the same
table pays the full footer-read cost.

Incremental runs are fast: a table that is already populated skips every file
already in the catalog (`add_data_files` is not idempotent, so re-adding would
double-count) and only reads footers for genuinely new files.

**You cannot query the catalog while a register (or `--prune`) is running.** The
local `.ducklake` catalog is a single embedded DuckDB file: one writer, or many
readers, but not both. A `ducklake_query.py` run started while a registration
holds the write lock fails with `Could not set lock on file ... Conflicting lock
is held`. Wait for the registration to finish, then query. For the same reason,
register one table at a time into a given catalog rather than in parallel.

**If a run is interrupted** (Ctrl-C, or the session is killed) part-way through a
table, you can be left with the table **declared but with zero or partial files
registered**, plus a dangling `questdb_lake.ducklake.wal` next to the catalog.
A later plain run then sees the table already exists and tries to add the missing
files **one at a time** (the slow per-file path), which is even slower than the
first run. The clean recovery is to rebuild from scratch:

```bash
python ducklake_register.py --bucket ... --prefix ... --fresh
```

`--fresh` deletes the catalog file (and its `.wal`) and re-registers every
current file through the single fast glob path. When registering more than one
table into the same catalog, pass `--fresh` on the **first** table only;
subsequent tables run without it so they add alongside rather than wiping it.

## Keeping the source Parquet read-only

When you register a file, DuckLake records it as one of its own data files.
Registering and querying only ever **read** the Parquet, so in normal use nothing
here writes to cold storage. The thing to know is that DuckLake also has optional
*maintenance* operations, compaction (`merge_adjacent_files`, `rewrite_data_files`)
and file cleanup (`expire_snapshots` + `cleanup_old_files`, also bundled into a
single `CHECKPOINT`), which are allowed to rewrite or delete the data files
DuckLake owns. You simply want QuestDB's files left exactly as it wrote them.

The register script already handles this: it sets `auto_compact = false` on the
catalog (DuckDB 1.5+/DuckLake 0.4+), globally and persisted, which turns off every
rewrite path (`merge_adjacent_files`, `rewrite_data_files`, `flush_inlined_data`,
`delete_orphaned_files`) for all tables in the catalog. With compaction off your
live partitions are never rewritten, and because the delete path
(`cleanup_old_files`) only removes files no longer referenced by the table, it
never has a live file to delete. `--prune` and `--fresh` only ever drop references
to partitions QuestDB has *already* removed from S3, so they never point the delete
path at a file that still exists. As used, nothing here touches the Parquet.

This was validated on real files: with `auto_compact = false`, an explicit
`merge_adjacent_files` and a `CHECKPOINT`, each followed by `expire_snapshots` +
`cleanup_old_files`, all left the source files byte-for-byte unchanged; with the
default `auto_compact = true`, the same sequence rewrote and then deleted them.

That makes the tooling safe on its own. As an **optional extra precaution** for
access that does *not* go through these scripts (a manual `CHECKPOINT`, an older
client that cannot set `auto_compact`, a future auto-maintenance default, a bug),
you can also make it structural at the bucket: give the DuckLake credentials
read-only access to the cold-storage bucket (`s3:GetObject` and `s3:ListBucket`,
without `s3:PutObject` / `s3:DeleteObject`). Then QuestDB is the only writer and
every other client, DuckLake included, can only list and read. The same posture
suits the Iceberg path over the same Parquet.

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
