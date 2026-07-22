"""Register QuestDB cold-storage Parquet into a DuckLake catalog, zero-copy.

Nothing is copied: ducklake_add_data_files only records references to the
existing Parquet in cold storage; the DuckLake data-path is never written to.
This is the DuckLake counterpart of ../questdb_to_iceberg.py -- the SAME
untouched Parquet, with a DuckLake metadata layer instead of an Iceberg one.
Flags mirror the Iceberg tools; nothing site-specific is hardcoded. --bucket and
--prefix are required, and the table name is inferred from the prefix
(cold_storage/market_data~699 -> market_data), exactly like questdb_to_iceberg.py.

Schema is derived from the Parquet itself. Column names/types come from DuckDB's
own inference (UUID, DOUBLE[][], VARCHAR, ...). Timestamps are declared
TIMESTAMPTZ (microsecond): that is the only type add_data_files can map QuestDB's
columns onto, because QuestDB writes them isAdjustedToUTC=1 and DuckDB has no
nanosecond-with-timezone type. Columns that are genuinely nanosecond in the
Parquet are flagged in the output -- DuckLake surfaces them at microseconds,
while Iceberg v3 keeps native nanoseconds. The Parquet on S3 is untouched either
way, so the ns detail is never lost from storage, only from DuckDB's view.

Runs are incremental: it lists the Parquet under the prefix now and registers only
files not already in the catalog (add_data_files is not idempotent, so re-adding
would double-count). --prune also reconciles drops: if registered files have
vanished from S3 (QuestDB dropped those cold-storage partitions), it re-registers
the surviving set so stale references don't dangle. Unlike Iceberg's DeleteFiles,
DuckLake has no per-file deregister, so prune re-registers the survivors rather
than surgically removing; it is cheap when nothing was dropped (pure incremental
add) and only pays a re-register when a drop is detected.

AWS auth: this script expects AWS credentials to already be in the environment,
and picks them up via DuckDB's credential_chain. Populate the environment first,
e.g. with the wrapper described in README.md:

    source duck_with_aws_credentials.sh
    python ducklake_register.py --bucket YOUR_BUCKET --prefix cold_storage/YOUR_TABLE~VER
    python ducklake_register.py --bucket ... --prefix ... --prune   # also deregister vanished files
    python ducklake_register.py --bucket ... --prefix ... --fresh   # recreate the catalog file
"""
import argparse
import os

import duckdb


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--bucket", required=True, help="S3 bucket holding the cold storage")
    p.add_argument("--prefix", required=True,
                   help="prefix to the table dir, e.g. cold_storage/market_data~699")
    p.add_argument("--region", default=None, help="AWS region for S3 (else AWS_DEFAULT_REGION)")
    p.add_argument("--catalog", default="questdb_lake.ducklake",
                   help="DuckLake catalog file (metadata)")
    p.add_argument("--data-path", default="ducklake_data",
                   help="DuckLake data path (unused for zero-copy registration)")
    p.add_argument("--fresh", action="store_true", help="delete and recreate the catalog file")
    p.add_argument("--prune", action="store_true",
                   help="deregister files no longer in S3 (re-registers the surviving set)")
    p.add_argument("--sample-rows", type=int, default=3)
    return p.parse_args()


def table_name_from_prefix(prefix):
    """Table name from a QuestDB prefix: last path segment, cut at '~<id>'."""
    last = prefix.rstrip("/").rsplit("/", 1)[-1]
    return last.split("~", 1)[0]


def connect(args):
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        raise SystemExit(
            "No AWS credentials in the environment. Run the wrapper first, e.g.\n"
            "    source duck_with_aws_credentials.sh\n"
            "(SSO login + aws configure export-credentials). See README.md."
        )
    con = duckdb.connect()
    con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;")
    region = f", REGION '{args.region}'" if args.region else ""
    con.execute(f"CREATE SECRET s3sec (TYPE s3, PROVIDER credential_chain{region});")
    os.makedirs(args.data_path, exist_ok=True)
    con.execute(f"ATTACH 'ducklake:{args.catalog}' AS lake (DATA_PATH '{args.data_path}');")
    return con


def derive_columns(con, root):
    """Return (one_file, [(name, duckdb_type), ...], ns_cols).

    Types come from DuckDB's own inference (UUID, DOUBLE[][], VARCHAR, ...). The
    timestamp columns come back as microsecond TIMESTAMP WITH TIME ZONE: DuckDB
    has no nanosecond-with-timezone type, and QuestDB writes isAdjustedToUTC=1,
    so this is the only type add_data_files can map the files onto. ns_cols flags
    which of those are really nanosecond in the Parquet, so we can warn that the
    DuckLake view drops sub-microsecond detail (Iceberg v3 keeps it).
    """
    one = con.execute(f"SELECT file FROM glob('{root}/**/*.parquet') LIMIT 1;").fetchone()[0]

    ns_cols = {
        name
        for name, _type, logical in con.execute("SELECT name, type, logical_type FROM parquet_schema(?);", [one]).fetchall()
        if logical and "NANOS=NanoSeconds()" in logical
    }
    cols = [
        (name, dtype)
        for name, dtype, *_ in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{one}', hive_partitioning=false);"
        ).fetchall()
    ]
    return one, cols, ns_cols


def table_exists(con, table):
    return con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ?;",
        [table],
    ).fetchone()[0] > 0


def registered_files(con, table):
    return {r[0] for r in con.execute(
        "SELECT data_file FROM ducklake_list_files('lake', ?);", [table]).fetchall()}


def s3_files(con, root):
    return [r[0] for r in con.execute(
        f"SELECT file FROM glob('{root}/**/*.parquet') ORDER BY file;").fetchall()]


def create_and_add_all(con, table, root):
    """Declare the table from the Parquet schema and register every current file."""
    _one, cols, ns_cols = derive_columns(con, root)
    ddl_cols = ", ".join(f'"{n}" {t}' for n, t in cols)
    print("schema declared up front:")
    for n, t in cols:
        flag = "   << ns in Parquet -> stored as us here (Iceberg v3 keeps ns)" if n in ns_cols else ""
        print(f"    {n}: {t}{flag}")
    con.execute(f'DROP TABLE IF EXISTS lake."{table}";')
    con.execute(f'CREATE TABLE lake."{table}" ({ddl_cols});')
    con.execute("CALL ducklake_add_data_files('lake', ?, ?);", [table, f"{root}/**/*.parquet"])


def register(con, table, root, sample_rows, prune):
    print(f"\n===== {table} =====")
    current = s3_files(con, root)
    current_set = set(current)

    if not table_exists(con, table):
        create_and_add_all(con, table, root)
        print(f"registered {len(current)} files zero-copy (new table)")
    else:
        registered = registered_files(con, table)
        to_add = [f for f in current if f not in registered]
        to_remove = [f for f in registered if f not in current_set]
        if prune and to_remove:
            # DuckLake has no per-file deregister (unlike Iceberg's DeleteFiles), so drop
            # the stale references by re-registering the surviving S3 set.
            create_and_add_all(con, table, root)
            print(f"prune: re-registered {len(current)} files "
                  f"(+{len(to_add)} new, -{len(to_remove)} no longer in S3)")
        else:
            for f in to_add:
                con.execute("CALL ducklake_add_data_files('lake', ?, ?);", [table, f])
            msg = f"added {len(to_add)} new files (incremental)"
            if to_remove:
                msg += (f"; {len(to_remove)} registered files no longer in S3 "
                        "-- run with --prune to deregister")
            print(msg)

    count = con.execute(f'SELECT count(*) FROM lake."{table}";').fetchone()[0]
    print(f"total rows: {count:,}")
    if sample_rows > 0:
        print(f"sample {sample_rows} rows through DuckLake:")
        df = con.execute(f'SELECT * FROM lake."{table}" LIMIT {sample_rows};').df()
        print(df.to_string(index=False))


def main():
    args = parse_args()
    if args.fresh:
        for f in (args.catalog, args.catalog + ".wal"):
            if os.path.exists(f):
                os.remove(f)
    con = connect(args)
    root = f"s3://{args.bucket.strip('/')}/{args.prefix.strip('/')}"
    table = table_name_from_prefix(args.prefix)
    register(con, table, root, args.sample_rows, args.prune)
    print("\ntables now in the DuckLake catalog:")
    for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' ORDER BY 1;").fetchall():
        print("  lake." + r[0])


if __name__ == "__main__":
    main()
