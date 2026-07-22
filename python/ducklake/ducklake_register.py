"""Register QuestDB cold-storage Parquet into a DuckLake catalog, zero-copy.

Nothing is copied: ducklake_add_data_files only records references to the
existing Parquet in cold storage; the DuckLake data-path is never written to.
This is the DuckLake counterpart of ../questdb_to_iceberg.py -- the SAME
untouched Parquet, with a DuckLake metadata layer instead of an Iceberg one.

Schema is derived from the Parquet itself. Column names/types come from DuckDB's
own inference (UUID, DOUBLE[][], VARCHAR, ...). Timestamps are declared
TIMESTAMPTZ (microsecond): that is the only type add_data_files can map QuestDB's
columns onto, because QuestDB writes them isAdjustedToUTC=1 and DuckDB has no
nanosecond-with-timezone type. Columns that are genuinely nanosecond in the
Parquet are flagged in the output -- DuckLake surfaces them at microseconds,
while Iceberg v3 keeps native nanoseconds. The Parquet on S3 is untouched either
way, so the ns detail is never lost from storage, only from DuckDB's view.

AWS auth: this script expects AWS credentials to already be in the environment,
and picks them up via DuckDB's credential_chain. Populate the environment first,
e.g. with the wrapper described in README.md:

    source duck_with_aws_credentials.sh   # SSO login + aws configure export-credentials
    python ducklake_register.py           # all known tables
    python ducklake_register.py --table trades
    python ducklake_register.py --fresh   # recreate the catalog file
"""
import argparse
import os

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))

# QuestDB cold-storage prefixes (the same untouched Parquet Iceberg points at).
# QuestDB suffixes each table dir with its ~<id>, so these are not guessable;
# add a new table's prefix here to register it.
BUCKET = "s3://questdb-javier-demos-cold-storage/cold_storage"
TABLES = {
    "fx_trades": f"{BUCKET}/fx_trades~701",
    "market_data": f"{BUCKET}/market_data~699",
    "trades": f"{BUCKET}/trades~1001",
}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--table", action="append", choices=list(TABLES), metavar="NAME",
                   help="table to register (repeatable); default: all")
    p.add_argument("--catalog", default=os.path.join(HERE, "questdb_lake.ducklake"),
                   help="DuckLake catalog file (metadata)")
    p.add_argument("--data-path", default=os.path.join(HERE, "ducklake_data"),
                   help="DuckLake data path (unused for zero-copy registration)")
    p.add_argument("--region", default="eu-west-1")
    p.add_argument("--fresh", action="store_true", help="delete and recreate the catalog file")
    p.add_argument("--sample-rows", type=int, default=3)
    return p.parse_args()


def connect(args):
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        raise SystemExit(
            "No AWS credentials in the environment. Run the wrapper first, e.g.\n"
            "    source duck_with_aws_credentials.sh\n"
            "(SSO login + aws configure export-credentials). See README.md."
        )
    con = duckdb.connect()
    con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;")
    con.execute(f"CREATE SECRET s3sec (TYPE s3, PROVIDER credential_chain, REGION '{args.region}');")
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


def register(con, table, root, sample_rows):
    one, cols, ns_cols = derive_columns(con, root)
    ddl_cols = ", ".join(f'"{n}" {t}' for n, t in cols)
    print(f"\n===== {table} =====")
    print("schema declared up front:")
    for n, t in cols:
        flag = "   << ns in Parquet -> stored as us here (Iceberg v3 keeps ns)" if n in ns_cols else ""
        print(f"    {n}: {t}{flag}")

    con.execute(f'DROP TABLE IF EXISTS lake."{table}";')
    con.execute(f'CREATE TABLE lake."{table}" ({ddl_cols});')
    con.execute("CALL ducklake_add_data_files('lake', ?, ?);", [table, f"{root}/**/*.parquet"])

    n_files = con.execute(f"SELECT file FROM glob('{root}/**/*.parquet');").fetchall()
    count = con.execute(f'SELECT count(*) FROM lake."{table}";').fetchone()[0]
    print(f"registered {len(n_files)} files zero-copy -> {count:,} rows")

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
    for table in (args.table or list(TABLES)):
        register(con, table, TABLES[table], args.sample_rows)
    print("\ntables now in the DuckLake catalog:")
    for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' ORDER BY 1;").fetchall():
        print("  lake." + r[0])


if __name__ == "__main__":
    main()
