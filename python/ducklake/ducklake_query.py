"""Read a DuckLake catalog written by ducklake_register.py: list tables, describe
one, sample rows, or filter to a single UTC day. The DuckLake counterpart of
../iceberg_reader.py.

AWS auth: creds must be in the environment (source duck_with_aws_credentials.sh);
DuckDB reads them via credential_chain. --list and --table-details are metadata
only (no S3); --sample-rows and --day read the Parquet from cold storage.

    source duck_with_aws_credentials.sh
    python ducklake_query.py --list
    python ducklake_query.py --table-details trades
    python ducklake_query.py --table trades --sample-rows 5
    python ducklake_query.py --table fx_trades --day 2026-06-19 --sample-rows 5
"""
import argparse
import datetime
import os

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--catalog", default=os.path.join(HERE, "questdb_lake.ducklake"),
                   help="DuckLake catalog file to read")
    p.add_argument("--data-path", default=os.path.join(HERE, "ducklake_data"))
    p.add_argument("--region", default="eu-west-1")
    p.add_argument("--list", action="store_true", help="list tables in the catalog")
    p.add_argument("--table-details", metavar="NAME", help="describe one table: schema + row count")
    p.add_argument("--table", metavar="NAME", help="table to sample / filter")
    p.add_argument("--sample-rows", type=int, default=5)
    p.add_argument("--day", metavar="YYYY-MM-DD", help="filter --table to one UTC day")
    p.add_argument("--ts-col", default="timestamp", help="timestamp column used by --day")
    return p.parse_args()


def connect(args):
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    if "AWS_ACCESS_KEY_ID" in os.environ:  # creds present -> wire up S3 for row reads
        con.execute("INSTALL aws; LOAD aws; INSTALL httpfs; LOAD httpfs;")
        con.execute(f"CREATE SECRET s3sec (TYPE s3, PROVIDER credential_chain, REGION '{args.region}');")
    con.execute(f"ATTACH 'ducklake:{args.catalog}' AS lake (DATA_PATH '{args.data_path}');")
    return con


def require_s3():
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        raise SystemExit("Reading rows needs AWS creds; source duck_with_aws_credentials.sh first.")


def day_predicate(ts_col, day):
    lo = f"{day} 00:00:00+00"
    hi = (datetime.date.fromisoformat(day) + datetime.timedelta(days=1)).isoformat() + " 00:00:00+00"
    return f' WHERE "{ts_col}" >= TIMESTAMPTZ \'{lo}\' AND "{ts_col}" < TIMESTAMPTZ \'{hi}\''


def main():
    args = parse_args()
    con = connect(args)

    if args.list:
        print(f"tables in {args.catalog}:")
        for (t,) in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' ORDER BY 1;"
        ).fetchall():
            print("  lake." + t)

    if args.table_details:
        t = args.table_details
        print(f"\n--- lake.{t} ---")
        print("schema:")
        for name, dtype, *_ in con.execute(f'DESCRIBE lake."{t}";').fetchall():
            print(f"    {name}: {dtype}")
        count = con.execute(f'SELECT count(*) FROM lake."{t}";').fetchone()[0]
        print(f"rows: {count:,}")

    if args.table:
        require_s3()
        t = args.table
        where = day_predicate(args.ts_col, args.day) if args.day else ""
        if args.day:
            n = con.execute(f'SELECT count(*) FROM lake."{t}"{where};').fetchone()[0]
            print(f"\nlake.{t} on {args.day} (UTC): {n:,} rows")
        if args.sample_rows > 0:
            print(f"\n--- lake.{t} sample {args.sample_rows} rows ---")
            df = con.execute(f'SELECT * FROM lake."{t}"{where} LIMIT {args.sample_rows};').df()
            print(df.to_string(index=False))


if __name__ == "__main__":
    main()
