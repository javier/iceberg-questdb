"""Zero-copy register QuestDB cold-storage Parquet as an Iceberg table.

PyIceberg `add_files` registers the existing S3 Parquet in place (no rewrite),
partitioned by hour(timestamp). Bucket, prefix, region, warehouse and auth are all
required CLI params (nothing site-specific is hardcoded, and the warehouse is
never assumed to live in the data bucket), so it works against any QuestDB
cold-storage layout. The Iceberg table name is taken from the prefix.

Runs are incremental by default: the first run creates the table and registers
every file; later runs register only files not already in the table (so you can
re-run after QuestDB writes a new hourly partition). Use --rebuild to drop and
re-register from scratch. Dropping never touches the S3 data, only the catalog.

Two QuestDB-specific quirks bite PyIceberg's `add_files` path *only*; both are
handled by `questdb_add_files_compat()` and scoped to the `add_files` call:

  1. Nested list<list<double>> columns (the order book) serialize with their
     element nodes named 'list' rather than the canonical 'element', so the leaf
     path is 'bids.list.list.list.element'. `add_files` builds a name-based
     path->field-id map keyed on 'element', so that lookup misses. This is a
     PyIceberg `add_files` limitation, not a QuestDB defect: the files are valid
     Parquet and read correctly in Athena, DuckDB, Spark and PyIceberg's own
     reader, none of which depend on the element node name.

  2. QuestDB writes no min/max column statistics, so `add_files` cannot infer the
     hour(timestamp) partition value from the footer and emits a null partition,
     which then crashes the Avro manifest writer. The data is already
     Hive-partitioned to the hour in the S3 path and each file holds exactly one
     hour, so we read the partition value from the path instead.
"""
import argparse
import re
import subprocess
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime, timezone

import boto3
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pyiceberg.io.pyarrow as _pyi
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import HourTransform
from pyiceberg.typedef import Record


# ===========================================================================
# AWS credentials: the only AWS-specific boundary in this script.
# Replace the body of get_aws_credentials() to match how you authenticate.
# Everything downstream just consumes the returned AwsCredentials.
# ===========================================================================
AwsCredentials = namedtuple("AwsCredentials", ["access_key", "secret_key", "token"])


def get_aws_credentials(profile, sso_profile=None):
    """Return AwsCredentials for S3 access.

    This PoC uses an AWS SSO profile: it refreshes the SSO session if expired,
    then hands back frozen credentials. If you authenticate differently, replace
    the body, for example:

        # 1) static access keys
        return AwsCredentials("AKIA...", "secret...", None)

        # 2) environment vars / shared config / instance/role profile
        c = boto3.Session().get_credentials().get_frozen_credentials()
        return AwsCredentials(c.access_key, c.secret_key, c.token)

    token may be None when you are not using temporary (STS) credentials.
    """
    check = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--profile", profile],
        capture_output=True,
    )
    if check.returncode != 0:
        print("SSO session expired or not logged in. Logging in...")
        subprocess.run(["aws", "sso", "login", "--profile", sso_profile or profile], check=True)

    c = boto3.Session(profile_name=profile).get_credentials().get_frozen_credentials()
    return AwsCredentials(c.access_key, c.secret_key, c.token)


def s3_client(creds, region):
    """boto3 S3 client built from the returned credentials (for listing)."""
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
    )


def pyarrow_s3(creds, region):
    """pyarrow S3 filesystem built from the returned credentials (for reads)."""
    return pafs.S3FileSystem(
        access_key=creds.access_key,
        secret_key=creds.secret_key,
        session_token=creds.token,
        region=region,
    )


def catalog_s3_props(creds, region):
    """PyIceberg SqlCatalog S3 properties built from the returned credentials."""
    props = {
        "s3.access-key-id": creds.access_key,
        "s3.secret-access-key": creds.secret_key,
        "s3.region": region,
    }
    if creds.token:  # omit for static (non-STS) keys
        props["s3.session-token"] = creds.token
    return props


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_HIVE_HOUR = re.compile(r"year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)")


def hour_partition_value(file_path):
    """Iceberg hour(timestamp) value (hours since epoch) read from a Hive path."""
    m = _HIVE_HOUR.search(file_path)
    if not m:
        raise ValueError(f"no year=/month=/day=/hour= partition in {file_path}")
    y, mo, d, h = (int(x) for x in m.groups())
    dt = datetime(y, mo, d, h, tzinfo=timezone.utc)
    return int((dt - _EPOCH).total_seconds()) // 3600


@contextmanager
def questdb_add_files_compat():
    """Patch PyIceberg's add_files internals for QuestDB Parquet, then restore.

    Scoped to the `with` block so we never leave PyIceberg globals mutated for
    the rest of the process. See the module docstring for the why.
    """
    orig_mapping = _pyi.parquet_path_to_id_mapping
    orig_p2d = _pyi.parquet_file_to_data_file

    def tolerant_mapping(schema):
        # Route a nested-list leaf ('bids.list.list.list.element') to its
        # top-level column's field id. There is one leaf under each order-book
        # column, so this resolves to the correct element field id; Iceberg
        # drops min/max for nested types anyway and keeps only counts.
        base = orig_mapping(schema)

        class _M(dict):
            def __missing__(self, key):
                top = key.split(".", 1)[0]
                for path, fid in base.items():
                    if path == top or path.startswith(top + "."):
                        return fid
                raise KeyError(key)

        return _M(base)

    def data_file_with_path_partition(io, table_metadata, file_path):
        data_file = orig_p2d(io, table_metadata, file_path)
        data_file[3] = Record(hour_partition_value(file_path))  # _data[3] = partition
        return data_file

    _pyi.parquet_path_to_id_mapping = tolerant_mapping
    _pyi.parquet_file_to_data_file = data_file_with_path_partition
    try:
        yield
    finally:
        _pyi.parquet_path_to_id_mapping = orig_mapping
        _pyi.parquet_file_to_data_file = orig_p2d


def table_name_from_prefix(prefix):
    """QuestDB cold-storage prefixes end in 'table_name~version'; take the name.

    e.g. 'cold_storage/market_data~699' -> 'market_data'
    """
    last = prefix.rstrip("/").split("/")[-1]
    return last.split("~")[0]


def list_parquet_files(s3, bucket, prefix):
    """Every data.parquet under s3://bucket/prefix, across all partitions."""
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("data.parquet"):
                files.append(f"s3://{bucket}/{obj['Key']}")
    return sorted(files)


def register_new_files(tbl, files):
    """Zero-copy add only the files not already registered. Returns the count."""
    registered = {task.file.file_path for task in tbl.scan().plan_files()}
    new_files = [f for f in files if f not in registered]
    print(f"{len(registered)} files already registered, {len(new_files)} new")
    if new_files:
        with questdb_add_files_compat():
            tbl.add_files(new_files)  # zero-copy register, no rewrite
    return len(new_files)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--bucket", required=True, help="S3 bucket holding the QuestDB cold storage")
    p.add_argument("--prefix", required=True, help="S3 prefix of the QuestDB table, e.g. cold_storage/market_data~699")
    p.add_argument("--region", required=True, help="AWS region of the bucket")
    p.add_argument("--profile", default="default", help="AWS profile name (see get_aws_credentials)")
    p.add_argument("--sso-profile", default=None, help="AWS SSO profile for `aws sso login` (defaults to --profile)")
    p.add_argument("--namespace", default="questdb", help="Iceberg namespace; table name is inferred from the QuestDB prefix (e.g. questdb.market_data)")
    p.add_argument("--warehouse", required=True, help="Iceberg warehouse URI for table metadata, e.g. s3://my-iceberg-bucket/warehouse (keep separate from the data bucket)")
    p.add_argument("--catalog-db", default="sqlite:///iceberg_catalog.db", help="SqlCatalog metadata DB URI")
    p.add_argument("--ts-col", default="timestamp", help="designated timestamp column to partition by hour()")
    p.add_argument("--rebuild", action="store_true", help="drop and re-register from scratch (default: incremental)")
    p.add_argument("--sample-rows", type=int, default=5, help="rows to pull for the read-back check (0 to skip)")
    return p.parse_args()


def main():
    args = parse_args()
    identifier = f"{args.namespace}.{table_name_from_prefix(args.prefix)}"

    creds = get_aws_credentials(args.profile, args.sso_profile)

    files = list_parquet_files(s3_client(creds, args.region), args.bucket, args.prefix)
    print(f"Found {len(files)} parquet files under s3://{args.bucket}/{args.prefix}")
    if not files:
        raise SystemExit("No files matched. Check --bucket/--prefix.")

    catalog = SqlCatalog(
        "iceberg",
        uri=args.catalog_db,
        warehouse=args.warehouse,
        **catalog_s3_props(creds, args.region),
    )
    catalog.create_namespace_if_not_exists(args.namespace)

    if args.rebuild and catalog.table_exists(identifier):
        print("--rebuild: dropping existing table (S3 data left intact)")
        catalog.drop_table(identifier)  # drop, not purge

    if catalog.table_exists(identifier):
        # incremental: add only the partitions that appeared since last run
        tbl = catalog.load_table(identifier)
        added = register_new_files(tbl, files)
        print("nothing to do; table is up to date" if not added else f"registered {added} new files")
    else:
        # first run: derive schema from a file, create the table, register all
        pa_s3 = pyarrow_s3(creds, args.region)
        with pa_s3.open_input_file(files[0].replace("s3://", "")) as f:
            schema = pq.read_schema(f)
        print("\n--- file schema ---")
        print(schema)

        tbl = catalog.create_table(identifier, schema=schema)
        with tbl.update_spec() as us:
            us.add_field(args.ts_col, HourTransform())
        with questdb_add_files_compat():
            tbl.add_files(files)  # zero-copy register, no rewrite
        print(f"created {identifier} and registered {len(files)} files")

    # --- validation, cheap until the sample-row pull ---
    print("\n--- iceberg table ---")
    print("schema:")
    print(tbl.schema())
    print("\npartition spec:")
    print(tbl.spec())

    tasks = list(tbl.scan().plan_files())
    total_rows = sum(t.file.record_count for t in tasks)
    print(f"\nregistered files: {len(tasks)}")
    print(f"total rows (from footer stats, no scan): {total_rows:,}")
    print("sample registered file paths:")
    for t in tasks[:3]:
        print(f"  {t.file.file_path}")

    if args.sample_rows > 0:
        print(f"\n--- {args.sample_rows} sample rows ---")
        sample = tbl.scan(limit=args.sample_rows).to_arrow()
        print(sample)
        for row in sample.to_pylist():
            print(row)


if __name__ == "__main__":
    main()
