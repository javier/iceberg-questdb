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

QuestDB's cold-storage Parquet is Iceberg-compatible out of the box: canonically
named list elements, no embedded Parquet field IDs, and min/max column statistics
(so add_files infers the hour(timestamp) partition straight from the footer). No
monkeypatching of PyIceberg is needed for that.

The one adaptation is nanosecond timestamps. QuestDB can store TIMESTAMP_NS, but
Iceberg has no nanosecond type until spec v3, which this PyIceberg release cannot
*write* yet (apache/iceberg-python#1551). So when the source has ns timestamps we
downcast them to microseconds: the Iceberg column becomes timestamptz(us) while
the underlying Parquet stays untouched (still zero-copy), and reads downcast on
the fly. The only cost is sub-microsecond precision. PyIceberg's add_files does
not thread its downcast-on-write flag into the schema-compatibility check, so we
force it there via the small `ns_downcast_compat()` context manager, scoped to the
add_files call.
"""
import argparse
import logging
import os
import subprocess
from collections import namedtuple
from contextlib import contextmanager, nullcontext

import boto3
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pyiceberg.io.pyarrow as _pyi
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import HourTransform


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


def schema_has_ns_timestamp(schema):
    """True if any column is a nanosecond timestamp (Iceberg has no ns type)."""
    return any(pa.types.is_timestamp(f.type) and f.type.unit == "ns" for f in schema)


@contextmanager
def ns_downcast_compat():
    """Make add_files accept ns timestamps by downcasting them to us.

    create_table honours PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE on its
    own, but add_files' schema-compatibility check ignores that flag, so we wrap
    it to force the downcast. Scoped to the `with` block, original restored on
    exit. No-op effect on microsecond tables.
    """
    orig = _pyi._check_pyarrow_schema_compatible

    def patched(requested_schema, provided_schema, downcast_ns_timestamp_to_us=False, format_version=2):
        return orig(requested_schema, provided_schema, downcast_ns_timestamp_to_us=True, format_version=format_version)

    _pyi._check_pyarrow_schema_compatible = patched
    try:
        yield
    finally:
        _pyi._check_pyarrow_schema_compatible = orig


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


def register_new_files(tbl, files, downcast_ns):
    """Zero-copy add only the files not already registered. Returns the count."""
    registered = {task.file.file_path for task in tbl.scan().plan_files()}
    new_files = [f for f in files if f not in registered]
    print(f"{len(registered)} files already registered, {len(new_files)} new")
    if new_files:
        with (ns_downcast_compat() if downcast_ns else nullcontext()):
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

    # Read one file's schema up front: needed to create the table, and to detect
    # nanosecond timestamps (which Iceberg cannot represent and must downcast).
    pa_s3 = pyarrow_s3(creds, args.region)
    with pa_s3.open_input_file(files[0].replace("s3://", "")) as f:
        file_schema = pq.read_schema(f)

    downcast_ns = schema_has_ns_timestamp(file_schema)
    if downcast_ns:
        os.environ["PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE"] = "true"
        # PyIceberg logs the downcast as a warning for every ns value it touches
        # (per file on register, per batch on read). We downcast on purpose, so
        # mute that one logger and print a single note instead.
        logging.getLogger("pyiceberg.io.pyarrow").setLevel(logging.ERROR)
        print("note: source has nanosecond timestamps; Iceberg has no ns type here, "
              "downcasting to microseconds (sub-us precision is dropped, data stays in place)")

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
        added = register_new_files(tbl, files, downcast_ns)
        print("nothing to do; table is up to date" if not added else f"registered {added} new files")
    else:
        # first run: derive schema from a file, create the table, register all
        print("\n--- file schema ---")
        print(file_schema)

        tbl = catalog.create_table(identifier, schema=file_schema)
        with tbl.update_spec() as us:
            us.add_field(args.ts_col, HourTransform())
        with (ns_downcast_compat() if downcast_ns else nullcontext()):
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
