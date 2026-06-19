"""Minimal, table-agnostic Iceberg reader. Lists tables or samples rows.

Registers nothing - it only points PyIceberg at an existing catalog and reads.
Two modes:

    # list every namespace.table in the catalog
    python iceberg_reader.py --catalog-db sqlite:///iceberg_catalog.db \
        --region eu-west-1 --list

    # print schema + a few rows from one table
    python iceberg_reader.py --catalog-db sqlite:///iceberg_catalog.db \
        --region eu-west-1 --table questdb.fx_trades --sample-rows 5

Works against catalogs written by either implementation, including v3 tables
created by the Java version (PyIceberg can read v3 even though it cannot write it).
AWS auth mirrors questdb_to_iceberg.py: replace get_aws_credentials() to taste.
"""
import argparse
import subprocess
from collections import namedtuple

import boto3
from pyiceberg.catalog.sql import SqlCatalog

AwsCredentials = namedtuple("AwsCredentials", ["access_key", "secret_key", "token"])


def get_aws_credentials(profile, sso_profile=None):
    """Return AwsCredentials for S3 access (AWS SSO by default; swap as needed)."""
    check = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--profile", profile], capture_output=True
    )
    if check.returncode != 0:
        print("SSO session expired or not logged in. Logging in...")
        subprocess.run(["aws", "sso", "login", "--profile", sso_profile or profile], check=True)
    c = boto3.Session(profile_name=profile).get_credentials().get_frozen_credentials()
    return AwsCredentials(c.access_key, c.secret_key, c.token)


def catalog_s3_props(creds, region):
    props = {
        "s3.access-key-id": creds.access_key,
        "s3.secret-access-key": creds.secret_key,
        "s3.region": region,
    }
    if creds.token:
        props["s3.session-token"] = creds.token
    return props


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--catalog-db", default="sqlite:///iceberg_catalog.db", help="SqlCatalog metadata DB URI")
    p.add_argument("--warehouse", default=None, help="warehouse URI (optional; table metadata paths are absolute)")
    p.add_argument("--region", required=True, help="AWS region of the warehouse/data buckets")
    p.add_argument("--profile", default="default", help="AWS profile name")
    p.add_argument("--sso-profile", default=None, help="AWS SSO profile for `aws sso login`")
    p.add_argument("--list", action="store_true", help="list all namespace.table identifiers")
    p.add_argument("--table", default=None, help="identifier to sample, e.g. questdb.fx_trades")
    p.add_argument("--table-details", default=None, help="identifier to describe: location, schema, partitions")
    p.add_argument("--sample-rows", type=int, default=5, help="rows to pull when --table is given")
    return p.parse_args()


def main():
    args = parse_args()
    if not args.list and not args.table and not args.table_details:
        raise SystemExit("nothing to do: pass --list, --table NS.NAME, or --table-details NS.NAME")

    creds = get_aws_credentials(args.profile, args.sso_profile)
    props = {"uri": args.catalog_db, **catalog_s3_props(creds, args.region)}
    if args.warehouse:
        props["warehouse"] = args.warehouse
    catalog = SqlCatalog("iceberg", **props)

    if args.list:
        print("tables:")
        for ns in catalog.list_namespaces():
            for ident in catalog.list_tables(ns):
                print("  " + ".".join(ident))

    if args.table_details:
        tbl = catalog.load_table(tuple(args.table_details.split(".")))
        print(f"\n--- {args.table_details} ---")
        print("format-version:", tbl.metadata.format_version)
        print("location:", tbl.location())
        print("metadata-location:", tbl.metadata_location)
        print("\nschema:")
        print(tbl.schema())
        print("\npartition spec:")
        print(tbl.spec())
        parts = tbl.inspect.partitions()
        keep = [c for c in ["partition", "record_count", "file_count",
                            "total_data_file_size_in_bytes", "last_updated_at"]
                if c in parts.column_names]
        print(f"\nregistered partitions ({parts.num_rows}):")
        print(parts.select(keep) if keep else parts)

    if args.table:
        ident = tuple(args.table.split("."))
        tbl = catalog.load_table(ident)
        print(f"\n--- {args.table} ---")
        print("format-version:", tbl.metadata.format_version)
        print("schema:")
        print(tbl.schema())
        if args.sample_rows > 0:
            print(f"\n--- {args.sample_rows} sample rows ---")
            print(tbl.scan(limit=args.sample_rows).to_arrow())


if __name__ == "__main__":
    main()
