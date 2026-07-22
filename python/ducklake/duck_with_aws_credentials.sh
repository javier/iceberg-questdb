# Populate the current shell with temporary AWS credentials for the questdb-dev
# account, so DuckDB (the CLI or the Python tools in this folder) can read the
# cold-storage bucket via its credential_chain.
#
# SOURCE it (so the exports land in your shell), do not execute it:
#
#     source duck_with_aws_credentials.sh
#     python ducklake_register.py          # creds are now in the environment
#     duckdb                               # ...or an interactive CLI session
#
PROFILE="${QUESTDB_AWS_PROFILE:-questdb-dev}"
SSO_PROFILE="${QUESTDB_SSO_PROFILE:-sso-main}"

if ! aws sts get-caller-identity --profile "$PROFILE" >/dev/null 2>&1; then
    aws sso login --profile "$SSO_PROFILE"
fi

eval "$(aws configure export-credentials --profile "$PROFILE" --format env)"

aws sts get-caller-identity   # sanity check: expect account 337384507863
