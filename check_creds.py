#!/usr/bin/env python3
"""Validate the S3 credentials in s3-vars.json before deploying them.

Usage:
  python check_creds.py           # identity check only (no writes)
  python check_creds.py --write   # also prove PutObject works (writes 1 tiny object)

Never prints the secret.
"""
import json, sys, pathlib

v = json.loads(pathlib.Path("s3-vars.json").read_text())
kid, sec = v["s3_access_key_id"], v["s3_secret_access_key"]

# --id <KEY_ID> tests a different access key ID against the SAME secret in the
# file, so you can check the old ID without editing s3-vars.json.
if "--id" in sys.argv:
    kid = sys.argv[sys.argv.index("--id") + 1].strip()
    print(f"(overriding access key id from --id)")

for name, val in (("s3_access_key_id", kid), ("s3_secret_access_key", sec)):
    if "PASTE" in val:
        sys.exit(f"ERROR: {name} is still a placeholder — fill in s3-vars.json first.")

print(f"key id   : {kid[:4]}…{kid[-4:]}  (len {len(kid)})")
print(f"secret   : {sec[:2]}…          (len {len(sec)})")
if len(kid) != 20:
    print("  WARNING: AWS access key IDs are normally 20 chars.")
if len(sec) != 40:
    print("  WARNING: AWS secret access keys are normally 40 chars.")

import boto3
from botocore.exceptions import ClientError

kw = dict(aws_access_key_id=kid, aws_secret_access_key=sec, region_name=v["s3_region"])

try:
    who = boto3.client("sts", **kw).get_caller_identity()
    print(f"\nPAIR VALID — arn={who['Arn']}  account={who['Account']}")
except ClientError as e:
    code = e.response["Error"]["Code"]
    hint = {
        "InvalidClientTokenId": "the access key ID is not recognised (wrong/deactivated ID)",
        "SignatureDoesNotMatch": "ID is real but the SECRET does not match it — mismatched pair",
    }.get(code, "")
    sys.exit(f"\nPAIR INVALID — {code}: {hint or e}")

if "--write" not in sys.argv:
    print("\nIdentity confirmed. Re-run with --write to verify PutObject on the bucket.")
    sys.exit(0)

key = "parquet/live/_credcheck/credcheck.txt"
try:
    boto3.client("s3", endpoint_url=v["s3_endpoint"], **kw).put_object(
        Bucket=v["s3_bucket"], Key=key, Body=b"credential check\n")
    print(f"PUT OK   — wrote s3://{v['s3_bucket']}/{key} (delete when done)")
except ClientError as e:
    sys.exit(f"PUT FAILED — {e.response['Error']['Code']}: {e.response['Error'].get('Message','')}")
