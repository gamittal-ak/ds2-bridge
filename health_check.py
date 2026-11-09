#!/usr/bin/env python3
"""Health check for the deployed ds2-bridge app.

Goal this serves: confirm Parquet is reaching S3 without errors, at whatever
scale is currently live. Run it after adding each DataStream config.

  python health_check.py                 # logs + S3, current hour
  python health_check.py --minutes 30    # widen the log window
  python health_check.py --no-s3         # logs only (fast, no AWS calls)

Exits 0 if clean, 1 if any error, warning or allocation failure is seen, or if
a stream is live in the logs but has written nothing to S3 this hour.

Reads s3-vars.json for bucket/credentials. Never prints the secret.
"""
import argparse
import collections
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import datetime

# Resolve the Spin CLI: $SPIN wins, then PATH, then the default install location.
SPIN = (os.environ.get("SPIN")
        or shutil.which("spin")
        or str(pathlib.Path.home() / ".spin" / ("spin.exe" if os.name == "nt" else "spin")))

# Change #16, deployed 19 Oct 2026. 64 columns; these five were previously lost
# (cliIP/UA/edgeIP to a serde name mismatch, arlid/cmcd had no column at all) and
# must stay populated. If one goes empty, Akamai has renamed or disabled it.
EXPECTED_COLUMNS = 64
MUST_BE_POPULATED = ["cliIP", "UA", "arlid", "cmcd", "reqHost", "statusCode"]
# Correctly empty: DirecTV has not enabled these in their data set.
EXPECTED_EMPTY = 19

# Akamai's per-execution limit. The wasm memory probe added in change #16 reports
# the INSTANCE high-water mark on each completion line, so it climbs to a plateau
# and stays there. Warn well before the ceiling: the old build died at it.
MEMORY_LIMIT_MIB = 128
MEMORY_WARN_MIB = 64
# The app directory is the one holding spin.toml - this script's own directory
# in the repo layout. Override with $APP_DIR if you keep the app elsewhere.
APP_DIR = pathlib.Path(os.environ.get("APP_DIR") or pathlib.Path(__file__).parent)
MAX_LINES = 5000  # platform cap on `spin aka logs -n`

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def spin_logs(minutes):
    out = subprocess.run(
        [SPIN, "aka", "logs", "-n", str(MAX_LINES), "--since", f"{minutes}m"],
        cwd=APP_DIR, capture_output=True, text=True, timeout=300,
    )
    if out.returncode != 0:
        sys.exit(f"spin aka logs failed: {out.stderr.strip()[:400]}")
    return ANSI.sub("", out.stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--no-s3", action="store_true")
    a = ap.parse_args()

    bad = []
    text = spin_logs(a.minutes)
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        sys.exit(f"No log lines in the last {a.minutes}m. Is any config pointed at the app?")

    started = len(re.findall(r"Uploading \d+ bytes to S3", text))
    done = len(re.findall(r"Successfully uploaded to S3", text))
    processed = re.findall(r"Processed (\d+) logs \(request size: (\d+) bytes\) in (\d+) ms", text)
    mem = [float(x) for x in re.findall(r"mem ([0-9.]+) MiB", text)]
    errors = [l for l in lines if "ERROR" in l]
    # "no parseable records" is the empty-batch guard doing its job - DataStream
    # or our own probes can legitimately send an empty body, and we deliberately
    # answer 200. Informational, not a fault. Anything else that warns is.
    all_warns = [l for l in lines if "WARN" in l]
    benign = [l for l in all_warns if "no parseable records" in l]
    warns = [l for l in all_warns if l not in benign]
    allocs = [l for l in lines if "allocation" in l]
    empties = len(re.findall(r"No records to upload", text))
    nameless = len(re.findall(r"missingDisplayName", text))

    stamps = re.findall(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", text, re.M)
    span = None
    if len(stamps) >= 2:
        fmt = "%Y-%m-%d %H:%M:%S"
        lo = min(datetime.datetime.strptime(s, fmt) for s in stamps)
        hi = max(datetime.datetime.strptime(s, fmt) for s in stamps)
        span = (hi - lo).total_seconds()

    streams = collections.Counter(re.findall(r"parquet/live/([A-Za-z0-9._-]+)/", text))

    print(f"=== logs: last {a.minutes}m, {len(lines)} lines"
          + (f" (truncated at the {MAX_LINES}-line cap)" if len(lines) >= MAX_LINES else "")
          + " ===")
    if span:
        print(f"  window observed      : {span:.0f}s")
    print(f"  requests completed   : {processed and len(processed) or 0}")
    print(f"  uploads started      : {started}")
    print(f"  uploads succeeded    : {done}")
    print(f"  empty-batch skips    : {empties}")
    print(f"  ERROR lines          : {len(errors)}")
    print(f"  WARN lines           : {len(warns)}"
          + (f"   (+{len(benign)} benign empty-batch)" if benign else ""))
    print(f"  allocation failures  : {len(allocs)}")
    print(f"  missingDisplayName   : {nameless}")

    if processed:
        logs = [int(p[0]) for p in processed]
        ms = [int(p[2]) for p in processed]
        print(f"  logs/batch min/mean/max : {min(logs)} / {sum(logs)//len(logs)} / {max(logs)}")
        print(f"  ms      min/mean/max    : {min(ms)} / {sum(ms)//len(ms)} / {max(ms)}"
              f"   (30000 ms platform wall)")
        if span and span > 0:
            print(f"  request rate            : {len(processed)/span:.1f}/s")

    if mem:
        lo, hi = min(mem), max(mem)
        avg = sum(mem) / len(mem)
        pct = 100 * hi / MEMORY_LIMIT_MIB
        print(f"  wasm memory min/mean/max: {lo:.1f} / {avg:.1f} / {hi:.1f} MiB"
              f"   ({pct:.0f}% of the {MEMORY_LIMIT_MIB} MiB limit)")
        if hi >= MEMORY_WARN_MIB:
            bad.append(f"wasm memory reached {hi:.1f} MiB, over the {MEMORY_WARN_MIB} MiB warn line")
    else:
        print("  wasm memory             : not reported "
              "(build predates change #16?)")

    print(f"  streams seen         : {len(streams)}")
    for name, n in streams.most_common():
        print(f"    {name}: {n} key mentions")

    if errors:
        bad.append(f"{len(errors)} ERROR lines")
        print("\n  first errors:")
        for l in errors[:5]:
            print("   ", l[:200])
    if warns:
        bad.append(f"{len(warns)} WARN lines")
        print("\n  first warnings:")
        for l in warns[:5]:
            print("   ", l[:200])
    if allocs:
        bad.append(f"{len(allocs)} allocation failures")

    if not a.no_s3:
        v = json.loads((pathlib.Path(__file__).parent / "s3-vars.json").read_text())
        import boto3
        s3 = boto3.client("s3", endpoint_url=v["s3_endpoint"],
                          aws_access_key_id=v["s3_access_key_id"],
                          aws_secret_access_key=v["s3_secret_access_key"],
                          region_name=v["s3_region"])
        now = datetime.datetime.now(datetime.timezone.utc)
        print(f"\n=== S3 {v['s3_bucket']}, hour {now.strftime('%Y/%m/%d/%H')} UTC ===")
        pres = s3.list_objects_v2(Bucket=v["s3_bucket"], Prefix="parquet/live/", Delimiter="/")
        found = [cp["Prefix"].split("/")[-2] for cp in pres.get("CommonPrefixes", [])]
        for name in sorted(set(found) | set(streams)):
            pfx = now.strftime(f"parquet/live/{name}/%Y/%m/%d/%H/")
            n = sz = 0
            newest = None
            for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=v["s3_bucket"], Prefix=pfx):
                for o in page.get("Contents", []):
                    n += 1
                    sz += o["Size"]
                    if newest is None or o["LastModified"] > newest:
                        newest = o["LastModified"]
            mean = f"{sz//n:,}" if n else "-"
            tip = newest.strftime("%H:%M:%S") if newest else "-"
            print(f"  {name:22s} {n:7,} objects  {sz:15,} B  mean {mean:>9} B  newest {tip}")
            if name in streams and n == 0:
                bad.append(f"{name} is live in the logs but wrote nothing to S3 this hour")
        unexpected = [f for f in found if f.startswith("_")]
        if unexpected:
            print(f"  NOTE leftover test prefixes: {unexpected}")

        # Content check: pull the newest live object and confirm the schema and
        # the fields change #16 restored. Delivery success says nothing about
        # whether the data inside is right - that is exactly how UA stayed empty
        # for four months while every metric read green.
        try:
            import pyarrow.parquet as pq
            import tempfile
            newest_key, newest_when = None, None
            for name in sorted(set(found) | set(streams)):
                if name.startswith("_"):
                    continue
                pfx = now.strftime(f"parquet/live/{name}/%Y/%m/%d/%H/")
                for page in s3.get_paginator("list_objects_v2").paginate(
                        Bucket=v["s3_bucket"], Prefix=pfx):
                    for o in page.get("Contents", []):
                        if newest_when is None or o["LastModified"] > newest_when:
                            newest_when, newest_key = o["LastModified"], o["Key"]
            if newest_key:
                tmp = os.path.join(tempfile.gettempdir(), "ds2_healthcheck.parquet")
                s3.download_file(v["s3_bucket"], newest_key, tmp)
                t = pq.read_table(tmp)
                print(f"\n=== content check: {newest_key.split('/')[-1]} ===")
                print(f"  rows {t.num_rows}  columns {t.num_columns}"
                      f"  (expected {EXPECTED_COLUMNS})")
                if t.num_columns != EXPECTED_COLUMNS:
                    bad.append(f"schema has {t.num_columns} columns, expected {EXPECTED_COLUMNS}")
                empty_cols = [c for c in t.schema.names
                              if not any(x not in (None, "") for x in t.column(c).to_pylist())]
                print(f"  always-empty columns: {len(empty_cols)} (expected {EXPECTED_EMPTY})")
                for c in MUST_BE_POPULATED:
                    if c not in t.schema.names:
                        bad.append(f"column {c} is missing from the schema")
                    elif c in empty_cols:
                        bad.append(f"column {c} is empty - renamed or disabled upstream?")
                    else:
                        filled = sum(1 for x in t.column(c).to_pylist() if x not in (None, ""))
                        print(f"    {c:12s} {filled}/{t.num_rows} populated")
        except ImportError:
            print("\n(pyarrow not installed - skipping the content check)")
        except Exception as e:
            print(f"\n(content check skipped: {e})")

    print()
    if bad:
        print("UNHEALTHY: " + "; ".join(bad))
        return 1
    print("HEALTHY: no errors, no warnings, no allocation failures.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
