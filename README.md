# ds2-bridge

Converts Akamai **DataStream 2** logs to **Parquet** and uploads them to S3, as
a WebAssembly component running on **Akamai Functions**.

Built for DirecTV. Live since 19 October 2026, both streams at 100% sampling.

```
DataStream 2  ──POST NDJSON──▶  ds2-bridge (Wasm)  ──PutObject──▶  S3
                                                                    │
                                                              Athena / Spark
```

Output lands at:

```
parquet/live/{stream}/{YYYY}/{MM}/{DD}/{HH}/log-{YYYY-MM-DD-HH-MM-SS-mmm}-{hash}.parquet
```

64 columns, ZSTD, `PARQUET_1_0`. The stream name comes from the URL path, so
**adding a DataStream config needs no code change and no redeploy** — point it
at `/api/v1/transform/multiple/<name>` and it starts filing into
`parquet/live/<name>/`.

---

## Read this first

**[RUNBOOK.md](RUNBOOK.md)** — current state, every procedure, troubleshooting,
open items and the facts worth not re-deriving. It is self-contained; start
there.

---

## Build

**This repository does not contain a built artifact.** `spin.toml` points at
`ds2-parquet/target/wasm32-wasip2/release/ds2_parquet.wasm`, which is
gitignored, so a fresh clone cannot deploy until you build.

Building needs the **WASI SDK** — `zstd-sys` compiles C for the wasm target and
the system toolchain will not do it:

```bash
export CC_wasm32_wasip2=/opt/wasi-sdk/bin/clang
export AR_wasm32_wasip2=/opt/wasi-sdk/bin/llvm-ar
export CFLAGS_wasm32_wasip2="--sysroot=/opt/wasi-sdk/share/wasi-sysroot"
export WASI_SYSROOT=/opt/wasi-sdk/share/wasi-sysroot
export WASI_SDK_PATH=/opt/wasi-sdk

cd ds2-parquet
cargo build --target wasm32-wasip2 --release
cargo test --release          # 8 tests, all must pass
```

Takes about 5 seconds incrementally. See RUNBOOK §2 for which machines can build.

## Deploy

```bash
spin aka deploy --no-confirm --variable @s3-vars.json
```

`--build` is opt-in, so whatever wasm sits at the `source` path is what ships.
**Check the md5 before deploying.**

`s3-vars.json` is **not in this repository** — it holds live credentials. Create
it locally:

```json
{
  "s3_bucket": "akamai-log-analysis",
  "s3_region": "us-west-2",
  "s3_endpoint": "https://s3.us-west-2.amazonaws.com",
  "s3_access_key_id": "...",
  "s3_secret_access_key": "..."
}
```

All five variables must be passed on **every** deploy — the platform does not
persist them. Omit one and the deploy succeeds but the component fails at
runtime.

Full procedure, including the stage step, is in RUNBOOK §4.2. Do not skip stage.

## Operate

```bash
python health_check.py      # errors, throughput, memory, S3, schema + field content
./tail_logs.sh              # live error watch; silent means healthy
python check_creds.py       # validate credentials before deploying them
```

`health_check.py` exits non-zero on any error, warning, allocation failure, a
schema that is not 64 columns, or any of `cliIP`/`UA`/`arlid`/`cmcd` going
empty. Run it after adding each config.

---

## Two rules that are easy to break silently

**Never add `#[serde(rename_all = ...)]` to `Datastream2Log`.** Field names are
the DataStream 2 JSON keys *verbatim*. camelCase conversion cannot produce
embedded acronyms, and it silently dropped `UA`, `cliIP` and `edgeIP` for four
months — the records parsed, the row counts were right, and the columns were
empty. `ds2::tests::serde_names_match_the_parquet_schema` guards this.

**Append new columns, never insert.** The consuming Athena table maps
positionally; inserting a column mid-schema shifts every column after it and
silently corrupts the table.

## Why `PARQUET_1_0`

`PARQUET_2_0` makes the byte-array fallback encoder `DELTA_BYTE_ARRAY`, which
reserves two 1 MiB buffers **per column** the moment a row group opens — about
123 MiB before reading a single log, against Akamai's 128 MiB per-execution
limit. The buffers are reserved and never written, so they never appear in RSS,
which is why it went undiagnosed for months. ZSTD was never the problem and is
unchanged.

Current footprint: ~31 MiB peak at 100% sampling, 25% of the limit.

---

## Layout

```
spin.toml                 app manifest, route and outbound allowlist
ds2-parquet/
  Cargo.toml  Cargo.lock  Cargo.lock is committed: it is the only place that
                          shows spin-sdk 7 pulls wasip3 0.7.1+wasi-0.3.0
  src/lib.rs              handler, S3 key, upload
  src/ds2.rs              schema, parsing, record batch
  src/aws.rs              WASI HTTP transport for the AWS SDK
RUNBOOK.md                operations
health_check.py  check_creds.py  tail_logs.sh
```

Originally derived from `mikkelhegn/ds2-bridge-rust`, since diverged by sixteen
changes. Not tracked as a fork.
