# ds2-bridge — runbook

Written 20 October 2026. **Start here to pick the project up.** Everything below
was performed or verified in this project; anything untested is labelled.

This runbook is self-contained — it is the only document shipped with the code.
The working notes, the full investigation and all measurements stay on the
originating machine and are not in this repository.

---

## 1. State at a glance

| | |
|---|---|
| App | `ds2-bridge`, ID `8ea37952-ad01-4d91-a9ae-ff6117547e87` |
| Endpoint | `https://8ea37952-ad01-4d91-a9ae-ff6117547e87.fwf.app/api/v1/transform` (wildcard) |
| Live artifact | md5 `555b2dde2cd44eaa96a77a0c03a50ebc` (change #16) |
| Streams | 89424 `c0p7-os`, 119761 `c0p6-os` — both **100% sampling**, 30 s frequency |
| Schema | **64 columns**; 19 correctly empty (DirecTV has not enabled them) |
| Memory | ~31 MiB peak, 25% of the 128 MiB per-execution limit |
| Throughput | ~17 req/s, ~400k objects and ~14 GB per 8 hours |
| S3 | `akamai-log-analysis`, us-west-2, `parquet/live/{stream}/{Y}/{M}/{D}/{HH}/` |

Adding a config requires **no code change and no redeploy** — see §4.1.

---

## 2. Where things live

| Thing | Location | Notes |
|---|---|---|
| Deployable source | `ds2-bridge-rust/` | Upstream `9f7f81c` + **16 uncommitted changes** |
| Built artifact | `ds2-bridge-rust/ds2-parquet/target/wasm32-wasip2/release/ds2_parquet.wasm` | The path `spin.toml` reads. Keep it — a redeploy needs no rebuild |
| Credentials | `s3-vars.json` (project root) | **Live production secret, plaintext.** Must be gitignored |
| Build host | Linode `root@172.236.224.108`, `/root/ds2-bridge-rust` | SSH key auth from this machine |
| Deploy host | **This Windows machine** | Only machine with a live `spin aka` token |
| Old production source | `functions-ds2-bridge-main@f02930228d9/` | Retired two-component build, kept for history |

**The split exists for a reason.** Windows cannot build (`dlltool.exe` missing
for the `x86_64-pc-windows-gnu` toolchain; switching to `msvc` will not help —
no MSVC toolset or Windows SDK is installed). The Linode's `spin aka` token is
**expired**. So: **build on the Linode, deploy from Windows.** The Linode may be
shut down between uses; its disk and toolchain persist.

---

## 3. Routine checks

```bash
cd ~/Documents/DTV_Functions_Parquet
python health_check.py                 # errors, throughput, memory, S3, content
python health_check.py --minutes 30    # wider log request (see note)
./tail_logs.sh                         # live error watch; silent = healthy
```

`health_check.py` exits non-zero on any error, warning, allocation failure, a
schema that is not 64 columns, or any of `cliIP`/`UA`/`arlid`/`cmcd` going
empty. **Run it after adding each config.**

**Note:** `--minutes` barely matters. `spin aka logs` caps `-n` at 5,000 and we
emit ~50 lines/second, so you always get roughly the last 100 seconds. For
anything retrospective use the console, which filters server-side:

```
https://ui.fwf.dev/app/7f0796ba-c230-4536-a158-f6d882664805/8ea37952-ad01-4d91-a9ae-ff6117547e87?tab=logs&timeRange=last-24-hours&logType=stderr
```

`Showing 0 logs` there is the authoritative "nothing failed today". The
**Usage** tab gives requests, CPU time and memory per request, none of which the
CLI reports.

---

## 4. Procedures

### 4.1 Add a DataStream config — zero touch

1. In Akamai DataStream, point the new stream at
   `https://8ea37952-ad01-4d91-a9ae-ff6117547e87.fwf.app/api/v1/transform/multiple/<name>`
2. It immediately files into `parquet/live/<name>/{Y}/{M}/{D}/{HH}/`
3. Run `python health_check.py` and confirm the new stream appears and the
   memory max has not moved materially

No code change, no redeploy, no manifest entry. The path segment becomes the S3
folder. A stream posting to the bare endpoint lands in `missingDisplayName/`
with a warning — that is a misconfigured stream, not a bug.

**Also needed, on DirecTV's side:** an Athena table for the new prefix, mirroring
`akamai_live_logs_parquet_c0p7`.

### 4.2 Deploy a code change

Four stages. Do not skip stage — change #16 touched 48 field names and stage is
what proved it before live traffic saw it.

**a. Build on the Linode**

```bash
scp ds2-parquet/src/<changed>.rs root@172.236.224.108:/root/ds2-bridge-rust/ds2-parquet/src/
ssh root@172.236.224.108 'export PATH=$HOME/.cargo/bin:$PATH
export CC_wasm32_wasip2=/opt/wasi-sdk/bin/clang
export AR_wasm32_wasip2=/opt/wasi-sdk/bin/llvm-ar
export CFLAGS_wasm32_wasip2="--sysroot=/opt/wasi-sdk/share/wasi-sysroot"
export WASI_SYSROOT=/opt/wasi-sdk/share/wasi-sysroot
export WASI_SDK_PATH=/opt/wasi-sdk
cd /root/ds2-bridge-rust/ds2-parquet
cargo build --target wasm32-wasip2 --release
cargo test --release
md5sum target/wasm32-wasip2/release/ds2_parquet.wasm'
```

`cargo` is **not** on a non-interactive SSH PATH — hence the first export.
Builds take ~5 s. All tests must pass (8 as of change #16).

**b. If the schema or field mapping changed, measure memory**

```bash
ssh root@172.236.224.108 'cd /root/ds2-bridge-loadtest
python3 run_cap_rust.py /tmp/b24_real.ndjson 1 64 3 /tmp/cap.csv tag'
```

Baseline: **19 MiB** at a 2.4 MB batch, **51 MiB** at the 10 MiB ceiling.

**Fixture warning.** `gen_bodies.py` builds from a `sample.json` that predates
change #16. Always pass generated bodies through `normalise_ds2_keys()` or the
fixture will not bind `UA`/`cliIP`/`edgeIP` and every measurement will
under-report. This already fooled us once: a cap run appeared to show memory
*improving* after adding columns. **If output size falls when the schema grows,
suspect the fixture, not the build.**

**c. Stage**

`--create-name` is refused while the workspace is linked to prod, so deploy
stage from a **separate directory**:

```bash
STAGE=/tmp/stage && rm -rf $STAGE && mkdir -p $STAGE/ds2-parquet/target/wasm32-wasip2/release
cp ds2-bridge-rust/spin.toml $STAGE/ && cp s3-vars.json $STAGE/
scp root@172.236.224.108:/root/ds2-bridge-rust/ds2-parquet/target/wasm32-wasip2/release/ds2_parquet.wasm \
    $STAGE/ds2-parquet/target/wasm32-wasip2/release/
sed -i 's/^name = "ds2-bridge"/name = "ds2-bridge-stage"/' $STAGE/spin.toml
cd $STAGE && spin aka deploy --no-confirm --create-name ds2-bridge-stage --variable @s3-vars.json
```

Verify with an empty body, then DirecTV's real sample, then read the Parquet
back and confirm every field lands. Delete the test objects and the app:

```bash
spin aka app delete --no-confirm --app-id <STAGE_ID>
```

**Stage writes to the real bucket** under whatever path segment you POST to. Use
a `_` prefix (`_stagecheck`) and delete the objects afterwards.

**d. Production**

```bash
cd ds2-bridge-rust
scp root@172.236.224.108:/root/.../ds2_parquet.wasm ds2-parquet/target/wasm32-wasip2/release/
cat .spin-aka/*                      # MUST be 8ea37952-ad01-4d91-a9ae-ff6117547e87
md5sum ds2-parquet/target/wasm32-wasip2/release/ds2_parquet.wasm
spin aka deploy --no-confirm --variable @../s3-vars.json
```

Then within a minute or two:

```bash
python health_check.py --minutes 5
```

`--build` is opt-in, so the prebuilt wasm is what ships. **Variables are not
persisted by the platform** — all five must be passed on every deploy or the
component fails at runtime on `variables::get`.

### 4.3 Rotate S3 credentials

```bash
# edit s3-vars.json - replace BOTH s3_access_key_id and s3_secret_access_key
python check_creds.py            # identity only
python check_creds.py --write    # proves PutObject, writes one object
# then redeploy so the app picks up the new values:
cd ds2-bridge-rust && spin aka deploy --no-confirm --variable @../s3-vars.json
```

Delete `parquet/live/_credcheck/credcheck.txt` afterwards. A stale secret
against a new ID gives `SignatureDoesNotMatch`, which `check_creds.py` reports
distinctly from a bad ID.

### 4.4 Rollback — **UNTESTED**

We have never rolled back. What is known: `spin aka app history` lists past
deployment versions, and `spin aka logs --deployment-version N` can target one,
which implies the platform retains them. **Whether a previous version can be
redeployed has not been verified.**

The route we know works is **roll forward**: rebuild the previous source on the
Linode and deploy it as a new version. Keep the current artifact md5 recorded in
`STATUS.md` before each deploy so you know what to return to. Confirm the
rollback mechanism before you need it, not during an incident.

---

## 5. Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `health_check.py` → UNHEALTHY, ERROR lines | Real fault | Read the error; check the console `stderr` + 24h view for scope |
| A stream appears in logs but writes nothing to S3 | Upload failing for that stream | Check `s3-vars.json` validity with `check_creds.py`; check the outbound allowlist in `spin.toml` matches the bucket and region |
| `cliIP`/`UA`/`arlid`/`cmcd` suddenly empty | Akamai renamed or disabled a field | `health_check.py` flags this. The unknown-key counter names any new key in the logs |
| `unrecognised DataStream fields` warning | Akamai added a field we do not model | Add it to `Datastream2Log` **and** `create_schema()`, **appended at the end** — DirecTV's Athena table maps positionally |
| Objects/minute drops for one stream only | Possibly 413s — batches over the 10 MiB cap | **Invisible to us.** Compare both streams' cadence. See §6 |
| `memory allocation of N bytes failed` | The original failure mode, should not recur | Check `WriterVersion` is still `PARQUET_1_0`. Escalate — this was the founding bug |
| Deploy succeeds, component fails at runtime | A variable was not passed | All five `s3_*` variables on every deploy |
| `Cannot specify --create-name...` | Workspace is linked to prod | Deploy stage from a separate directory (§4.2c) |
| `invalid_grant: Offline session not active` | `spin aka` token expired | `spin aka login`, or `--token` / `SPIN_AKA_ACCESS_TOKEN` |
| `not a terminal` on delete | Missing flag | `spin aka app delete --no-confirm --app-id <id>` |
| CLI logs seem to ignore `--since` | Not a fault | 5,000-line cap, ~100 s of history. Use the console |

---

## 6. The one blind spot

**A 413 is invisible to us.** If a batch exceeds the 10 MiB request cap, Akamai
rejects it on `Content-Length` before the component instantiates — nothing
logged, health check blind, data absent. Tested 19 Oct: a 12,082,763 B body
returns HTTP 413 with `size_sent=0` in 0.2 s; 9,982,525 B is accepted and
processed in 353 ms at 43.9 MiB.

**Headroom:** `c0p6-os` peaks at ~41% of the cap, so roughly **2.4x** growth
before rejections begin. `c0p7-os` has ~19.6x. Batch size scales with viewership
at fixed 30 s frequency, so a live event is the risk.

**There is no frequency lever** — DataStream offers only 30 s or 60 s, and 60 s
would double batch size and leave c0p6 effectively at the cap. 30 s is already
the safe choice.

Both mitigations are outside our control:

1. **Ask Akamai to raise the request/response limit.** The quotas page says *"If
   you're interested in higher limits, please contact your Akamai
   representative."*
2. **Ask Akamai to surface platform-level rejections in app logs.** It is their
   infrastructure doing the rejecting; this is a product gap, not a technical
   limit.

Worth checking first, both free: whether the console **Usage** tab's request
count includes rejected requests (if so, the gap versus our processed count is a
ready-made detector), and whether the console exposes a log type beyond
`stdout`/`stderr`.

---

## 7. Open items

| Item | Owner | Notes |
|---|---|---|
| **GitHub repo** | us | 16 changes uncommitted. `origin` still points at `mikkelhegn/ds2-bridge-rust` — a careless push sends DirecTV-specific changes to the Functions team's public repo. Rename to `upstream` first, and gitignore `s3-vars.json` |
| `ALTER TABLE ADD COLUMNS (arlid string, cmcd string)` | DirecTV | Until then `SELECT *` will not show the two new columns |
| Hour-level partition in Athena | DirecTV | Acknowledged, not yet done. Their table projects `year/month/day` only, so an hour query scans all 24 hours |
| `edgeIP` returns literal `-` | DirecTV | Populated but carries Akamai's no-value placeholder. Is that expected on their property? |
| Raise the 10 MiB cap | Akamai | See §6 |
| Surface 413s in logs | Akamai | See §6 |
| What DS2 does with a 500 or 413 | unverified | Determines whether a failed batch is retried or lost. A 413 can never succeed on retry |
| Verify rollback | us | §4.4 |
| Log thinning | us | Considered and **declined** 20 Oct — see `OBSERVABILITY.md` |
| ~~Warehouse reading `PARQUET_1_0`~~ | — | **Confirmed 20 Oct 2026** — DirecTV can read the data. Closed |

---

## 8. Command gotchas

Every one of these cost time at least once.

| Gotcha | Consequence |
|---|---|
| The workspace is **linked to prod** (`.spin-aka/`) | A bare `spin aka deploy` ships to production. Run `spin aka info` first — it prints the linked app |
| `--create-name` is refused while linked | `Cannot specify --create-name when deploying to an existing app`. Deploy stage from a **separate directory** (§4.2c), never by unlinking prod |
| `spin aka app delete` needs `--no-confirm` | Without it: `Caused by: not a terminal` |
| `--no-confirm` is required for any automation | Prompts hang on non-interactive stdin |
| **Variables are not persisted** | All five `s3_*` must be passed on *every* deploy or the component fails at runtime on `variables::get` |
| `--build` is opt-in | Easy to redeploy a stale wasm. Always check the md5 before deploying |
| `-n` caps at **5000** | Larger values are rejected: `max_lines: must be ... less than or equal to 5000` |
| `--since` is ineffective at our volume | 5000 lines is ~100 s of history regardless. There is no `--until` and no pagination. Use the console |
| Log output contains **ANSI colour codes** | Breaks naive `grep`/`awk`. Strip with `sed -r 's/\x1b\[[0-9;]*m//g'` |
| `-f` means `--from`, **not** follow | There is no `--follow`. `tail_logs.sh` polls and de-duplicates |
| `cargo` is not on a non-interactive SSH PATH | Builds fail with "command not found" until `PATH` is exported |
| Token expiry is silent until used | Surfaces as `invalid_grant: Offline session not active` |
| `RUST_BACKTRACE` is ignored on the platform | Only works under local `spin up` |

### Which tool answers which question

| Question | Tool | Why |
|---|---|---|
| Did anything fail today? | **Console**, `stderr` + 24 h | Filters server-side across the whole range. Authoritative |
| What is throughput / memory right now? | `health_check.py` | Needs the detail lines; sample is ~1,667 requests |
| What happened an hour ago? | **Console** | The CLI cannot reach back |
| Requests, CPU, memory per request | **Console → Usage tab** | The CLI reports none of it |

Console: `https://ui.fwf.dev/app/<account-id>/<app-id>?tab=logs`

## 9. Facts not to re-derive

- The founding bug was **one line**: `WriterVersion::PARQUET_2_0` → `PARQUET_1_0`.
  `PARQUET_2_0` makes the byte-array fallback `DELTA_BYTE_ARRAY`, which reserves
  two 1 MiB buffers **per column** at row-group open — ~123 MiB before reading a
  single log. **Not ZSTD**, which is unchanged and still in use.
- It hid because **reserved memory never appears in RSS**. The Wasm limit counts
  reserved. Every intuitive metric said the app was fine.
- **128 MiB is per execution, not per app** — measured, not inferred.
- Field names in `Datastream2Log` are the DataStream 2 JSON keys **verbatim**.
  **Never add `serde(rename_all)`** — camelCase cannot produce embedded acronyms
  and silently dropped `UA`, `cliIP`, `edgeIP` for four months.
- New columns are **appended**, never inserted — the Athena table maps
  positionally.
- `"wasip2"` means WASI preview 2. There is **no `wasm32-wasip3` rustup target**;
  our component compiles with the wasip2 target and imports `wasi:http@0.3.0`
  via the `wasip3` crate. The build command and the real host requirement
  disagree — the lockfile is the truth.
