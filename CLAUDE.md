# pooldepth

Go script that reproduces Dune query 7685210 ("FINAL pool depth template") — Aerodrome Slipstream
hourly pool depth on Base — directly from a Base JSON-RPC endpoint. Stdlib only, no go-ethereum.

Reference SQL (optimized version): `pool_depth_template_optimized.sql` in this folder if present,
otherwise https://dune.com/queries/7685210.

## Files

- `main.go` — the whole pipeline (RPC client, log decoding, hourly aggregation, band depth, CSV out)
- `go.mod` — module `pooldepth`, Go 1.22, no dependencies

## Run

```bash
go vet ./...
go run . -rpc "$BASE_RPC_URL" \
         -pool 0x0aed2bd5abdffcde57c0bcf30e75cd594b8876a9 \
         -quote token1 \
         -start 2026-05-01 \
         -out depth.csv
```

Flags: `-rpc` (default https://mainnet.base.org), `-pool`, `-quote` (`token0`|`token1`, the slot
holding the USD token), `-start` (UTC date, inclusive), `-end-block` (0 = latest), `-step` (getLogs
block chunk, default 10000), `-workers` (concurrent header fetchers, default 8), `-batch`
(getBlockByNumber calls per JSON-RPC batch, default 100), `-linear-ts` (derive block timestamps as
`ts(first) + 2s*(n-first)` instead of fetching headers; Base is a fixed-2s OP-stack chain and the
script verifies the last block before trusting it), `-interval` (bucket size in minutes, default 60;
must divide 1440 — e.g. 15 for quarter-hours; the output column is still named `hour` so the same
Dune query works), `-out`.

Recommended invocation (verified 2026-09-03, ~7 min for 3 days of a busy pool):

```bash
go run . -rpc https://mainnet.base.org -pool <pool> -quote token1 -start 2026-09-01 -linear-ts -out depth.csv
```

## Provider notes (measured 2026-09-03)

- **Public https://mainnet.base.org**: since ~2026-09-08 getLogs is capped at a **2,000-block range**
  (HTTP 413, "eth_getLogs is limited to a 2,000 range"); `-step` defaults to 2000 accordingly. Swap,
  Mint and Burn are fetched in one OR-filtered request per chunk. Batches are capped at 10 calls,
  and it 429s quickly. Without `-linear-ts` the timestamp phase is the bottleneck
  (needs `-batch 10 -workers 2` and takes ages). With `-linear-ts` it is the best free option.
- **Alchemy free tier**: getLogs is limited to a **10-block range** — useless for this script.
  Batches of 100 work but return ~1.6 MB per batch. Intermittent 503s. Only worth it on PAYG.
- Rate-limit errors (429 / "over rate limit" / "capacity") on getLogs are retried with backoff and
  never trigger range bisection; only other errors (too many results) bisect.

Output CSV columns match the Dune query exactly:
`hour, open, high, low, close, impact_p90, tier, base_250, base_500, base_1500, quote_250, quote_500, quote_1500`

## First-run checklist — status as of 2026-09-03

1. ✅ `go vet ./...` passes; builds with Go 1.26.
2. ✅ Topic0 hashes confirmed against real logs from pool 0x0aed…76a9 (Swap: 3 topics/5 data words,
   Mint: 4/4, Burn: 4/3). The pool also emits Collect (0x709353…), which is correctly ignored.
3. ✅ Decoded values plausible on CP-USDC (0x78fa9fb7a76c8ea204754a8d4ff301aa29935e8e, USDC = token1):
   price ≈ $0.04–0.08, quote depth in the thousands of USDC.
4. ✅ Compared all 22 hours of CP-USDC against Dune execution 01M1KHVE00KA1C1MVW3JW8QNXD
   (2026-09-03). `high`, `low`, `tier` identical; `open`/`close` differ in ~8 hours by <1% (same-block
   ties); `impact_p90` differs in every hour by <0.2% except two hours at ~7% (t-digest); depths match
   to <1e-6 except where `close` chose a different same-block swap (then `sp` differs and the ±2.5%
   band can move 20-40% in this thin pool) and the still-open last hour (Dune ran later).

Pools seen so far: 0x0aed…76a9 was deployed 2026-05-07 (block 45665723), so `-start 2026-05-01`
captures its full life. CP-USDC 0x78fa…5e8e first swapped 2026-09-02 14:00 UTC (block 50784937).
ALIGN-USDC 0x0f56aeba06f65e2790c5b6687f5c3128b7456f76 (USDC = token1, tick spacing 100) was deployed
2026-08-18 15:42 UTC (block 50139213); use `-start 2026-08-18`.

## Pipeline (mirrors the SQL CTEs)

| SQL CTE | Script |
|---|---|
| `clpool_call_initialize` + `tokens.erc20` | `eth_call` `token0()`, `token1()`, `decimals()` |
| `swap_samples` / `priced` | decode Swap logs, skip `liquidity == 0`, compute `price_usd` and `px_impact` |
| `hourly` (ohlc / impact_p90 / close_ts / sp) | single pass over swaps sorted by (block, logIndex) into `hourRow` |
| `liq_delta_events` | Mint → `(tickLower, +amt), (tickUpper, -amt)`; Burn → `(tickLower, -amt), (tickUpper, +amt)` |
| `assigned` / `hourly_tick_net` | events applied to `liqNet[tick]` while `event_ts <= hour.closeTs`, carried forward |
| `seg` / `seg_px` / `band_amounts` | per hour: sort ticks, cumulative `active`, segment `[s_lo, s_hi)`, ±2.5/5/15% bands |
| `band_valued` / `band_depths` / final | scale by decimals, quote positive / base negative, write row |

Semantics preserved from the SQL (with `-interval`, read "hour" as "bucket" — the rules are identical):
- Hours with no swaps are absent from the output.
- A mint/burn belongs to hour H if its block timestamp `<= close_ts(H)` (last swap of H). Events after
  the last swap-hour are dropped.
- Only mints/burns from `-start` onward are counted. Positions opened earlier are invisible, so the
  depth is "liquidity added since start", same as the Dune query. For true depth, set `-start` to the
  pool's creation date.
- Hours whose tick map has fewer than 2 ticks produce no `band_depths` row and are skipped (inner join).

## Known differences vs Dune

- `impact_p90`: Dune uses `approx_percentile` (t-digest); script computes an exact interpolated
  percentile. Expect small deviations, and occasionally a different `tier` label at thresholds.
- Same-second ties for `open`/`close`/`sp`: Dune's `min_by`/`max_by` is nondeterministic within a
  block; the script orders by `logIndex`.

## Performance notes

- The slow step is block timestamps: one `eth_getBlockByNumber` per distinct block touched by any
  event, batched `-batch`/request across `-workers` goroutines. A busy pool over 4 months can be 100k+
  blocks. Prefer `-linear-ts`, which removes this phase entirely on Base; otherwise raise `-workers`
  on a provider with generous rate limits, lower it if you get 429s.
- `eth_getLogs` ranges auto-bisect on non-retryable provider errors (4xx other than 429/408 return
  immediately; 429/5xx/network errors retry with backoff first). `-step` defaults to 2000 for the
  public endpoint; raise it on a provider that allows wider ranges.
- To iterate on the math without re-hitting the RPC, the natural next step is to cache decoded
  logs + timestamps to a local JSON/gob file — not implemented yet.

## Conventions

- Keep it stdlib-only; don't add go-ethereum unless there's a concrete need.
- Don't change the float64 math order in the hourly/band loops without re-comparing against Dune —
  it's deliberately the same expression order as the SQL.
- Timestamps are UTC throughout; output `hour` is `YYYY-MM-DD HH:MM:SS` UTC.

XDP-USDC 0x1ef035205f94c7734827961c438474d089a334a0 (USDC = token1, tick spacing 200) was deployed
2026-09-11 08:22 UTC (block 51162803); use `-start 2026-09-11`.

## Incremental mode + filter logs (2026-10-10)

Since ~2026-10-09 mainnet.base.org answers every `eth_getLogs` with 429 / -32011 "request limit
reached" (CI hung until the 2h timeout). It still serves `eth_newFilter` → `eth_getFilterLogs` →
`eth_uninstallFilter`, which returns the identical log set — `-filter-logs` uses that. This is a
loophole Coinbase may close; the paid PAYG RPC is the planned permanent fix (set `RPC`).

`-state DIR` (see `state.go`) makes runs incremental: `state.json` checkpoint (`next_block` /
`next_bucket` = start of the last, possibly open bucket), `liq_snapshot.csv` (liqNet per tick folded
up to the checkpoint, exact float round-trip) and `liq_events.csv` (mint/burns after it). Finished
rows come verbatim from the previous `-out` CSV. Verified on LAPTOP-USDC: seed + incremental output is
byte-identical to a full scan at the same end block (same CPU arch; arm64 vs amd64 differ in the last
float digit because Go fuses multiply-adds on arm64). Delete the state folder to force a full rescan.

Persistent 429s now fail fast (`log.Fatal`) instead of bisecting; archive/pruned errors too.
`-start` also accepts `YYYY-MM-DDTHH:MM`.
