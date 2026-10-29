# XDP-USDC pool depth — auto-refreshed to Dune

Reproduces Dune query 7685210 (hourly / N-minute pool depth) straight from a Base RPC and
uploads the CSV to Dune every day via GitHub Actions.

- `main.go` — the pipeline (see `CLAUDE.md` for flags and semantics)
- `upload_dune.py` — pushes a CSV to Dune's table upload API (replaces the table)
- `.github/workflows/refresh.yml` — daily 02:00 UTC run for XDP-USDC **v2** (0xc3cbfbaab532a04e36b6f901d725eaa5113fd781, USDC = token1) at 15-minute buckets
- `state_v2/` — incremental checkpoint for v2 (`state/` is the frozen v1 checkpoint)

## Setup

1. Push this folder to a GitHub repo.
2. Repo → Settings → Secrets and variables → Actions → **New repository secret**:
   `DUNE_API_KEY` = your Dune API key (Dune → Settings → API).
3. Actions tab → "Refresh XDP-USDC depth (v2)" → **Run workflow** to test.
4. Query it on Dune:

```sql
SELECT date_parse(hour, '%Y-%m-%dT%H:%i') AS ts, *   -- Dune stores hour as text like '2026-10-29T14:45'
FROM dune.forcemulti.dataset_xdp_usdc_depth_15m_v2   -- v1 (old pool): dataset_xdp_usdc_depth_15m
ORDER BY 1
```

To track another pool, copy the job in `refresh.yml` and change `POOL`, `QUOTE`, `START`,
`OUT`, `DUNE_TABLE`.

## Aerodrome pool migration (2026-10-21) — v1 vs v2

Aerodrome upgraded Slipstream and redeployed the pools under a new factory. XDP-USDC moved to a new
pool, so the data is split in two:

| | v1 (frozen) | v2 (live) |
|---|---|---|
| Pool | `0x1ef035205f94c7734827961c438474d089a334a0` | `0xc3cbfbaab532a04e36b6f901d725eaa5113fd781` |
| Factory | `0xf8f2eb4940cfe7d13603dddd87f123820fc061ef` | `0x9cb220581b91b0c83223012fe69688b92ad07a76` |
| Pool implementation | `0xc770898522d2a9c8da7a10d63989b6b58305b665` | `0x1642634c8cb1c9f1b84c9a1a08534f8c3f4e89f8` |
| Data from | 2026-09-11 | pool deployed 2026-10-21 05:27:49 UTC (block 52,885,561), first swap 2026-10-22 00:00 UTC |
| CSV | `depth_xdp_usdc_15m.csv` | `depth_xdp_usdc_15m_v2.csv` |
| State | `state/` | `state_v2/` |
| Dune table | `dune.forcemulti.dataset_xdp_usdc_depth_15m` | `dune.forcemulti.dataset_xdp_usdc_depth_15m_v2` |

What did **not** change: token pair and order (USDC = token1, so `QUOTE: token1`), tick spacing
(200), and the Swap / Mint / Burn event signatures and layout — the script runs unchanged. Verified
locally on 2026-10-22: the liquidity the script rebuilds from Mint/Burn equals the new pool's
on-chain `liquidity()` exactly, so v2 depth covers all of the new pool's liquidity.

Liquidity moved to the new pool quickly: by 2026-10-22 06:00 UTC it held ~58x the old pool's in-range liquidity (±2.5% depth ~$85k, tier Reliable). The old pool keeps trading on thin residual liquidity (arbitrage/aggregator routing).

The v1 table is **not** continued: its last bucket is 2026-10-22 06:45 UTC (refreshes stopped manually on 2026-10-22). v2 is a separate
table starting at the new pool's first swap. To chart both pools together, query them side by side,
e.g. `SELECT 'v1' AS pool, * FROM dune.forcemulti.dataset_xdp_usdc_depth_15m UNION ALL SELECT 'v2', * FROM dune.forcemulti.dataset_xdp_usdc_depth_15m_v2`.
