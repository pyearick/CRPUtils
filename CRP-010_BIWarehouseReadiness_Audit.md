# CRP-010 — BIWarehouse readiness checks: audit, fixes, and open design

_Status: fixes applied 2026-08-03. The "normal-size baseline" section below is the
"for further input" part — it is a proposal, not built._

## Why this exists

BIWarehouse (`[BIWarehouse].[BIData].*` on BI-SQL001) is refreshed from
`CRPREPORTS\BI` by SQL Server **snapshot replication**: it TRUNCATEs then BULK
INSERTs each article one at a time, **non-atomically**. Mid-refresh, any table can
be empty, partial, or stale. A consumer that reads during that window silently gets
bad data and — if it writes a snapshot table — can clobber yesterday's good data
with garbage or emptiness. That zero-row **false all-clear** is the worst failure
mode because nothing errors.

**Observed example (2026-08-02 → 08-03).** On 08-02, `BIWarehouse.BIData.FillRate`
and `BIWarehouse.BIData.BOMMaster` were **empty** mid-refresh — both are tracked in
the readiness view (floors 100,000 and 50,000). By 08-03 they were repopulated
(FillRate ≈ 608K in the 12-mo window, BOMMaster ≈ 224K) and the census showed **0
empty tables**. This is exactly the window the gate protects: had the PLM
orchestrator run at the 08-02 moment, the new top-level preflight would have found
those two tables below floor, **aborted before any write** (leaving yesterday's good
snapshots intact), and emailed. Run at the 08-03 moment it passes silently — the
alert is edge-triggered on not-ready, not a daily all-clear.

The shared guard is `CRPUtils/BIWarehouseStatus.py` → `is_ready(...)`, a two-phase
check (row counts above a floor **and** stable across two samples 15s apart, to
catch the load-then-truncate race). Its authoritative source is the SQL view
`CRPAF.dbo.vw_BIWarehouseReadiness` (see PLM's
`PLM_078_ReadinessView_AddPurchaseOrders.sql`).

## What CRP-009 changed, and the risk it created

CRP-009 (commit `cf9285c`, 2026-08-02) added a second scoping mode to `is_ready()`:

- **`required_sources=['BIWarehouse']`** (original, strict) — blocks if *any*
  tracked table under that source DB is below its floor. Every real caller uses this.
- **`required_tables=[...]`** (new) — scope the check to only the source-qualified
  tables a job actually reads, so one unrelated empty source (e.g. `FillRate` empty
  at CRPReports on 2026-08-02) can't block a job that never reads it.

The **risk**: the readiness view only tracks ~10 curated tables. In the CRP-009
code, if a caller scoped to tables the view does **not** track, `is_ready()` logged a
warning and **returned ready** — a silent all-clear against a warehouse it never
actually verified. That is precisely the "a script continues even though the full
BIWarehouse is not populated" behavior the punchlist flagged.

## Fixes applied under CRP-010

### 1. `CRPUtils/BIWarehouseStatus.py`
- **Fail-safe the false-pass.** When `required_tables` matches **none** of the
  tracked tables, `is_ready()` now returns **NOT ready** with a clear reason
  (logged at ERROR), instead of returning ready. A mixed list still gates on the
  tracked subset and logs the untracked ones it can't check. Safe to tighten now —
  no caller uses `required_tables` yet; all use `required_sources`.
- **All-tables census (`get_table_census()`).** Row count of **every**
  `BIWarehouse.BIData` table via `sys.partitions` metadata (instant, no scan — the
  same technique PLM-078 moved the view's unfiltered checks onto). 41 tables today
  vs the ~10 the view tracks.
- **Empty-table sweep (`find_empty_tables()`).** Every BIData table currently at 0
  rows — the mid-replication TRUNCATE tell. **Report-only**: it does not change the
  `is_ready()` verdict; callers decide what an empty table means for them.
- The module CLI (`python BIWarehouseStatus.py`) now prints the census empty-sweep
  alongside the tracked-table detail.

### 2. `PLM/PLM_Orchestrator.py` — gate once, at the top (the priority fix)
- `biwarehouse_preflight()` runs `is_ready(required_sources=['BIWarehouse'])` plus
  the empty sweep **once**, at the start of `run_daily()`, **before any write**
  (before `ensure_step_log_table()` and both phases). One check protects the whole
  pipeline; no per-script latency.
- On **not ready**: log loudly, send an **URGENT email to Pat** via the shared
  `PLM_Email` relay (honors the current `REDIRECT_ALL_TO` staging), and **abort
  before any write** — so yesterday's good snapshots survive. Exit code **75**
  (`EX_TEMPFAIL`), mapped to `PMA_AgentStatus` status **`Data-Not-Ready`** so the
  Overseer treats it as an upstream condition, not an orchestrator fault (matches
  the LostSales exit-75 convention).
- `--no-gate` bypasses the preflight for debugging (mirrors `PLM_PODetailLoader`).

## Current adoption inventory

| Consumer | Gate | Notes |
|---|---|---|
| `CRPUtils/BIWarehouseStatus.py` | the shared gate | CRP-010 fail-safe + census added |
| `PLM/PLM_Orchestrator.py` | **NEW** top-level `is_ready(['BIWarehouse'])` | aborts before write, emails on block |
| `PLM/PLM_PODetailLoader.py` | `is_ready(required_sources=['BIWarehouse'])` | self-gates, exit 2, dry-run default — good |
| `LostSales/LS_04_EarlyWarning_Pt03_ComponentCover.py` | `is_ready(required_sources=['BIWarehouse'])` | aborts leaving snapshot intact — **but** import is `try/except → None → proceed ungated` (soft hole, see below) |
| `LostSales/LS_10_SafetyStockMultiplier_Pt02.py` | none direct | relies on an upstream orchestrator preflight + a downstream empty-`cat` guard (exit 75) |

## Still open (recommendations — not changed here)

- **LostSales soft-fail import.** `LS_04 Pt03` sets `bi_is_ready = None` if CRPUtils
  can't import, then proceeds **without** the gate (only a warning). On a machine
  where the import silently fails, the gate is off. Recommend: treat an
  unimportable gate as NOT ready (fail safe), same as the orchestrator now does.
- **Other orchestrators.** `PMA_Orchestrator` and `BDH_Orchestrator` should get the
  same top-of-pipeline gate (the PLM draft `PLM-xxx_ReadinessGateEverywhere_draft.md`
  scopes this). Not touched under CRP-010 — flag for that item.
- **Adoption is thin by design.** The gate was added late; most BIWarehouse readers
  predate it. The high-leverage fix is the orchestrator-level gate (done for PLM),
  not retrofitting every script.

## For further input — "know when ANY table is off its normal size"

The empty sweep answers "is any table at 0 rows" without a baseline. Answering "is
any table **abnormally small** vs normal" needs a stored per-table baseline. Proposed
design (needs your decisions before building):

1. **Baseline store** — a small `CRPAF.dbo.BIWarehouseTableBaseline`
   (`TableName`, `TypicalRows`, `UpdatedAt`), one row per BIData table.
2. **Seed + refresh policy** — seed from a known-good census; refresh `TypicalRows`
   as a rolling median (e.g. last N healthy runs) so it tracks real growth without
   drifting on a bad day. **Decision:** rolling median vs last-known-good vs manual.
3. **Abnormal threshold** — flag `ActualRows < f × TypicalRows` (e.g. f = 0.5) or 0.
   **Decision:** the fraction, and whether "abnormal but non-zero" blocks or only
   alerts.
4. **Alerting** — reuse the orchestrator's `send_not_ready_alert` path; the census
   already returns per-table rows.
5. **Optional SQL-side** — fold the whole census into `vw_BIWarehouseReadiness` with
   per-table floors from the baseline table, so the gate is a single view read.

Open question for you: should abnormal-but-non-zero **block** the pipeline (strict)
or only **email** (advisory)? Blocking is safer but risks false stops on legitimate
swings; advisory keeps the current empty-only gate and layers visibility on top.

## Verification

- `python BIWarehouseStatus.py` — prints tracked-table readiness + the census empty
  sweep (read-only; ~15s for the stability window). Confirmed 2026-08-03: 10 tracked
  tables OK, 41 BIData tables swept, 0 empty.
- `python PLM_Orchestrator.py --daily --dry-run` — preflight is skipped in dry-run
  (planning only). To exercise the gate's abort path, point it at a mid-refresh /
  thresholded-down warehouse and confirm it emails + exits 75 with no writes.
