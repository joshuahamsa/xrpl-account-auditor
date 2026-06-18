# XRPL Account Audit Tool — Design Spec

**Date:** 2026-06-18
**Status:** Approved (design)

## Purpose

Audit a given XRPL account and the network of accounts around it to identify
wallets that are likely controlled by the **same operator**. Motivating use
case: a developer known to run many wallets/handles (dozens to hundreds
activated over years) across multiple projects. The tool crawls the public
XRPL, builds a relationship graph and timeline, and surfaces ranked clusters of
accounts that appear to share an operator — with transparent, explainable
evidence for every link.

Doing this by hand on Bithomp/XRPScan is an intractable time-sink; this tool
automates the crawl and the correlation.

## Non-goals

- Not a real-time monitor. It crawls historical ledger data on demand.
- Not a definitive attribution authority. It produces ranked, evidence-backed
  **hypotheses** with confidence tiers, not legal proof.
- No black-box ML. Clustering is a transparent, rules-based weighted scorer so
  every conclusion can be explained.

## Architecture

Two phases, decoupled by a single SQLite database, so the expensive/slow crawl
is separated from the cheap/iterable analysis.

```
                    ┌─────────────────────────────────────┐
  seed account ───▶ │  PHASE 1: CRAWLER (async BFS)        │
                    │  frontier queue ◀──┐                 │
                    │     │              │ enqueue new     │
                    │     ▼              │ private accts   │
                    │  worker pool (N) ──┘                 │
                    │     │ account_tx (paginated, Clio WS)│
                    │     ▼                                │
                    │  parser ─▶ edges + counterparties    │
                    └─────────┬───────────────────────────┘
                              ▼  writes (idempotent)
                        ┌───────────┐
                        │  SQLite   │  ◀── single source of truth
                        └─────┬─────┘
                              ▼  reads
                    ┌─────────────────────────────────────┐
                    │  PHASE 2: CLUSTER (offline, re-run)  │
                    │  signals ─▶ weighted scoring ─▶      │
                    │  clusters + confidence tiers         │
                    └─────────┬───────────────────────────┘
                              ▼
            Obsidian vault  +  GEXF/DOT  +  queryable SQLite
```

### Why a fixed worker pool, not one-task-per-account

The intuitive model is "spin up a worker for every discovered account." In
practice that means hundreds of simultaneous WebSocket connections to a public
node, which gets the IP rate-limit-banned. Instead: a **fixed pool of N async
workers** (default 5–8, tunable via `--workers`) pulling from a shared frontier
queue. Same parallel-expansion behavior, polite to the node. The crawl is a
concurrent breadth-first search with a persisted frontier + visited set, making
it **resumable** — a multi-hour crawl survives crashes/restarts.

## Phase 1 — Crawler

### Data source

Public XRPL **Clio full-history** node over WebSocket. Default endpoint a
full-history cluster (e.g. `wss://xrplcluster.com`), overridable via `--node`.
On startup the client verifies the node actually serves complete history
(`server_info` complete_ledgers) and warns/fails if not. History is fetched per
account via `account_tx` with `marker` pagination until exhausted.

### Expansion policy (bounding the crawl)

For each counterparty discovered in an account's history, enqueue it for
crawling **only if all** of:

1. Not already visited or queued (dedup via visited set).
2. Within `--max-hops` of the seed (default 4).
3. Under the global `--max-accounts` cap (default 5000) — a hard safety stop.
4. **Not a service leaf.**

**Service-leaf auto-detection:** any account whose distinct-counterparty count
exceeds `--degree-cap` (default 500) is classified a service leaf (exchange,
AMM, marketplace, distributor). Its edges are recorded, but it is **never
crawled through** — this is the mechanism that stops the frontier exploding
into the entire ledger via shared exchange counterparties. Classification is
provisional during the crawl (a counterparty's degree grows as we see more) and
finalized in phase 2.

### Robustness

- WebSocket reconnect with exponential backoff.
- Resume mid-pagination from the per-account stored `marker`.
- Back off on rate-limit / busy responses.
- Idempotent writes: `transactions.tx_hash` is the primary key with
  `INSERT OR IGNORE`; re-running never duplicates.
- Per-account `crawl_status` (`pending` / `done` / `leaf`) drives resume.

## Data model (SQLite)

- **`accounts`** — `address` (PK), `hop_depth`, `activation_parent`,
  `tx_count`, `counterparty_count`, `is_service_leaf`, `crawl_status`,
  `domain`, `last_marker`, `first_seen_ledger`, `last_seen_ledger`.
- **`transactions`** — `tx_hash` (PK), `ledger_index`, `close_time`, `tx_type`,
  `sender`, `destination`, `amount`, `currency`, `issuer`, `fee`, `result`,
  `raw_json`.
- **`edges`** — derived graph: `src`, `dst`, `edge_type`
  (`activation` | `payment` | `nft_transfer` | `nft_sale` | `regular_key` |
  `signer_list` | `trustline`), `tx_hash`, `ledger_index`, `metadata` (JSON).
- **`signals`** — phase-2 computed signals (per account-pair and per-account
  fingerprints), with signal type, value, and contributing weight.
- **`clusters`** — `cluster_id`, `member` accounts, `confidence_tier`,
  `evidence` (JSON breakdown of which signals fired).
- **`crawl_state`** — frontier contents + run metadata for resumability.

## Phase 2 — Clustering (transparent, rules-based)

Computes signals over the stored data, scores candidate same-operator
account-pairs, and groups them into clusters. Every link records its evidence
so any cluster is fully explainable.

| Signal | Strength | Computation |
|---|---|---|
| Key/signer overlap | **Strong** | Shared `SetRegularKey` target, `SignerList` membership, or the same regular key set across multiple accounts. Near-certain shared control. |
| Activation / funding chains | **Strong** | `activation_parent` edges + direct XRP/token self-transfers between private (non-leaf) accounts. Operators funding their own new wallets. |
| Shared counterparties + NFT flow | Medium | Jaccard similarity of private-counterparty sets; NFT mint→transfer→sale provenance chains linking accounts. |
| Behavioral fingerprints | Weak / corroborating | Activity hour-of-day histogram (timezone), `Domain` field reuse, memo / destination-tag pattern reuse, fee-setting habits. |

**Scoring & clustering:**

- Strong signals form high-weight edges; cluster via union-find on strong edges.
- Medium/weak signals adjust pair confidence and attach looser members.
- Output **confidence tiers**: `Confirmed` / `Likely` / `Possible`.
- Weights are configurable (`--weights`) so the model can be tuned without code
  changes. Re-runnable over the same DB.

## Outputs

### Obsidian vault (primary)

`audit report --format obsidian --vault <dir>`:

- **One note per account** (`<address>.md`): YAML frontmatter (address,
  hop_depth, tx/counterparty counts, cluster, confidence, activation_parent,
  is_service_leaf); body grouped by edge type (`## Activations`,
  `## Payments`, `## NFT transfers`) with `[[wiki-links]]` to connected
  accounts. Obsidian's graph view renders the relationship network natively.
- Service-leaf accounts tagged `#service-leaf` so the graph view can hide /
  de-emphasize exchange hubs on demand (one filter toggle).
- **Cluster hub notes**: one per detected cluster, listing members as
  `[[links]]`, confidence tier, and the evidence breakdown.
- Tags for filtering: `#cluster/<id>`, `#confidence/confirmed|likely|possible`,
  `#service-leaf`.
- **Dataview-friendly** frontmatter for ad-hoc queries with the Dataview plugin.
- Ranked report rendered as an entry note linking into cluster/account notes.
- **Regenerable**: tool-generated notes live in a known subfolder and are
  overwritten idempotently; user-authored notes elsewhere in the vault are
  never touched.

### GEXF / Graphviz DOT (secondary)

`audit report --format gexf|dot`: full graph export for Gephi — force-directed
layouts and centrality metrics on large graphs that Obsidian's graph view isn't
built for. Nodes colored by cluster, edges typed.

### Queryable SQLite (foundation)

Clean schema plus canned report views; the vault and graph exports both derive
from it. Direct SQL for ad-hoc investigation.

## CLI

```
audit crawl <seed_addr> [--workers N] [--max-hops 4] [--degree-cap 500] \
                        [--max-accounts 5000] [--node wss://...] [--resume]
audit cluster [--weights <file>]                 # re-runnable phase 2
audit report  --format obsidian|gexf|dot [--vault <dir>] [--min-confidence ...]
audit status                                     # crawl progress / DB stats
```

## Stack

- `xrpl-py` — async WebSocket client + `account_tx`.
- `networkx` — graph construction, clustering helpers, GEXF/DOT export.
- `click` — CLI.
- stdlib `sqlite3` — storage.

Dependencies kept deliberately minimal.

## Project structure

```
xrpl_audit/
  __init__.py
  cli.py            # click entrypoints
  ledger_client.py  # Clio WS client, account_tx pagination, retry/backoff
  crawler.py        # async BFS, worker pool, frontier, expansion policy
  parser.py         # tx JSON -> normalized records + typed edges
  storage.py        # SQLite schema + DAO (idempotent writes)
  signals.py        # phase-2 signal computation
  cluster.py        # weighted scoring + union-find clustering
  report.py         # Obsidian vault + GEXF/DOT export + canned views
tests/
  fixtures/         # recorded account_tx JSON
  test_parser.py
  test_signals.py
  test_cluster.py
  test_crawler.py   # against mocked responses (no live node)
pyproject.toml
README.md
```

## Testing strategy (TDD)

- **Parser**: recorded `account_tx` JSON fixtures → assert extracted typed
  edges and normalized records. Highest-value tests; written first.
- **Signals**: each signal computation tested with synthetic fixtures of known
  structure (e.g. two accounts sharing a regular key → strong-link signal).
- **Expansion policy / service-leaf detection**: synthetic degree scenarios.
- **Clustering scorer**: synthetic signal sets → assert expected clusters and
  confidence tiers.
- **Crawler**: integration test against mocked node responses — no live node in
  the suite. An optional live smoke test is gated behind an env flag.

## Implementation staging

The full scope (4 signals, 3 output formats) is built in stages so a working
crawl + DB lands early and real data can be inspected before clustering is
complete:

1. **Foundation** — `ledger_client`, `parser`, `storage`, `crawler` with
   expansion policy → working resumable crawl writing to SQLite + `audit
   status`.
2. **Clustering** — signals strong→weak (`signals`, `cluster`) + `audit
   cluster`.
3. **Outputs** — Obsidian vault, then GEXF/DOT, then canned SQL views
   (`report`).

Each stage is independently testable and useful.
