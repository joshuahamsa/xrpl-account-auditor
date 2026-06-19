# xrpl-audit

Crawl an XRPL account's relationship graph and cluster wallets likely run by the same operator.

## What this is

`xrpl-audit` is a forensic tool for investigating activity on the XRP Ledger.
You give it one **seed account** — say, an address tied to a scam, a hack, an
exchange withdrawal, or a counterparty you're doing diligence on — and it maps
the surrounding network of accounts and the money flowing between them, then
makes an educated guess about **which of those accounts are actually controlled
by the same person or entity**.

The output is an investigator's case file: a browseable [Obsidian](https://obsidian.md)
vault (one linked note per account and per cluster, with the evidence behind
every link), plus graph exports for Gephi (`.gexf`) or Graphviz (`.dot`).

## Why it exists

The XRP Ledger is fully public, but that transparency is deceptive. A single
real-world actor rarely uses a single account. They spread funds across dozens
or hundreds of wallets — to obscure how much they hold, to launder proceeds, to
sidestep limits, or simply to operate at scale. On-chain, those wallets look
independent. The connective tissue (who funded whom, who shares signing keys,
who moves money back and forth) is real but buried under millions of
transactions.

Doing this by hand is hopeless: each account can have tens of thousands of
transactions, each transaction points at more accounts, and the graph fans out
exponentially. `xrpl-audit` automates two things humans can't do at that scale:

1. **Discovery** — systematically walk the graph outward from the seed and
   pull every account and transaction into a local database.
2. **Attribution** — score the relationships between accounts and group the
   ones whose behavior betrays a shared operator.

## How it works

It runs in two phases over a local SQLite database.

### Phase 1 — Crawl (`audit crawl`)

A breadth-first walk of the ledger starting at the seed. For each account it
fetches the **full transaction history** (paginated `account_tx`), stores the
raw transactions, and records typed **edges** between accounts — payments,
account *activations* (funding a brand-new wallet), trustlines, NFT transfers,
and so on. It then enqueues the newly-discovered counterparties and repeats.

Two bounds keep the crawl finite and focused:

- **`--max-hops`** — how many steps out from the seed to walk (depth).
- **`--degree-cap`** — an account with more counterparties than this is flagged
  a **service leaf** (an exchange, marketplace, or other hub) and recorded but
  *not* expanded. Hubs connect everyone to everyone; following them would drown
  the operator's own network in noise.

The crawl is **resumable** (`--resume`) and stores per-page markers, so it can
be stopped and continued without re-fetching. Public nodes aggressively
rate-limit and IP-ban under load, so the crawler is built to survive that: a
global request **rate limiter** (`--rate`), patient backoff and reconnect on
throttling, per-request timeouts to outlast silent hangs, and `--retry-errors`
to re-queue accounts that a transient ban knocked out. It defaults to a
full-history node (`wss://s2.ripple.com`).

### Phase 2 — Cluster (`audit cluster`)

Once the graph is crawled, clustering scores every **pair** of accounts on
independent **signals**, each contributing weighted evidence:

| Signal | Strength | What it suggests |
|--------|----------|------------------|
| `regular_key` / `shared_regular_key` | 1.0 | Accounts set, or share, the same signing key — near-proof of common control |
| `signer_list` | 0.9 | Shared multisig signer configuration |
| `activation` | 0.7 | One account funded/created the other |
| `self_transfer` | 0.6 | Direct transfers between two non-hub accounts — an operator shuffling their own funds |
| `domain_reuse` | 0.5 | Same `Domain` field set on-chain |
| `counterparty_jaccard` | ≤0.5 | Strong overlap in who they each transact with |
| `nft_flow` | 0.4 | NFTs moved directly between them |
| `active_hours` | 0.2 | Similar daily activity-time fingerprint |

Pair scores are summed and bucketed into **confidence tiers** — `confirmed`
(≥1.0), `likely` (≥0.6), `possible` (≥0.3) — and connected accounts are merged
into **clusters**. A cluster is the tool's hypothesis: *"these wallets are
probably the same hand, and here's why."* Weights are tunable via
`--weights weights.json`.

> Terms at a glance: a **cluster** (e.g. `cluster-13`) is a *group of accounts*
> produced in Phase 2, **not** something the crawler works on. The crawl deals
> in **accounts**; `status` showing thousands of `pending` just means that many
> accounts are still queued to be fetched. Clusters only exist after you run
> `audit cluster`.

## Setup (uv)

The project is managed with [uv](https://docs.astral.sh/uv/) and pinned to Python
3.11 (see `.python-version`). One command builds the virtualenv, installs all
deps from the lockfile, and installs the package itself (editable):

    uv sync

That's it — no system Python or pip involved.

## Run

Use `uv run`, which executes inside the project venv:

    uv run audit --db case.db crawl rSEED_ADDRESS --max-hops 4 --degree-cap 500 --workers 4 --rate 12
    uv run audit --db case.db cluster
    uv run audit --db case.db report --format obsidian --vault ./case-vault
    uv run audit --db case.db report --format gexf --out case.gexf
    uv run audit --db case.db status

The crawl prints per-account progress to stderr by default
(`[crawl]  42 done |  137 queued | hop 2 | rXXX (1234 tx)`); pass `--quiet`/`-q`
to silence it. Progress goes to stderr, so the final JSON summary on stdout
stays clean for piping.

Key crawl flags:

- `--node` — full-history XRPL websocket node (default `wss://s2.ripple.com`).
- `--rate` — max requests/sec across all workers (default 12). Public nodes ban
  by IP under load; keep this modest. `0` disables the throttle.
- `--workers` — concurrent fetchers (default 4). More workers ≠ faster if
  `--rate` is the bottleneck; it mainly helps overlap network latency.
- `--resume` — continue an interrupted crawl from where it left off.
- `--retry-errors` — also re-queue accounts left in `error` (e.g. knocked out by
  a transient ban) and crawl them again. Combine with `--resume`.

A crawl can be safely stopped (Ctrl-C) and resumed:

    uv run audit --db case.db crawl rSEED_ADDRESS --resume --retry-errors

Prefer a bare `audit` command? Either activate the venv (`source .venv/bin/activate`,
then `audit ...`) or install it as a global uv tool: `uv tool install --editable .`.

Open `./case-vault` in Obsidian; use the graph view and toggle the `#service-leaf`
tag group off to focus on the operator network. Confidence tiers: confirmed / likely / possible.

## Tests

The suite runs fully offline (no live node) against an injected fake ledger:

    uv run pytest -q

A gated live smoke test against a real Clio node is not included yet; add one
behind an `XRPL_AUDIT_LIVE=1` guard if you want end-to-end coverage.

## Without uv

If you'd rather not use uv, the CLI also runs straight from a checkout with no
install step (needs `xrpl-py`, `networkx`, `click` on your Python 3.10+):

    python -m xrpl_audit --db case.db status
