# xrpl-audit

Crawl an XRPL account's relationship graph and cluster wallets likely run by the same operator.

## Setup (uv)

The project is managed with [uv](https://docs.astral.sh/uv/) and pinned to Python
3.11 (see `.python-version`). One command builds the virtualenv, installs all
deps from the lockfile, and installs the package itself (editable):

    uv sync

That's it — no system Python or pip involved.

## Run

Use `uv run`, which executes inside the project venv:

    uv run audit --db case.db crawl rSEED_ADDRESS --max-hops 4 --degree-cap 500 --workers 6
    uv run audit --db case.db cluster
    uv run audit --db case.db report --format obsidian --vault ./case-vault
    uv run audit --db case.db report --format gexf --out case.gexf
    uv run audit --db case.db status

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
