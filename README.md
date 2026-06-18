# xrpl-audit

Crawl an XRPL account's relationship graph and cluster wallets likely run by the same operator.

## Requirements
Python 3.10+, and the runtime deps: `xrpl-py`, `networkx`, `click`.
(Dev/testing also needs `pytest` and `pytest-asyncio`.)

## Run (no install needed)
The CLI runs straight from a checkout — no build step:

    python -m xrpl_audit --db case.db crawl rSEED_ADDRESS --max-hops 4 --degree-cap 500 --workers 6
    python -m xrpl_audit --db case.db cluster
    python -m xrpl_audit --db case.db report --format obsidian --vault ./case-vault
    python -m xrpl_audit --db case.db report --format gexf --out case.gexf
    python -m xrpl_audit --db case.db status

## Optional: install the `audit` command
To get a top-level `audit` shell command, install editably. On older pip (build
isolation can't reach PyPI), bypass isolation so it uses your system setuptools:

    pip install --user --no-build-isolation -e .
    audit --db case.db status   # then `audit ...` works anywhere

(A plain `pip install -e .` works too if your pip/setuptools support PEP 660 and
can fetch build deps; the `--no-build-isolation` form above avoids both needs.)

Open `./case-vault` in Obsidian; use the graph view and toggle the `#service-leaf`
tag group off to focus on the operator network. Confidence tiers: confirmed / likely / possible.

## Tests
The suite runs fully offline (no live node) against an injected fake ledger:

    python -m pytest -q

A gated live smoke test against a real Clio node is not included yet; add one
behind an `XRPL_AUDIT_LIVE=1` guard if you want end-to-end coverage.
