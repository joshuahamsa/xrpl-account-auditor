# xrpl-audit

Crawl an XRPL account's relationship graph and cluster wallets likely run by the same operator.

## Install
    pip install -e .

## Use
    audit --db case.db crawl rSEED_ADDRESS --max-hops 4 --degree-cap 500 --workers 6
    audit --db case.db cluster
    audit --db case.db report --format obsidian --vault ./case-vault
    audit --db case.db report --format gexf --out case.gexf
    audit --db case.db status

Open `./case-vault` in Obsidian; use the graph view and toggle the `#service-leaf`
tag group off to focus on the operator network. Confidence tiers: confirmed / likely / possible.

## Live smoke test
    XRPL_AUDIT_LIVE=1 pytest tests/ -k live
