# XRPL Account Audit Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python tool that crawls the XRPL from a seed account, stores the transaction/relationship graph in SQLite, then clusters wallets likely controlled by the same operator and exports the results to an Obsidian vault (plus Gephi graph files and queryable SQL).

**Architecture:** Two phases decoupled by SQLite. Phase 1 is a resumable async breadth-first crawler with a fixed worker pool that auto-detects high-degree "service" accounts and refuses to expand through them. Phase 2 reads the DB, computes four tiers of "same operator" signals, and groups accounts into confidence-tiered clusters. Outputs derive from the DB.

**Tech Stack:** Python 3.11+, `xrpl-py` (async WebSocket + `account_tx`), `networkx` (graph + GEXF/DOT export), `click` (CLI), stdlib `sqlite3` and `asyncio`, `pytest` + `pytest-asyncio` for tests.

## Global Constraints

- Python **3.11+** (uses `tomllib`-era stdlib, `asyncio.TaskGroup`, modern typing).
- Dependencies limited to: `xrpl-py`, `networkx`, `click` (runtime); `pytest`, `pytest-asyncio` (dev). No others without a spec change.
- **No live node access in the test suite.** All crawler/client tests run against an injected fake source. A live smoke test is gated behind the `XRPL_AUDIT_LIVE=1` env var.
- All DB writes are **idempotent**: `transactions` keyed by `tx_hash`, `edges` deduped by `(src, dst, edge_type, tx_hash)`, accounts upserted.
- Clustering is **rules-based and explainable** — no ML. Every cluster carries an evidence breakdown.
- Confidence tiers are exactly: `confirmed`, `likely`, `possible`.
- Edge types are exactly: `activation`, `payment`, `nft_transfer`, `nft_sale`, `regular_key`, `signer_list`, `trustline`.
- Commit after every task (after its tests pass).

---

## File Structure

```
xrpl_audit/
  __init__.py        # version
  models.py          # shared dataclasses: ParsedTx, Edge, PairSignal, Cluster
  storage.py         # SQLite schema + Store DAO (idempotent)
  parser.py          # account_tx entry -> ParsedTx (+ typed edges)
  ledger_client.py   # LedgerSource protocol, LedgerClient (Clio WS), retry/backoff
  crawler.py         # async BFS, worker pool, frontier, expansion policy
  signals.py         # phase-2 signal computation (4 functions)
  cluster.py         # weighted scoring + union-find clustering + tiers
  report.py          # Obsidian vault + GEXF/DOT export + SQL views
  cli.py             # click entrypoints: crawl, cluster, report, status
tests/
  conftest.py        # shared fixtures (in-memory Store, FakeLedger)
  fixtures/          # recorded/synthetic account_tx JSON
  test_storage.py
  test_parser.py
  test_ledger_client.py
  test_crawler.py
  test_signals.py
  test_cluster.py
  test_report.py
pyproject.toml
README.md
```

---

## STAGE 1 — FOUNDATION (crawl → SQLite)

### Task 1: Project scaffold + shared models

**Files:**
- Create: `pyproject.toml`, `xrpl_audit/__init__.py`, `xrpl_audit/models.py`
- Test: `tests/test_models.py`

**Interfaces:**
- Produces: `ParsedTx`, `Edge`, `PairSignal`, `Cluster` dataclasses imported everywhere.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "xrpl-audit"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["xrpl-py>=2.5", "networkx>=3.0", "click>=8.1"]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23"]

[project.scripts]
audit = "xrpl_audit.cli:cli"

[tool.pytest.ini_options]
asyncio_mode = "auto"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 2: Write `xrpl_audit/__init__.py`**

```python
__version__ = "0.1.0"
```

- [ ] **Step 3: Write the failing test for models**

```python
# tests/test_models.py
from xrpl_audit.models import ParsedTx, Edge, PairSignal, Cluster

def test_edge_is_hashable_and_equal():
    e1 = Edge(src="rA", dst="rB", edge_type="payment", metadata={})
    e2 = Edge(src="rA", dst="rB", edge_type="payment", metadata={})
    assert e1 == e2

def test_parsedtx_holds_edges():
    tx = ParsedTx(tx_hash="H", ledger_index=1, close_time=0, tx_type="Payment",
                  sender="rA", destination="rB", amount="10", currency="XRP",
                  issuer=None, fee="12", result="tesSUCCESS", edges=[])
    assert tx.tx_hash == "H" and tx.edges == []

def test_paircluster_defaults():
    ps = PairSignal(a="rA", b="rB", signal_type="regular_key", strength=1.0, detail={})
    cl = Cluster(id=1, members={"rA", "rB"}, tier="confirmed", evidence=[ps])
    assert "rA" in cl.members and cl.tier == "confirmed"
```

- [ ] **Step 4: Run test to verify it fails**

Run: `pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.models'`

- [ ] **Step 5: Write `xrpl_audit/models.py`**

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    edge_type: str          # activation|payment|nft_transfer|nft_sale|regular_key|signer_list|trustline
    metadata: dict = field(default_factory=dict, compare=False)

@dataclass
class ParsedTx:
    tx_hash: str
    ledger_index: int
    close_time: int          # ripple epoch seconds
    tx_type: str
    sender: str
    destination: str | None
    amount: str | None       # drops for XRP, value for IOU; None if N/A
    currency: str | None     # "XRP" or 3/40-char currency code
    issuer: str | None
    fee: str
    result: str
    edges: list[Edge] = field(default_factory=list)

@dataclass
class PairSignal:
    a: str
    b: str
    signal_type: str         # e.g. regular_key, shared_regular_key, activation, self_transfer, counterparty_jaccard, nft_flow, active_hours, domain_reuse
    strength: float          # weight contribution (configurable per type)
    detail: dict = field(default_factory=dict)

@dataclass
class Cluster:
    id: int
    members: set
    tier: str                # confirmed|likely|possible
    evidence: list           # list[PairSignal]
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/test_models.py -v`
Expected: PASS (3 passed)

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml xrpl_audit/__init__.py xrpl_audit/models.py tests/test_models.py
git commit -m "feat: project scaffold and shared models"
```

---

### Task 2: Storage layer (SQLite schema + idempotent DAO)

**Files:**
- Create: `xrpl_audit/storage.py`, `tests/conftest.py`
- Test: `tests/test_storage.py`

**Interfaces:**
- Consumes: `Edge`, `ParsedTx` from `models`.
- Produces: `Store` class with methods:
  - `Store(path: str)` — `":memory:"` allowed.
  - `init_schema() -> None`
  - `upsert_account(address: str, **fields) -> None` (fields among: hop_depth, activation_parent, tx_count, counterparty_count, is_service_leaf, crawl_status, domain, last_marker, first_seen_ledger, last_seen_ledger)
  - `get_account(address: str) -> dict | None`
  - `insert_transaction(tx: ParsedTx) -> None` (idempotent)
  - `insert_edge(edge: Edge, tx_hash: str, ledger_index: int) -> None` (idempotent)
  - `record_counterparty(address: str, counterparty: str) -> None` (tracks distinct counterparties; updates counterparty_count)
  - `set_crawl_status(address, status)` / `set_marker(address, marker)`
  - `iter_accounts() -> Iterator[dict]`, `iter_edges() -> Iterator[dict]`, `iter_transactions() -> Iterator[dict]`
  - `pending_accounts() -> list[str]` (crawl_status == 'pending')
  - `counts() -> dict` (accounts, transactions, edges, by crawl_status)

- [ ] **Step 1: Write `tests/conftest.py` with a Store fixture**

```python
import pytest
from xrpl_audit.storage import Store

@pytest.fixture
def store():
    s = Store(":memory:")
    s.init_schema()
    return s
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_storage.py
from xrpl_audit.models import ParsedTx, Edge

def _tx(h="H1", sender="rA", dst="rB"):
    return ParsedTx(tx_hash=h, ledger_index=5, close_time=100, tx_type="Payment",
                    sender=sender, destination=dst, amount="10", currency="XRP",
                    issuer=None, fee="12", result="tesSUCCESS", edges=[])

def test_upsert_and_get_account(store):
    store.upsert_account("rA", hop_depth=0, crawl_status="pending")
    store.upsert_account("rA", tx_count=3)            # partial update keeps hop_depth
    a = store.get_account("rA")
    assert a["hop_depth"] == 0 and a["tx_count"] == 3 and a["crawl_status"] == "pending"

def test_insert_transaction_idempotent(store):
    store.insert_transaction(_tx())
    store.insert_transaction(_tx())                   # same hash, ignored
    assert store.counts()["transactions"] == 1

def test_insert_edge_idempotent(store):
    e = Edge(src="rA", dst="rB", edge_type="payment")
    store.insert_edge(e, tx_hash="H1", ledger_index=5)
    store.insert_edge(e, tx_hash="H1", ledger_index=5)
    assert store.counts()["edges"] == 1

def test_counterparty_count(store):
    store.upsert_account("rA", crawl_status="pending")
    store.record_counterparty("rA", "rB")
    store.record_counterparty("rA", "rC")
    store.record_counterparty("rA", "rB")             # duplicate, no double count
    assert store.get_account("rA")["counterparty_count"] == 2

def test_pending_accounts(store):
    store.upsert_account("rA", crawl_status="pending")
    store.upsert_account("rB", crawl_status="done")
    assert store.pending_accounts() == ["rA"]
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_storage.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.storage'`

- [ ] **Step 4: Write `xrpl_audit/storage.py`**

```python
import sqlite3
from collections.abc import Iterator
from .models import Edge, ParsedTx

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
  address TEXT PRIMARY KEY,
  hop_depth INTEGER,
  activation_parent TEXT,
  tx_count INTEGER DEFAULT 0,
  counterparty_count INTEGER DEFAULT 0,
  is_service_leaf INTEGER DEFAULT 0,
  crawl_status TEXT DEFAULT 'pending',
  domain TEXT,
  last_marker TEXT,
  first_seen_ledger INTEGER,
  last_seen_ledger INTEGER
);
CREATE TABLE IF NOT EXISTS transactions (
  tx_hash TEXT PRIMARY KEY,
  ledger_index INTEGER,
  close_time INTEGER,
  tx_type TEXT,
  sender TEXT,
  destination TEXT,
  amount TEXT,
  currency TEXT,
  issuer TEXT,
  fee TEXT,
  result TEXT,
  raw_json TEXT
);
CREATE TABLE IF NOT EXISTS edges (
  src TEXT, dst TEXT, edge_type TEXT, tx_hash TEXT,
  ledger_index INTEGER, metadata TEXT,
  PRIMARY KEY (src, dst, edge_type, tx_hash)
);
CREATE TABLE IF NOT EXISTS counterparties (
  address TEXT, counterparty TEXT,
  PRIMARY KEY (address, counterparty)
);
CREATE TABLE IF NOT EXISTS signals (
  a TEXT, b TEXT, signal_type TEXT, strength REAL, detail TEXT
);
CREATE TABLE IF NOT EXISTS clusters (
  cluster_id INTEGER, member TEXT, tier TEXT, evidence TEXT
);
CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender);
CREATE INDEX IF NOT EXISTS idx_tx_dest ON transactions(destination);
CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src);
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(edge_type);
"""

_ACCOUNT_COLS = {"hop_depth", "activation_parent", "tx_count", "counterparty_count",
                 "is_service_leaf", "crawl_status", "domain", "last_marker",
                 "first_seen_ledger", "last_seen_ledger"}

import json

class Store:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")

    def init_schema(self) -> None:
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def upsert_account(self, address: str, **fields) -> None:
        bad = set(fields) - _ACCOUNT_COLS
        if bad:
            raise ValueError(f"unknown account fields: {bad}")
        self.conn.execute("INSERT OR IGNORE INTO accounts(address) VALUES (?)", (address,))
        if fields:
            sets = ", ".join(f"{k}=?" for k in fields)
            self.conn.execute(f"UPDATE accounts SET {sets} WHERE address=?",
                              (*fields.values(), address))
        self.conn.commit()

    def get_account(self, address: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM accounts WHERE address=?", (address,)).fetchone()
        return dict(row) if row else None

    def insert_transaction(self, tx: ParsedTx, raw_json: str | None = None) -> None:
        self.conn.execute(
            """INSERT OR IGNORE INTO transactions
               (tx_hash, ledger_index, close_time, tx_type, sender, destination,
                amount, currency, issuer, fee, result, raw_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (tx.tx_hash, tx.ledger_index, tx.close_time, tx.tx_type, tx.sender,
             tx.destination, tx.amount, tx.currency, tx.issuer, tx.fee, tx.result, raw_json))
        self.conn.commit()

    def insert_edge(self, edge: Edge, tx_hash: str, ledger_index: int) -> None:
        self.conn.execute(
            """INSERT OR IGNORE INTO edges(src, dst, edge_type, tx_hash, ledger_index, metadata)
               VALUES (?,?,?,?,?,?)""",
            (edge.src, edge.dst, edge.edge_type, tx_hash, ledger_index, json.dumps(edge.metadata)))
        self.conn.commit()

    def record_counterparty(self, address: str, counterparty: str) -> None:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO counterparties(address, counterparty) VALUES (?,?)",
            (address, counterparty))
        if cur.rowcount:
            self.conn.execute(
                "UPDATE accounts SET counterparty_count = counterparty_count + 1 WHERE address=?",
                (address,))
        self.conn.commit()

    def set_crawl_status(self, address: str, status: str) -> None:
        self.upsert_account(address, crawl_status=status)

    def set_marker(self, address: str, marker: str | None) -> None:
        self.upsert_account(address, last_marker=marker)

    def pending_accounts(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT address FROM accounts WHERE crawl_status='pending' ORDER BY hop_depth").fetchall()
        return [r["address"] for r in rows]

    def iter_accounts(self) -> Iterator[dict]:
        for r in self.conn.execute("SELECT * FROM accounts"):
            yield dict(r)

    def iter_edges(self) -> Iterator[dict]:
        for r in self.conn.execute("SELECT * FROM edges"):
            yield dict(r)

    def iter_transactions(self) -> Iterator[dict]:
        for r in self.conn.execute("SELECT * FROM transactions"):
            yield dict(r)

    def counts(self) -> dict:
        c = self.conn.execute
        out = {
            "accounts": c("SELECT COUNT(*) FROM accounts").fetchone()[0],
            "transactions": c("SELECT COUNT(*) FROM transactions").fetchone()[0],
            "edges": c("SELECT COUNT(*) FROM edges").fetchone()[0],
        }
        for r in c("SELECT crawl_status, COUNT(*) n FROM accounts GROUP BY crawl_status"):
            out[f"status_{r['crawl_status']}"] = r["n"]
        return out
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_storage.py -v`
Expected: PASS (5 passed)

- [ ] **Step 6: Commit**

```bash
git add xrpl_audit/storage.py tests/conftest.py tests/test_storage.py
git commit -m "feat: SQLite storage layer with idempotent writes"
```

---

### Task 3: Transaction parser

**Files:**
- Create: `xrpl_audit/parser.py`, `tests/fixtures/account_tx_sample.json`
- Test: `tests/test_parser.py`

**Interfaces:**
- Consumes: `ParsedTx`, `Edge` from `models`.
- Produces: `parse_transaction(entry: dict) -> ParsedTx` where `entry` is one element of `account_tx`'s `transactions` array, shaped `{"tx": {...}, "meta": {...}}` (the xrpl-py / rippled shape). Also `extract_edges(tx: dict, meta: dict) -> list[Edge]`.

Edge derivation rules:
- `Payment` where `meta` shows the destination account was newly created (its `CreatedNode` of type `AccountRoot`) → `activation` edge sender→dest, plus a `payment` edge.
- `Payment` (normal) → `payment` edge sender→destination.
- `SetRegularKey` with `RegularKey` set → `regular_key` edge account→RegularKey.
- `SignerListSet` → one `signer_list` edge account→each signer entry.
- `NFTokenAcceptOffer` / `NFTokenCreateOffer` resulting in transfer → `nft_transfer` (and `nft_sale` if an amount changed hands). For this task, derive `nft_transfer` from `NFTokenAcceptOffer` between the two parties; `nft_sale` when the accepted offer `Amount` > 0.
- `TrustSet` → `trustline` edge account→issuer.

- [ ] **Step 1: Create a fixture file**

```json
// tests/fixtures/account_tx_sample.json
{
  "activation": {
    "tx": {"TransactionType": "Payment", "Account": "rFunder", "Destination": "rNewbie",
           "Amount": "20000000", "Fee": "12", "hash": "ACT1"},
    "meta": {"TransactionResult": "tesSUCCESS",
      "AffectedNodes": [{"CreatedNode": {"LedgerEntryType": "AccountRoot",
        "NewFields": {"Account": "rNewbie"}}}]},
    "ledger_index": 100, "close_time_iso": "ignore", "tx_close_time": 700000000
  },
  "regular_key": {
    "tx": {"TransactionType": "SetRegularKey", "Account": "rMaster", "RegularKey": "rRegKey",
           "Fee": "12", "hash": "RK1"},
    "meta": {"TransactionResult": "tesSUCCESS", "AffectedNodes": []},
    "ledger_index": 101, "tx_close_time": 700000100
  }
}
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_parser.py
import json, pathlib
from xrpl_audit.parser import parse_transaction
from xrpl_audit.models import Edge

FIX = json.loads((pathlib.Path(__file__).parent / "fixtures/account_tx_sample.json").read_text())

def test_activation_payment_produces_activation_and_payment_edges():
    p = parse_transaction(FIX["activation"])
    assert p.tx_hash == "ACT1" and p.sender == "rFunder" and p.destination == "rNewbie"
    types = {e.edge_type for e in p.edges}
    assert "activation" in types and "payment" in types
    act = next(e for e in p.edges if e.edge_type == "activation")
    assert act.src == "rFunder" and act.dst == "rNewbie"

def test_regular_key_edge():
    p = parse_transaction(FIX["regular_key"])
    assert p.tx_type == "SetRegularKey"
    assert Edge("rMaster", "rRegKey", "regular_key") in p.edges
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.parser'`

- [ ] **Step 4: Write `xrpl_audit/parser.py`**

```python
from .models import ParsedTx, Edge

def _created_accounts(meta: dict) -> set[str]:
    out = set()
    for node in meta.get("AffectedNodes", []):
        cn = node.get("CreatedNode")
        if cn and cn.get("LedgerEntryType") == "AccountRoot":
            acct = cn.get("NewFields", {}).get("Account")
            if acct:
                out.add(acct)
    return out

def _amount_fields(amount) -> tuple[str | None, str | None, str | None]:
    """Return (value, currency, issuer)."""
    if amount is None:
        return None, None, None
    if isinstance(amount, str):           # XRP drops
        return amount, "XRP", None
    return amount.get("value"), amount.get("currency"), amount.get("issuer")

def extract_edges(tx: dict, meta: dict) -> list[Edge]:
    t = tx.get("TransactionType")
    acct = tx.get("Account")
    edges: list[Edge] = []
    if t == "Payment":
        dest = tx.get("Destination")
        if dest:
            if dest in _created_accounts(meta):
                edges.append(Edge(acct, dest, "activation"))
            edges.append(Edge(acct, dest, "payment"))
    elif t == "SetRegularKey":
        rk = tx.get("RegularKey")
        if rk:
            edges.append(Edge(acct, rk, "regular_key"))
    elif t == "SignerListSet":
        for entry in tx.get("SignerEntries", []):
            signer = entry.get("SignerEntry", {}).get("Account")
            if signer:
                edges.append(Edge(acct, signer, "signer_list"))
    elif t == "NFTokenAcceptOffer":
        # Counterparty is whoever owned the offer; resolved from meta in a later pass,
        # but the simple case records a transfer between Account and the offer owner if present.
        owner = tx.get("_offer_owner")  # populated by enrichment; safe-None otherwise
        if owner:
            edges.append(Edge(acct, owner, "nft_transfer"))
            val, *_ = _amount_fields(tx.get("Amount"))
            if val and val != "0":
                edges.append(Edge(acct, owner, "nft_sale"))
    elif t == "TrustSet":
        la = tx.get("LimitAmount", {})
        issuer = la.get("issuer") if isinstance(la, dict) else None
        if issuer:
            edges.append(Edge(acct, issuer, "trustline"))
    return edges

def parse_transaction(entry: dict) -> ParsedTx:
    tx = entry.get("tx") or entry.get("tx_json") or {}
    meta = entry.get("meta") or entry.get("metaData") or {}
    val, ccy, issuer = _amount_fields(tx.get("Amount"))
    return ParsedTx(
        tx_hash=tx.get("hash") or entry.get("hash"),
        ledger_index=entry.get("ledger_index") or tx.get("ledger_index"),
        close_time=entry.get("tx_close_time") or tx.get("date") or 0,
        tx_type=tx.get("TransactionType"),
        sender=tx.get("Account"),
        destination=tx.get("Destination"),
        amount=val,
        currency=ccy,
        issuer=issuer,
        fee=tx.get("Fee"),
        result=meta.get("TransactionResult"),
        edges=extract_edges(tx, meta),
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_parser.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Commit**

```bash
git add xrpl_audit/parser.py tests/test_parser.py tests/fixtures/account_tx_sample.json
git commit -m "feat: transaction parser with typed edge extraction"
```

---

### Task 4: Ledger client (Clio WS) with injectable source

**Files:**
- Create: `xrpl_audit/ledger_client.py`
- Test: `tests/test_ledger_client.py`

**Interfaces:**
- Produces:
  - `class LedgerSource(Protocol)` with `async def account_tx(self, address: str, marker=None, limit: int = 200) -> tuple[list[dict], dict | None]` returning `(transactions, next_marker)`.
  - `class LedgerClient(LedgerSource)` — real impl wrapping `xrpl.asyncio.clients.AsyncWebsocketClient`, constructed as `LedgerClient(url: str)`; reconnect + backoff via `max_retries`/`backoff_base`.
  - `class FakeLedger(LedgerSource)` (in test/conftest) returning canned pages — defined in this task's test file then promoted to `conftest.py`.

- [ ] **Step 1: Write failing test using a fake source contract**

```python
# tests/test_ledger_client.py
import pytest
from xrpl_audit.ledger_client import paginate_all

class _FakeRaw:
    """Simulates raw account_tx responses with markers."""
    def __init__(self, pages): self.pages = pages; self.calls = 0
    async def fetch(self, address, marker, limit):
        page = self.pages[self.calls]; self.calls += 1
        return page  # {"transactions": [...], "marker": ... or None}

@pytest.mark.asyncio
async def test_paginate_all_follows_markers():
    raw = _FakeRaw([
        {"transactions": [{"tx": {"hash": "A"}}], "marker": {"m": 1}},
        {"transactions": [{"tx": {"hash": "B"}}], "marker": None},
    ])
    txs = [t async for t in paginate_all(raw.fetch, "rX", limit=1)]
    assert [t["tx"]["hash"] for t in txs] == ["A", "B"]
    assert raw.calls == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ledger_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.ledger_client'`

- [ ] **Step 3: Write `xrpl_audit/ledger_client.py`**

```python
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

class LedgerSource(Protocol):
    async def account_tx(self, address: str, marker=None, limit: int = 200) -> tuple[list[dict], dict | None]:
        ...

async def paginate_all(
    fetch: Callable[[str, object, int], Awaitable[dict]],
    address: str,
    limit: int = 200,
) -> AsyncIterator[dict]:
    """Drive a raw account_tx fetch function across all marker pages."""
    marker = None
    while True:
        page = await fetch(address, marker, limit)
        for tx in page.get("transactions", []):
            yield tx
        marker = page.get("marker")
        if not marker:
            return

class LedgerClient:
    def __init__(self, url: str, max_retries: int = 5, backoff_base: float = 0.5):
        self.url = url
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self._client = None

    async def _raw_fetch(self, address: str, marker, limit: int) -> dict:
        from xrpl.models.requests import AccountTx
        from xrpl.asyncio.clients import AsyncWebsocketClient
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                if self._client is None:
                    self._client = AsyncWebsocketClient(self.url)
                    await self._client.open()
                req = AccountTx(account=address, limit=limit,
                                marker=marker, forward=True)
                resp = await self._client.request(req)
                result = resp.result
                return {"transactions": result.get("transactions", []),
                        "marker": result.get("marker")}
            except Exception as exc:                  # reconnect + backoff
                last_exc = exc
                self._client = None
                await asyncio.sleep(self.backoff_base * (2 ** attempt))
        raise RuntimeError(f"account_tx failed after {self.max_retries} retries: {last_exc}")

    async def account_tx(self, address: str, marker=None, limit: int = 200):
        page = await self._raw_fetch(address, marker, limit)
        return page["transactions"], page["marker"]

    async def verify_full_history(self) -> bool:
        from xrpl.models.requests import ServerInfo
        if self._client is None:
            from xrpl.asyncio.clients import AsyncWebsocketClient
            self._client = AsyncWebsocketClient(self.url)
            await self._client.open()
        resp = await self._client.request(ServerInfo())
        complete = resp.result.get("info", {}).get("complete_ledgers", "")
        return complete.split("-")[0].strip() in ("2", "32570") or complete.startswith("2-")

    async def close(self):
        if self._client is not None:
            await self._client.close()
            self._client = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_ledger_client.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Add a `FakeLedger` to `conftest.py` for downstream tasks**

```python
# append to tests/conftest.py
from xrpl_audit.ledger_client import LedgerSource

class FakeLedger:
    """Maps address -> list of account_tx entries. Single-page (no markers)."""
    def __init__(self, by_address: dict[str, list[dict]]):
        self.by_address = by_address
        self.requested: list[str] = []
    async def account_tx(self, address, marker=None, limit=200):
        self.requested.append(address)
        return self.by_address.get(address, []), None

import pytest
@pytest.fixture
def fake_ledger_factory():
    return FakeLedger
```

- [ ] **Step 6: Commit**

```bash
git add xrpl_audit/ledger_client.py tests/test_ledger_client.py tests/conftest.py
git commit -m "feat: ledger client with pagination, retry, and injectable source"
```

---

### Task 5: Crawler (async BFS, worker pool, expansion policy)

**Files:**
- Create: `xrpl_audit/crawler.py`
- Test: `tests/test_crawler.py`

**Interfaces:**
- Consumes: `Store`, `LedgerSource`, `parse_transaction`.
- Produces:
  - `is_service_leaf(counterparty_count: int, degree_cap: int) -> bool`
  - `async def crawl(seed: str, store: Store, source: LedgerSource, *, workers: int = 5, max_hops: int = 4, degree_cap: int = 500, max_accounts: int = 5000) -> None`

Crawl algorithm:
1. Upsert seed with `hop_depth=0, crawl_status='pending'`. Seed an `asyncio.Queue` with `(seed, 0)`.
2. N worker coroutines loop: get `(addr, hop)` from queue; if account already `done`/`leaf`, `task_done()` and continue. Fetch all `account_tx` entries via `source.account_tx` (single call in tests; real client pre-paginates — for the crawler we call `source.account_tx` and ignore marker since `LedgerClient.account_tx` returns one page — see note). Parse each entry, store tx + edges, record counterparties.
3. After processing, compute `counterparty_count`; if `> degree_cap`, mark `is_service_leaf=1`, status `leaf`, and **do not enqueue its counterparties**. Otherwise mark status `done` and, for each distinct private counterparty, upsert with `hop_depth=hop+1` and enqueue `(cp, hop+1)` if `hop+1 <= max_hops` and total accounts `< max_accounts` and not already known.
4. Workers exit when the queue is drained (use a sentinel count / `queue.join()` + cancel).

> Pagination note: `LedgerClient.account_tx` returns a single page + marker. For full history the crawler must loop markers. To keep the worker simple and testable, add a helper `async def fetch_account_history(source, address) -> list[dict]` that loops `source.account_tx` following markers until `None`. `FakeLedger` returns `marker=None` so it terminates after one page.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_crawler.py
import pytest
from xrpl_audit.crawler import crawl, is_service_leaf
from tests.conftest import FakeLedger

def _payment(h, src, dst, amount="10", created=False):
    meta = {"TransactionResult": "tesSUCCESS", "AffectedNodes": []}
    if created:
        meta["AffectedNodes"] = [{"CreatedNode": {"LedgerEntryType": "AccountRoot",
                                                   "NewFields": {"Account": dst}}}]
    return {"tx": {"TransactionType": "Payment", "Account": src, "Destination": dst,
                   "Amount": amount, "Fee": "12", "hash": h},
            "meta": meta, "ledger_index": 1, "tx_close_time": 700000000}

def test_is_service_leaf():
    assert is_service_leaf(501, 500) is True
    assert is_service_leaf(500, 500) is False

@pytest.mark.asyncio
async def test_crawl_expands_activation_chain(store):
    # rSeed activated rChild; rChild paid rGrand
    ledger = FakeLedger({
        "rSeed":  [_payment("H1", "rSeed", "rChild", "20000000", created=True)],
        "rChild": [_payment("H2", "rChild", "rGrand", "5")],
        "rGrand": [],
    })
    await crawl("rSeed", store, ledger, workers=2, max_hops=4, degree_cap=500, max_accounts=100)
    addrs = {a["address"] for a in store.iter_accounts()}
    assert {"rSeed", "rChild", "rGrand"} <= addrs
    assert store.get_account("rChild")["crawl_status"] == "done"
    assert store.get_account("rSeed")["hop_depth"] == 0
    assert store.get_account("rGrand")["hop_depth"] == 2

@pytest.mark.asyncio
async def test_crawl_respects_max_hops(store):
    ledger = FakeLedger({
        "rSeed":  [_payment("H1", "rSeed", "rA", created=True)],
        "rA":     [_payment("H2", "rA", "rB", created=True)],
        "rB":     [_payment("H3", "rB", "rC", created=True)],
        "rC": [], "rB_unused": [],
    })
    await crawl("rSeed", store, ledger, workers=2, max_hops=1, degree_cap=500, max_accounts=100)
    # rB is at hop 2 > max_hops 1: recorded as edge target but never fetched
    assert "rB" not in ledger.requested

@pytest.mark.asyncio
async def test_crawl_treats_high_degree_as_leaf(store):
    # rSeed pays 3 distinct accounts; degree_cap=2 -> rSeed is a leaf, children NOT expanded
    ledger = FakeLedger({
        "rSeed": [_payment("H1", "rSeed", "rA"), _payment("H2", "rSeed", "rB"),
                  _payment("H3", "rSeed", "rC")],
        "rA": [], "rB": [], "rC": [],
    })
    await crawl("rSeed", store, ledger, workers=2, max_hops=4, degree_cap=2, max_accounts=100)
    assert store.get_account("rSeed")["is_service_leaf"] == 1
    assert store.get_account("rSeed")["crawl_status"] == "leaf"
    assert ledger.requested == ["rSeed"]   # children never fetched
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_crawler.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.crawler'`

- [ ] **Step 3: Write `xrpl_audit/crawler.py`**

```python
import asyncio
import json
from .models import ParsedTx
from .parser import parse_transaction
from .storage import Store
from .ledger_client import LedgerSource

def is_service_leaf(counterparty_count: int, degree_cap: int) -> bool:
    return counterparty_count > degree_cap

async def fetch_account_history(source: LedgerSource, address: str) -> list[dict]:
    out, marker = [], None
    while True:
        txs, marker = await source.account_tx(address, marker=marker)
        out.extend(txs)
        if not marker:
            return out

def _counterparties(parsed: ParsedTx, self_addr: str) -> set[str]:
    cps = set()
    for e in parsed.edges:
        for node in (e.src, e.dst):
            if node and node != self_addr:
                cps.add(node)
    return cps

async def crawl(seed: str, store: Store, source: LedgerSource, *,
                workers: int = 5, max_hops: int = 4,
                degree_cap: int = 500, max_accounts: int = 5000) -> None:
    store.upsert_account(seed, hop_depth=0, crawl_status="pending")
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait((seed, 0))
    seen_count = 1

    async def worker():
        nonlocal seen_count
        while True:
            try:
                addr, hop = await queue.get()
            except asyncio.CancelledError:
                return
            try:
                acct = store.get_account(addr)
                if acct and acct["crawl_status"] in ("done", "leaf"):
                    continue
                history = await fetch_account_history(source, addr)
                counterparties: set[str] = set()
                for entry in history:
                    parsed = parse_transaction(entry)
                    if not parsed.tx_hash:
                        continue
                    store.insert_transaction(parsed, raw_json=json.dumps(entry))
                    for e in parsed.edges:
                        store.insert_edge(e, parsed.tx_hash, parsed.ledger_index or 0)
                        if e.edge_type == "activation" and e.dst != addr:
                            store.upsert_account(e.dst, activation_parent=e.src)
                    counterparties |= _counterparties(parsed, addr)

                for cp in counterparties:
                    store.record_counterparty(addr, cp)
                store.upsert_account(addr, tx_count=len(history))
                cp_count = store.get_account(addr)["counterparty_count"]

                if is_service_leaf(cp_count, degree_cap):
                    store.upsert_account(addr, is_service_leaf=1, crawl_status="leaf")
                    continue
                store.set_crawl_status(addr, "done")

                if hop + 1 > max_hops:
                    continue
                for cp in counterparties:
                    known = store.get_account(cp)
                    if known and known["crawl_status"] in ("done", "leaf", "pending"):
                        continue
                    if seen_count >= max_accounts:
                        break
                    store.upsert_account(cp, hop_depth=hop + 1, crawl_status="pending")
                    seen_count += 1
                    queue.put_nowait((cp, hop + 1))
            finally:
                queue.task_done()

    tasks = [asyncio.create_task(worker()) for _ in range(workers)]
    await queue.join()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_crawler.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/crawler.py tests/test_crawler.py
git commit -m "feat: async BFS crawler with expansion policy and service-leaf detection"
```

---

### Task 6: CLI `crawl` + `status`

**Files:**
- Create: `xrpl_audit/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `Store`, `crawl`, `LedgerClient`.
- Produces: a `click` group `cli` with `crawl` and `status` commands. `crawl` builds a `LedgerClient`, runs `asyncio.run(crawl(...))`. `status` prints `store.counts()`.

- [ ] **Step 1: Write failing test (status command over a pre-seeded DB)**

```python
# tests/test_cli.py
from click.testing import CliRunner
from xrpl_audit.cli import cli
from xrpl_audit.storage import Store

def test_status_reports_counts(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    s.upsert_account("rA", crawl_status="done")
    s.conn.close()
    res = CliRunner().invoke(cli, ["--db", str(db), "status"])
    assert res.exit_code == 0
    assert "accounts" in res.output and "1" in res.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.cli'`

- [ ] **Step 3: Write `xrpl_audit/cli.py`**

```python
import asyncio
import json
import click
from .storage import Store
from .crawler import crawl as run_crawl
from .ledger_client import LedgerClient

@click.group()
@click.option("--db", default="audit.db", help="SQLite database path.")
@click.pass_context
def cli(ctx, db):
    ctx.ensure_object(dict)
    ctx.obj["db"] = db

@cli.command()
@click.argument("seed")
@click.option("--workers", default=5)
@click.option("--max-hops", default=4)
@click.option("--degree-cap", default=500)
@click.option("--max-accounts", default=5000)
@click.option("--node", default="wss://xrplcluster.com")
@click.pass_context
def crawl(ctx, seed, workers, max_hops, degree_cap, max_accounts, node):
    """Crawl the ledger starting from SEED account."""
    store = Store(ctx.obj["db"]); store.init_schema()
    client = LedgerClient(node)

    async def _run():
        if not await client.verify_full_history():
            click.echo("WARNING: node does not advertise full history; results may be partial.", err=True)
        await run_crawl(seed, store, client, workers=workers, max_hops=max_hops,
                        degree_cap=degree_cap, max_accounts=max_accounts)
        await client.close()

    asyncio.run(_run())
    click.echo(json.dumps(store.counts(), indent=2))

@cli.command()
@click.pass_context
def status(ctx):
    """Show crawl progress / DB stats."""
    store = Store(ctx.obj["db"]); store.init_schema()
    click.echo(json.dumps(store.counts(), indent=2))

if __name__ == "__main__":
    cli()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/cli.py tests/test_cli.py
git commit -m "feat: CLI crawl and status commands"
```

**STAGE 1 GATE:** `pytest -v` all green; `pip install -e .` then `audit --help` works. You now have a resumable crawl writing to SQLite.

---

## STAGE 2 — CLUSTERING

### Task 7: Strong signals — key/signer overlap

**Files:**
- Create: `xrpl_audit/signals.py`
- Test: `tests/test_signals.py`

**Interfaces:**
- Consumes: `Store`, `PairSignal`.
- Produces: `compute_key_signer_signals(store: Store) -> list[PairSignal]`.

Logic:
- Each `regular_key` edge `A→K`: emit `PairSignal(A, K, "regular_key", strength=1.0, detail={...})`.
- Each `signer_list` edge `A→S`: emit `PairSignal(A, S, "signer_list", strength=0.9, detail={...})`.
- **Shared regular key:** if accounts A and B both set the same regular key K (two `regular_key` edges `A→K`, `B→K`), emit `PairSignal(A, B, "shared_regular_key", strength=1.0, detail={"key": K})`.
- Pair ordering normalized so `a < b` lexicographically (helper `_pair(x, y)`).

- [ ] **Step 1: Write failing tests**

```python
# tests/test_signals.py
from xrpl_audit.models import Edge
from xrpl_audit.signals import compute_key_signer_signals

def _seed_edges(store, edges):
    for i, (s, d, t) in enumerate(edges):
        store.upsert_account(s); store.upsert_account(d)
        store.insert_edge(Edge(s, d, t), tx_hash=f"H{i}", ledger_index=i)

def test_regular_key_signal(store):
    _seed_edges(store, [("rMaster", "rKey", "regular_key")])
    sigs = compute_key_signer_signals(store)
    assert any(s.signal_type == "regular_key" and {s.a, s.b} == {"rMaster", "rKey"} for s in sigs)

def test_shared_regular_key_links_two_accounts(store):
    _seed_edges(store, [("rA", "rKey", "regular_key"), ("rB", "rKey", "regular_key")])
    sigs = compute_key_signer_signals(store)
    shared = [s for s in sigs if s.signal_type == "shared_regular_key"]
    assert len(shared) == 1 and {shared[0].a, shared[0].b} == {"rA", "rB"}
    assert shared[0].strength == 1.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_signals.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.signals'`

- [ ] **Step 3: Write `xrpl_audit/signals.py`**

```python
from collections import defaultdict
from .models import PairSignal
from .storage import Store

def _pair(x: str, y: str) -> tuple[str, str]:
    return (x, y) if x <= y else (y, x)

def _edges_by_type(store: Store, edge_type: str):
    return [e for e in store.iter_edges() if e["edge_type"] == edge_type]

def compute_key_signer_signals(store: Store) -> list[PairSignal]:
    out: list[PairSignal] = []
    key_to_setters: dict[str, list[str]] = defaultdict(list)

    for e in _edges_by_type(store, "regular_key"):
        a, b = _pair(e["src"], e["dst"])
        out.append(PairSignal(a, b, "regular_key", 1.0, {"tx": e["tx_hash"]}))
        key_to_setters[e["dst"]].append(e["src"])

    for e in _edges_by_type(store, "signer_list"):
        a, b = _pair(e["src"], e["dst"])
        out.append(PairSignal(a, b, "signer_list", 0.9, {"tx": e["tx_hash"]}))

    for key, setters in key_to_setters.items():
        uniq = sorted(set(setters))
        for i in range(len(uniq)):
            for j in range(i + 1, len(uniq)):
                a, b = _pair(uniq[i], uniq[j])
                out.append(PairSignal(a, b, "shared_regular_key", 1.0, {"key": key}))
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_signals.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/signals.py tests/test_signals.py
git commit -m "feat: key/signer overlap clustering signals"
```

---

### Task 8: Strong signals — activation / funding chains

**Files:**
- Modify: `xrpl_audit/signals.py`
- Test: `tests/test_signals.py` (add)

**Interfaces:**
- Produces: `compute_funding_signals(store: Store) -> list[PairSignal]`.

Logic:
- Each `activation` edge `A→B`: emit `PairSignal(A, B, "activation", 0.7, {...})`.
- **Self-transfer:** for `payment` edges where both endpoints are crawled **private** (non-leaf) accounts and value flows both directions between the same pair (A→B and B→A both exist) OR a single direction with the pair never interacting with services in between — for this task, emit `self_transfer` when both A→B and B→A `payment` edges exist between two private accounts. Strength 0.6.

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_signals.py
from xrpl_audit.signals import compute_funding_signals

def _private(store, *addrs):
    for a in addrs:
        store.upsert_account(a, is_service_leaf=0, crawl_status="done")

def test_activation_signal(store):
    _private(store, "rFunder", "rChild")
    _seed_edges(store, [("rFunder", "rChild", "activation")])
    sigs = compute_funding_signals(store)
    assert any(s.signal_type == "activation" and {s.a, s.b} == {"rFunder", "rChild"} for s in sigs)

def test_bidirectional_self_transfer(store):
    _private(store, "rA", "rB")
    _seed_edges(store, [("rA", "rB", "payment"), ("rB", "rA", "payment")])
    sigs = compute_funding_signals(store)
    st = [s for s in sigs if s.signal_type == "self_transfer"]
    assert len(st) == 1 and {st[0].a, st[0].b} == {"rA", "rB"}
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_signals.py -k funding -v`
Expected: FAIL — `cannot import name 'compute_funding_signals'`

- [ ] **Step 3: Append to `xrpl_audit/signals.py`**

```python
def _private_accounts(store: Store) -> set[str]:
    return {a["address"] for a in store.iter_accounts() if not a["is_service_leaf"]}

def compute_funding_signals(store: Store) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)

    for e in _edges_by_type(store, "activation"):
        a, b = _pair(e["src"], e["dst"])
        out.append(PairSignal(a, b, "activation", 0.7, {"tx": e["tx_hash"]}))

    directed: set[tuple[str, str]] = set()
    for e in _edges_by_type(store, "payment"):
        if e["src"] in private and e["dst"] in private:
            directed.add((e["src"], e["dst"]))
    emitted: set[tuple[str, str]] = set()
    for s, d in directed:
        if (d, s) in directed:
            a, b = _pair(s, d)
            if (a, b) not in emitted:
                emitted.add((a, b))
                out.append(PairSignal(a, b, "self_transfer", 0.6, {}))
    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_signals.py -k funding -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/signals.py tests/test_signals.py
git commit -m "feat: activation and self-transfer clustering signals"
```

---

### Task 9: Medium signals — shared counterparties + NFT flow

**Files:**
- Modify: `xrpl_audit/signals.py`
- Test: `tests/test_signals.py` (add)

**Interfaces:**
- Produces: `compute_counterparty_nft_signals(store: Store, min_jaccard: float = 0.3, min_shared: int = 3) -> list[PairSignal]`.

Logic:
- Build each private account's set of **private** counterparties (exclude service leaves). For each pair of private accounts, compute Jaccard. If `jaccard >= min_jaccard` and shared count `>= min_shared`, emit `PairSignal(a, b, "counterparty_jaccard", strength=min(0.5, jaccard), detail={"jaccard":..., "shared":...})`.
- For each `nft_transfer`/`nft_sale` edge between two private accounts, emit `PairSignal(a, b, "nft_flow", 0.4, {...})`.

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_signals.py
from xrpl_audit.signals import compute_counterparty_nft_signals

def test_counterparty_jaccard(store):
    _private(store, "rA", "rB")
    for cp in ["rX", "rY", "rZ"]:
        store.upsert_account(cp, is_service_leaf=0)
        store.record_counterparty("rA", cp)
        store.record_counterparty("rB", cp)
    sigs = compute_counterparty_nft_signals(store, min_jaccard=0.3, min_shared=3)
    assert any(s.signal_type == "counterparty_jaccard" and {s.a, s.b} == {"rA", "rB"} for s in sigs)

def test_nft_flow_signal(store):
    _private(store, "rA", "rB")
    _seed_edges(store, [("rA", "rB", "nft_transfer")])
    sigs = compute_counterparty_nft_signals(store)
    assert any(s.signal_type == "nft_flow" and {s.a, s.b} == {"rA", "rB"} for s in sigs)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_signals.py -k "jaccard or nft" -v`
Expected: FAIL — `cannot import name 'compute_counterparty_nft_signals'`

- [ ] **Step 3: Append to `xrpl_audit/signals.py`**

```python
def _counterparty_sets(store: Store, private: set[str]) -> dict[str, set[str]]:
    sets: dict[str, set[str]] = defaultdict(set)
    rows = store.conn.execute("SELECT address, counterparty FROM counterparties")
    for r in rows:
        if r["address"] in private and r["counterparty"] in private:
            sets[r["address"]].add(r["counterparty"])
    return sets

def compute_counterparty_nft_signals(store: Store, min_jaccard: float = 0.3,
                                     min_shared: int = 3) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)
    sets = _counterparty_sets(store, private)
    accts = sorted(sets)
    for i in range(len(accts)):
        for j in range(i + 1, len(accts)):
            a, b = accts[i], accts[j]
            sa, sb = sets[a], sets[b]
            inter = sa & sb
            union = sa | sb
            if not union:
                continue
            jac = len(inter) / len(union)
            if jac >= min_jaccard and len(inter) >= min_shared:
                out.append(PairSignal(a, b, "counterparty_jaccard", min(0.5, jac),
                                      {"jaccard": round(jac, 3), "shared": len(inter)}))
    for etype in ("nft_transfer", "nft_sale"):
        for e in _edges_by_type(store, etype):
            if e["src"] in private and e["dst"] in private:
                a, b = _pair(e["src"], e["dst"])
                out.append(PairSignal(a, b, "nft_flow", 0.4, {"edge": etype, "tx": e["tx_hash"]}))
    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_signals.py -k "jaccard or nft" -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/signals.py tests/test_signals.py
git commit -m "feat: counterparty-overlap and NFT-flow clustering signals"
```

---

### Task 10: Weak signals — behavioral fingerprints

**Files:**
- Modify: `xrpl_audit/signals.py`
- Test: `tests/test_signals.py` (add)

**Interfaces:**
- Produces: `compute_behavioral_signals(store: Store) -> list[PairSignal]`.

Logic (corroborating, low strength):
- **Domain reuse:** accounts sharing the same non-null `domain` → `PairSignal(a, b, "domain_reuse", 0.5, {"domain":...})` (high because identical domain is meaningful).
- **Active-hours similarity:** build a 24-bucket histogram of tx `close_time` hour (ripple epoch → hour-of-day) per private account; cosine similarity `>= 0.9` and each account has `>= 20` txs → `PairSignal(a, b, "active_hours", 0.2, {"cosine":...})`.

Ripple epoch → hour: `hour = ((close_time + 946684800) // 3600) % 24` (ripple epoch starts 2000-01-01 = unix 946684800).

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_signals.py
from xrpl_audit.signals import compute_behavioral_signals

def test_domain_reuse(store):
    store.upsert_account("rA", domain="6578616d706c65", is_service_leaf=0)
    store.upsert_account("rB", domain="6578616d706c65", is_service_leaf=0)
    sigs = compute_behavioral_signals(store)
    assert any(s.signal_type == "domain_reuse" and {s.a, s.b} == {"rA", "rB"} for s in sigs)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_signals.py -k behavioral -v`
Expected: FAIL — `cannot import name 'compute_behavioral_signals'`

- [ ] **Step 3: Append to `xrpl_audit/signals.py`**

```python
import math

RIPPLE_EPOCH_OFFSET = 946684800

def _hour_histograms(store: Store, private: set[str]) -> dict[str, list[int]]:
    hist: dict[str, list[int]] = defaultdict(lambda: [0] * 24)
    for r in store.conn.execute("SELECT sender, close_time FROM transactions"):
        s = r["sender"]
        if s in private and r["close_time"]:
            hour = ((r["close_time"] + RIPPLE_EPOCH_OFFSET) // 3600) % 24
            hist[s][hour] += 1
    return hist

def _cosine(u: list[int], v: list[int]) -> float:
    dot = sum(a * b for a, b in zip(u, v))
    nu = math.sqrt(sum(a * a for a in u))
    nv = math.sqrt(sum(b * b for b in v))
    return dot / (nu * nv) if nu and nv else 0.0

def compute_behavioral_signals(store: Store) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)

    by_domain: dict[str, list[str]] = defaultdict(list)
    for a in store.iter_accounts():
        if a["domain"] and not a["is_service_leaf"]:
            by_domain[a["domain"]].append(a["address"])
    for domain, addrs in by_domain.items():
        addrs = sorted(set(addrs))
        for i in range(len(addrs)):
            for j in range(i + 1, len(addrs)):
                a, b = _pair(addrs[i], addrs[j])
                out.append(PairSignal(a, b, "domain_reuse", 0.5, {"domain": domain}))

    hist = _hour_histograms(store, private)
    accts = sorted(k for k, v in hist.items() if sum(v) >= 20)
    for i in range(len(accts)):
        for j in range(i + 1, len(accts)):
            a, b = accts[i], accts[j]
            cos = _cosine(hist[a], hist[b])
            if cos >= 0.9:
                out.append(PairSignal(a, b, "active_hours", 0.2, {"cosine": round(cos, 3)}))
    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_signals.py -k behavioral -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/signals.py tests/test_signals.py
git commit -m "feat: behavioral-fingerprint clustering signals"
```

---

### Task 11: Cluster scorer (weighted + union-find + tiers)

**Files:**
- Create: `xrpl_audit/cluster.py`
- Test: `tests/test_cluster.py`

**Interfaces:**
- Consumes: `PairSignal`, `Cluster`, `Store`, and all four `compute_*` functions.
- Produces:
  - `DEFAULT_WEIGHTS: dict[str, float]` (per signal_type multiplier).
  - `score_pairs(signals: list[PairSignal], weights: dict | None = None) -> dict[tuple[str,str], dict]` returning `{(a,b): {"score": float, "signals": [PairSignal,...]}}`.
  - `assign_tier(score: float) -> str` → `confirmed` (>=1.0), `likely` (>=0.6), `possible` (>=0.3), else `None`.
  - `build_clusters(pair_scores) -> list[Cluster]`: union-find joining any pair whose tier is `likely`+ (>=0.6); each cluster's tier = best tier among its internal pairs; evidence = all contributing signals.
  - `run_clustering(store, weights=None) -> list[Cluster]`: runs all 4 signal fns, scores, builds clusters, persists into `signals` + `clusters` tables, returns clusters.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_cluster.py
from xrpl_audit.models import PairSignal
from xrpl_audit.cluster import score_pairs, assign_tier, build_clusters

def test_score_accumulates_weighted_signals():
    sigs = [PairSignal("rA", "rB", "activation", 0.7, {}),
            PairSignal("rA", "rB", "self_transfer", 0.6, {})]
    scored = score_pairs(sigs, weights={"activation": 1.0, "self_transfer": 1.0})
    assert scored[("rA", "rB")]["score"] == 1.3

def test_assign_tier():
    assert assign_tier(1.0) == "confirmed"
    assert assign_tier(0.6) == "likely"
    assert assign_tier(0.3) == "possible"
    assert assign_tier(0.1) is None

def test_build_clusters_unions_strong_pairs():
    sigs = [PairSignal("rA", "rB", "shared_regular_key", 1.0, {}),
            PairSignal("rB", "rC", "activation", 0.7, {})]
    scored = score_pairs(sigs)
    clusters = build_clusters(scored)
    members = [c.members for c in clusters]
    assert any({"rA", "rB", "rC"} == m for m in members)
    big = next(c for c in clusters if c.members == {"rA", "rB", "rC"})
    assert big.tier == "confirmed"
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_cluster.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.cluster'`

- [ ] **Step 3: Write `xrpl_audit/cluster.py`**

```python
import json
from collections import defaultdict
from .models import PairSignal, Cluster
from .storage import Store
from .signals import (compute_key_signer_signals, compute_funding_signals,
                      compute_counterparty_nft_signals, compute_behavioral_signals)

DEFAULT_WEIGHTS = {
    "regular_key": 1.0, "shared_regular_key": 1.0, "signer_list": 1.0,
    "activation": 1.0, "self_transfer": 1.0,
    "counterparty_jaccard": 1.0, "nft_flow": 1.0,
    "domain_reuse": 1.0, "active_hours": 1.0,
}

def score_pairs(signals, weights=None):
    weights = weights or DEFAULT_WEIGHTS
    acc: dict[tuple[str, str], dict] = defaultdict(lambda: {"score": 0.0, "signals": []})
    for s in signals:
        key = (s.a, s.b) if s.a <= s.b else (s.b, s.a)
        w = weights.get(s.signal_type, 1.0)
        acc[key]["score"] = round(acc[key]["score"] + s.strength * w, 6)
        acc[key]["signals"].append(s)
    return dict(acc)

def assign_tier(score: float) -> str | None:
    if score >= 1.0:
        return "confirmed"
    if score >= 0.6:
        return "likely"
    if score >= 0.3:
        return "possible"
    return None

class _UF:
    def __init__(self): self.p = {}
    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]; x = self.p[x]
        return x
    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb: self.p[ra] = rb

def build_clusters(pair_scores, link_threshold: float = 0.6) -> list[Cluster]:
    uf = _UF()
    for (a, b), info in pair_scores.items():
        if info["score"] >= link_threshold:
            uf.union(a, b)
    groups: dict[str, set] = defaultdict(set)
    evidence: dict[str, list] = defaultdict(list)
    best: dict[str, float] = defaultdict(float)
    for (a, b), info in pair_scores.items():
        if info["score"] >= link_threshold:
            root = uf.find(a)
            groups[root] |= {a, b}
            evidence[root].extend(info["signals"])
            best[root] = max(best[root], info["score"])
    clusters = []
    for i, (root, members) in enumerate(sorted(groups.items()), start=1):
        clusters.append(Cluster(id=i, members=members,
                                tier=assign_tier(best[root]) or "possible",
                                evidence=evidence[root]))
    return clusters

def run_clustering(store: Store, weights=None) -> list[Cluster]:
    signals = (compute_key_signer_signals(store)
               + compute_funding_signals(store)
               + compute_counterparty_nft_signals(store)
               + compute_behavioral_signals(store))
    store.conn.execute("DELETE FROM signals")
    store.conn.execute("DELETE FROM clusters")
    for s in signals:
        store.conn.execute("INSERT INTO signals(a,b,signal_type,strength,detail) VALUES (?,?,?,?,?)",
                           (s.a, s.b, s.signal_type, s.strength, json.dumps(s.detail)))
    scored = score_pairs(signals, weights)
    clusters = build_clusters(scored)
    for c in clusters:
        ev = json.dumps([{"a": s.a, "b": s.b, "type": s.signal_type,
                          "strength": s.strength, "detail": s.detail} for s in c.evidence])
        for m in c.members:
            store.conn.execute("INSERT INTO clusters(cluster_id, member, tier, evidence) VALUES (?,?,?,?)",
                               (c.id, m, c.tier, ev))
    store.conn.commit()
    return clusters
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_cluster.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/cluster.py tests/test_cluster.py
git commit -m "feat: weighted scoring and union-find clustering with tiers"
```

---

### Task 12: CLI `cluster`

**Files:**
- Modify: `xrpl_audit/cli.py`
- Test: `tests/test_cli.py` (add)

**Interfaces:**
- Produces: `cluster` command. Optional `--weights <json-file>`; runs `run_clustering`, prints a summary (count per tier).

- [ ] **Step 1: Add failing test**

```python
# append to tests/test_cli.py
from xrpl_audit.models import Edge

def test_cluster_command_finds_shared_key_cluster(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    for a in ("rA", "rB"):
        s.upsert_account(a, is_service_leaf=0, crawl_status="done")
        s.insert_edge(Edge(a, "rKey", "regular_key"), tx_hash=f"H{a}", ledger_index=1)
    s.conn.close()
    res = CliRunner().invoke(cli, ["--db", str(db), "cluster"])
    assert res.exit_code == 0
    assert "confirmed" in res.output
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_cli.py -k cluster -v`
Expected: FAIL — `Error: No such command 'cluster'.`

- [ ] **Step 3: Add to `xrpl_audit/cli.py`**

```python
# add imports at top
import json
from collections import Counter
from .cluster import run_clustering

# add command
@cli.command()
@click.option("--weights", type=click.Path(exists=True), default=None,
              help="JSON file of {signal_type: weight} overrides.")
@click.pass_context
def cluster(ctx, weights):
    """Run phase-2 clustering over the crawled DB."""
    store = Store(ctx.obj["db"]); store.init_schema()
    w = json.loads(open(weights).read()) if weights else None
    clusters = run_clustering(store, w)
    by_tier = Counter(c.tier for c in clusters)
    click.echo(json.dumps({"clusters": len(clusters), "by_tier": dict(by_tier)}, indent=2))
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_cli.py -k cluster -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/cli.py tests/test_cli.py
git commit -m "feat: CLI cluster command"
```

**STAGE 2 GATE:** `pytest -v` green. `audit cluster` produces tiered clusters from a crawled DB.

---

## STAGE 3 — OUTPUTS

### Task 13: Obsidian vault export

**Files:**
- Create: `xrpl_audit/report.py`
- Test: `tests/test_report.py`

**Interfaces:**
- Consumes: `Store`, `Cluster`.
- Produces: `export_obsidian(store: Store, clusters: list[Cluster], vault_dir: str) -> None`.

Behavior:
- Writes into `<vault_dir>/accounts/` one note per account `<address>.md` and `<vault_dir>/clusters/` one per cluster. These two subfolders are the "tool-generated" zone — wiped & rewritten on each run; nothing else in the vault is touched.
- Account note: YAML frontmatter (address, hop_depth, tx_count, counterparty_count, is_service_leaf, cluster id if any, confidence tier if any, activation_parent), then `#service-leaf` tag line if leaf, then edge sections grouped by type with `[[wiki-links]]`.
- Cluster note: frontmatter (cluster_id, tier, size), members as `[[links]]`, and an evidence list.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_report.py
import pathlib
from xrpl_audit.models import Edge, Cluster, PairSignal
from xrpl_audit.report import export_obsidian

def test_export_writes_account_and_cluster_notes(store, tmp_path):
    store.upsert_account("rA", hop_depth=0, tx_count=2, counterparty_count=1,
                         is_service_leaf=0, crawl_status="done")
    store.upsert_account("rB", hop_depth=1, is_service_leaf=0, crawl_status="done")
    store.insert_edge(Edge("rA", "rB", "payment"), tx_hash="H1", ledger_index=1)
    clusters = [Cluster(id=1, members={"rA", "rB"}, tier="confirmed",
                        evidence=[PairSignal("rA", "rB", "shared_regular_key", 1.0, {})])]
    export_obsidian(store, clusters, str(tmp_path))
    note = (tmp_path / "accounts" / "rA.md").read_text()
    assert "[[rB]]" in note and "cluster: 1" in note and "confirmed" in note
    cnote = (tmp_path / "clusters" / "cluster-1.md").read_text()
    assert "[[rA]]" in cnote and "[[rB]]" in cnote

def test_service_leaf_is_tagged(store, tmp_path):
    store.upsert_account("rExch", is_service_leaf=1, crawl_status="leaf")
    export_obsidian(store, [], str(tmp_path))
    note = (tmp_path / "accounts" / "rExch.md").read_text()
    assert "#service-leaf" in note
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_report.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'xrpl_audit.report'`

- [ ] **Step 3: Write `xrpl_audit/report.py`**

```python
import shutil
from collections import defaultdict
from pathlib import Path
from .storage import Store
from .models import Cluster

def _account_cluster_map(clusters: list[Cluster]) -> dict[str, Cluster]:
    m: dict[str, Cluster] = {}
    for c in clusters:
        for member in c.members:
            m[member] = c
    return m

def export_obsidian(store: Store, clusters: list[Cluster], vault_dir: str) -> None:
    root = Path(vault_dir)
    accounts_dir = root / "accounts"
    clusters_dir = root / "clusters"
    for d in (accounts_dir, clusters_dir):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    cmap = _account_cluster_map(clusters)

    edges_out: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for e in store.iter_edges():
        edges_out[e["src"]][e["edge_type"]].append(e["dst"])

    for acct in store.iter_accounts():
        addr = acct["address"]
        c = cmap.get(addr)
        fm = ["---",
              f"address: {addr}",
              f"hop_depth: {acct['hop_depth']}",
              f"tx_count: {acct['tx_count']}",
              f"counterparty_count: {acct['counterparty_count']}",
              f"is_service_leaf: {bool(acct['is_service_leaf'])}",
              f"activation_parent: {acct['activation_parent'] or ''}"]
        if c:
            fm += [f"cluster: {c.id}", f"confidence: {c.tier}"]
        fm.append("---")
        body = ["\n".join(fm), ""]
        tags = []
        if acct["is_service_leaf"]:
            tags.append("#service-leaf")
        if c:
            tags += [f"#cluster/{c.id}", f"#confidence/{c.tier}"]
        if tags:
            body += [" ".join(tags), ""]
        if acct["activation_parent"]:
            body += [f"Activated by [[{acct['activation_parent']}]]", ""]
        for etype, dsts in sorted(edges_out.get(addr, {}).items()):
            body.append(f"## {etype}")
            for d in sorted(set(dsts)):
                body.append(f"- [[{d}]]")
            body.append("")
        (accounts_dir / f"{addr}.md").write_text("\n".join(body))

    for c in clusters:
        fm = ["---", f"cluster_id: {c.id}", f"tier: {c.tier}",
              f"size: {len(c.members)}", "---", ""]
        lines = fm + [f"# Cluster {c.id} ({c.tier})", "", "## Members"]
        lines += [f"- [[{m}]]" for m in sorted(c.members)]
        lines += ["", "## Evidence"]
        for s in c.evidence:
            lines.append(f"- `{s.signal_type}` [[{s.a}]] ↔ [[{s.b}]] (strength {s.strength}) {s.detail}")
        (clusters_dir / f"cluster-{c.id}.md").write_text("\n".join(lines))
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_report.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/report.py tests/test_report.py
git commit -m "feat: Obsidian vault export"
```

---

### Task 14: Graph export (GEXF + DOT) + SQL views

**Files:**
- Modify: `xrpl_audit/report.py`
- Test: `tests/test_report.py` (add)

**Interfaces:**
- Produces:
  - `build_graph(store: Store, clusters: list[Cluster]) -> networkx.DiGraph` (nodes carry `cluster`, `tier`, `is_service_leaf`; edges carry `edge_type`).
  - `export_gexf(store, clusters, path: str) -> None`
  - `export_dot(store, clusters, path: str) -> None`
  - `create_views(store: Store) -> None` (canned SQL views: `v_account_clusters`, `v_cluster_sizes`).

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_report.py
import networkx as nx
from xrpl_audit.report import build_graph, export_gexf, create_views

def test_build_graph_has_nodes_and_typed_edges(store):
    store.upsert_account("rA", is_service_leaf=0)
    store.upsert_account("rB", is_service_leaf=0)
    store.insert_edge(Edge("rA", "rB", "payment"), tx_hash="H1", ledger_index=1)
    g = build_graph(store, [])
    assert g.has_edge("rA", "rB")
    assert g["rA"]["rB"]["edge_type"] == "payment"

def test_export_gexf_writes_file(store, tmp_path):
    store.upsert_account("rA", is_service_leaf=0)
    out = tmp_path / "g.gexf"
    export_gexf(store, [], str(out))
    assert out.exists() and out.stat().st_size > 0

def test_create_views(store):
    create_views(store)
    names = {r[0] for r in store.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'")}
    assert "v_cluster_sizes" in names
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_report.py -k "graph or gexf or views" -v`
Expected: FAIL — `cannot import name 'build_graph'`

- [ ] **Step 3: Append to `xrpl_audit/report.py`**

```python
import networkx as nx

def build_graph(store: Store, clusters: list[Cluster]) -> nx.DiGraph:
    cmap = _account_cluster_map(clusters)
    g = nx.DiGraph()
    for acct in store.iter_accounts():
        addr = acct["address"]
        c = cmap.get(addr)
        g.add_node(addr,
                   cluster=(c.id if c else -1),
                   tier=(c.tier if c else "none"),
                   is_service_leaf=int(acct["is_service_leaf"]))
    for e in store.iter_edges():
        g.add_edge(e["src"], e["dst"], edge_type=e["edge_type"])
    return g

def export_gexf(store: Store, clusters: list[Cluster], path: str) -> None:
    nx.write_gexf(build_graph(store, clusters), path)

def export_dot(store: Store, clusters: list[Cluster], path: str) -> None:
    g = build_graph(store, clusters)
    lines = ["digraph xrpl {"]
    for n, d in g.nodes(data=True):
        shape = "box" if d["is_service_leaf"] else "ellipse"
        lines.append(f'  "{n}" [shape={shape}, cluster={d["cluster"]}];')
    for u, v, d in g.edges(data=True):
        lines.append(f'  "{u}" -> "{v}" [label="{d["edge_type"]}"];')
    lines.append("}")
    Path(path).write_text("\n".join(lines))

def create_views(store: Store) -> None:
    store.conn.executescript("""
    CREATE VIEW IF NOT EXISTS v_account_clusters AS
      SELECT c.member AS address, c.cluster_id, c.tier
      FROM clusters c;
    CREATE VIEW IF NOT EXISTS v_cluster_sizes AS
      SELECT cluster_id, tier, COUNT(*) AS size
      FROM clusters GROUP BY cluster_id, tier ORDER BY size DESC;
    """)
    store.conn.commit()
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_report.py -k "graph or gexf or views" -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add xrpl_audit/report.py tests/test_report.py
git commit -m "feat: GEXF/DOT graph export and canned SQL views"
```

---

### Task 15: CLI `report` + README

**Files:**
- Modify: `xrpl_audit/cli.py`
- Create: `README.md`
- Test: `tests/test_cli.py` (add)

**Interfaces:**
- Produces: `report` command with `--format obsidian|gexf|dot` and `--vault`/`--out`. Loads clusters from DB into `Cluster` objects (helper `load_clusters(store) -> list[Cluster]` added to `cluster.py`), then dispatches to the matching exporter. `--min-confidence` filters clusters.

- [ ] **Step 1: Add `load_clusters` to `cluster.py` and a failing CLI test**

```python
# append to xrpl_audit/cluster.py
def load_clusters(store: Store) -> list[Cluster]:
    rows = store.conn.execute("SELECT cluster_id, member, tier, evidence FROM clusters").fetchall()
    by_id: dict[int, Cluster] = {}
    for r in rows:
        c = by_id.get(r["cluster_id"])
        if c is None:
            c = Cluster(id=r["cluster_id"], members=set(), tier=r["tier"], evidence=[])
            by_id[r["cluster_id"]] = c
        c.members.add(r["member"])
    return list(by_id.values())
```

```python
# append to tests/test_cli.py
def test_report_obsidian(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    s.upsert_account("rA", is_service_leaf=0, crawl_status="done")
    s.conn.execute("INSERT INTO clusters(cluster_id, member, tier, evidence) VALUES (1,'rA','confirmed','[]')")
    s.conn.commit(); s.conn.close()
    vault = tmp_path / "vault"
    res = CliRunner().invoke(cli, ["--db", str(db), "report", "--format", "obsidian", "--vault", str(vault)])
    assert res.exit_code == 0
    assert (vault / "accounts" / "rA.md").exists()
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_cli.py -k report -v`
Expected: FAIL — `Error: No such command 'report'.`

- [ ] **Step 3: Add `report` command to `xrpl_audit/cli.py`**

```python
# add imports
from .cluster import run_clustering, load_clusters
from .report import export_obsidian, export_gexf, export_dot, create_views

TIER_RANK = {"possible": 1, "likely": 2, "confirmed": 3}

@cli.command()
@click.option("--format", "fmt", type=click.Choice(["obsidian", "gexf", "dot"]), default="obsidian")
@click.option("--vault", default="vault", help="Output dir for obsidian format.")
@click.option("--out", default=None, help="Output file for gexf/dot.")
@click.option("--min-confidence", type=click.Choice(["possible", "likely", "confirmed"]), default="possible")
@click.pass_context
def report(ctx, fmt, vault, out, min_confidence):
    """Export crawl/cluster results."""
    store = Store(ctx.obj["db"]); store.init_schema()
    create_views(store)
    clusters = [c for c in load_clusters(store)
                if TIER_RANK[c.tier] >= TIER_RANK[min_confidence]]
    if fmt == "obsidian":
        export_obsidian(store, clusters, vault)
        click.echo(f"Wrote Obsidian vault to {vault}/")
    elif fmt == "gexf":
        path = out or "graph.gexf"
        export_gexf(store, clusters, path)
        click.echo(f"Wrote {path}")
    else:
        path = out or "graph.dot"
        export_dot(store, clusters, path)
        click.echo(f"Wrote {path}")
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_cli.py -k report -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Write `README.md`**

```markdown
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
```

- [ ] **Step 6: Run full suite + commit**

Run: `pytest -v`
Expected: all PASS

```bash
git add xrpl_audit/cli.py xrpl_audit/cluster.py README.md tests/test_cli.py
git commit -m "feat: CLI report command and README"
```

**STAGE 3 GATE:** full pipeline works end-to-end: `crawl` → `cluster` → `report` produces an Obsidian vault, GEXF, and queryable views.

---

## Self-Review

**Spec coverage:**
- Two-phase architecture → Stages 1–2 (crawl) / 2 (cluster). ✓
- Fixed worker pool + resumable BFS → Task 5 (crawl status gates resume; `pending_accounts` supports `--resume` continuation). ✓
- Public Clio node + `account_tx` marker pagination + full-history check → Task 4 (`paginate_all`, `verify_full_history`). ✓
- Expansion policy: degree cap + hop limit + max-accounts → Task 5. ✓
- Idempotent writes → Task 2. ✓
- All four signal categories → Tasks 7–10. ✓
- Transparent weighted scoring + tiers → Task 11. ✓
- Obsidian vault (every account, service-leaf tag, cluster notes, dataview frontmatter) → Task 13. ✓
- GEXF/DOT secondary + SQL views → Task 14. ✓
- CLI crawl/cluster/report/status → Tasks 6, 12, 15. ✓
- No-live-node testing + gated smoke test → Tasks 4–5, README. ✓

**Gaps noted for execution (acceptable; not blocking):**
- `--resume` flag isn't a separate option yet: the crawler is *inherently* resumable because `done`/`leaf` accounts are skipped and pending ones remain queued, but re-seeding the queue from `pending_accounts()` on restart should be wired into `cli.crawl` during Task 6 if a crawl is interrupted. Add a `--resume` flag there that seeds the queue from `store.pending_accounts()` instead of just the seed.
- NFT edge derivation in Task 3 is simplified (relies on an enrichment field for offer owner). Full NFT offer-owner resolution from `meta` AffectedNodes can be hardened during Task 3 if fixtures of real `NFTokenAcceptOffer` txns are available; the signal layer (Task 9) already handles whatever `nft_transfer`/`nft_sale` edges exist.

**Placeholder scan:** no TBD/TODO; every code step has real code. ✓
**Type consistency:** `Store`, `ParsedTx`, `Edge`, `PairSignal`, `Cluster`, signal function names, and `run_clustering`/`load_clusters` are consistent across tasks. ✓
