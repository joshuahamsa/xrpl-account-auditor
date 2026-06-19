import sqlite3
import json
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

    def pending_accounts(self, include_errors: bool = False) -> list[str]:
        statuses = ("pending", "error") if include_errors else ("pending",)
        placeholders = ",".join("?" * len(statuses))
        rows = self.conn.execute(
            f"SELECT address FROM accounts WHERE crawl_status IN ({placeholders}) "
            "ORDER BY hop_depth", statuses).fetchall()
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
