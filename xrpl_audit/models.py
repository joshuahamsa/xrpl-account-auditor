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
