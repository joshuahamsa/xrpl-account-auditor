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
