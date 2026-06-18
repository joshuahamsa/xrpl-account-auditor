import asyncio
import json
from .models import ParsedTx
from .parser import parse_transaction
from .storage import Store
from .ledger_client import LedgerSource


def is_service_leaf(counterparty_count: int, degree_cap: int) -> bool:
    return counterparty_count > degree_cap


async def fetch_account_history(source: LedgerSource, address: str, store=None) -> list[dict]:
    out = []
    marker = None
    if store is not None:
        acct = store.get_account(address)
        if acct and acct.get("last_marker"):
            marker = json.loads(acct["last_marker"])
    while True:
        txs, marker = await source.account_tx(address, marker=marker)
        out.extend(txs)
        if store is not None:
            store.set_marker(address, json.dumps(marker) if marker else None)
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
                degree_cap: int = 500, max_accounts: int = 5000,
                resume: bool = False) -> None:
    queue: asyncio.Queue = asyncio.Queue()
    enqueued: set[str] = set()
    if resume:
        pend = store.pending_accounts()
        if not pend:
            pend = [seed]
        for addr in pend:
            a = store.get_account(addr)
            hop = (a.get("hop_depth") if a else 0) or 0
            enqueued.add(addr)
            queue.put_nowait((addr, hop))
    else:
        store.upsert_account(seed, hop_depth=0, crawl_status="pending")
        enqueued.add(seed)
        queue.put_nowait((seed, 0))

    async def worker():
        while True:
            try:
                addr, hop = await queue.get()
            except asyncio.CancelledError:
                return
            try:
                acct = store.get_account(addr)
                if acct and acct["crawl_status"] in ("done", "leaf"):
                    continue
                history = await fetch_account_history(source, addr, store)
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
                prior = (store.get_account(addr) or {}).get("tx_count") or 0
                store.upsert_account(addr, tx_count=prior + len(history))
                cp_count = store.get_account(addr)["counterparty_count"]

                if is_service_leaf(cp_count, degree_cap):
                    store.upsert_account(addr, is_service_leaf=1, crawl_status="leaf")
                    continue
                store.set_crawl_status(addr, "done")

                if hop + 1 > max_hops:
                    continue
                for cp in counterparties:
                    if cp in enqueued:
                        continue
                    acct_cp = store.get_account(cp)
                    if acct_cp and acct_cp["crawl_status"] in ("done", "leaf"):
                        continue
                    if len(enqueued) >= max_accounts:
                        break
                    store.upsert_account(cp, hop_depth=hop + 1, crawl_status="pending")
                    enqueued.add(cp)
                    queue.put_nowait((cp, hop + 1))
            finally:
                queue.task_done()

    tasks = [asyncio.create_task(worker()) for _ in range(workers)]
    await queue.join()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
