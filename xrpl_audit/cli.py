import asyncio
import json
from collections import Counter
import click
from .storage import Store
from .crawler import crawl as run_crawl
from .ledger_client import LedgerClient, make_quiet_handler
from .cluster import run_clustering, load_clusters
from .report import export_obsidian, export_gexf, export_dot, create_views

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
@click.option("--node", default="wss://s2.ripple.com", help="Full-history XRPL websocket node.")
@click.option("--rate", default=10.0, help="Max requests/sec across all workers (global throttle) to stay under the node's per-IP cap. 0 disables.")
@click.option("--resume", is_flag=True, default=False, help="Resume an interrupted crawl from pending accounts/markers.")
@click.option("--retry-errors", is_flag=True, default=False, help="Also re-queue accounts left in 'error' (e.g. from a rate-limit) and crawl them again.")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress the per-account progress output.")
@click.pass_context
def crawl(ctx, seed, workers, max_hops, degree_cap, max_accounts, node, rate, resume, retry_errors, quiet):
    """Crawl the ledger starting from SEED account."""
    store = Store(ctx.obj["db"]); store.init_schema()
    client = LedgerClient(node, min_request_interval=(1.0 / rate if rate > 0 else 0.0))

    def _progress(ev):
        tag = " [leaf]" if ev["leaf"] else ""
        click.echo(
            f"[crawl] {ev['processed']:>5} done | {ev['queued']:>5} queued | "
            f"hop {ev['hop']} | {ev['address']} ({ev['tx_count']} tx){tag}",
            err=True)

    noise = {"n": 0}

    async def _run():
        # Absorb the fire-and-forget websocket-teardown exceptions xrpl leaks on
        # every reconnect/close (harmless "Task exception was never retrieved"
        # spam) while still letting real bugs surface.
        def _count_noise(_ctx):
            noise["n"] += 1
        asyncio.get_running_loop().set_exception_handler(make_quiet_handler(on_noise=_count_noise))

        full_history = await client.verify_full_history()
        if full_history is None:
            click.echo("WARNING: could not verify node history at startup; proceeding anyway.", err=True)
        elif not full_history:
            click.echo("WARNING: node does not advertise full history; results may be partial.", err=True)
        try:
            await run_crawl(seed, store, client, workers=workers, max_hops=max_hops,
                            degree_cap=degree_cap, max_accounts=max_accounts, resume=resume,
                            retry_errors=retry_errors,
                            on_progress=None if quiet else _progress)
        finally:
            await client.close()

    asyncio.run(_run())
    if noise["n"] and not quiet:
        click.echo(f"[crawl] suppressed {noise['n']} expected websocket-teardown exceptions", err=True)
    click.echo(json.dumps(store.counts(), indent=2))

@cli.command()
@click.pass_context
def status(ctx):
    """Show crawl progress / DB stats."""
    store = Store(ctx.obj["db"]); store.init_schema()
    click.echo(json.dumps(store.counts(), indent=2))

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

if __name__ == "__main__":
    cli()
