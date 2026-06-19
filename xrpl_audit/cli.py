import asyncio
import json
from collections import Counter
import click
from .storage import Store
from .crawler import crawl as run_crawl
from .ledger_client import LedgerClient
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
@click.option("--node", default="wss://xrplcluster.com")
@click.option("--resume", is_flag=True, default=False, help="Resume an interrupted crawl from pending accounts/markers.")
@click.option("--retry-errors", is_flag=True, default=False, help="Also re-queue accounts left in 'error' (e.g. from a rate-limit) and crawl them again.")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress the per-account progress output.")
@click.pass_context
def crawl(ctx, seed, workers, max_hops, degree_cap, max_accounts, node, resume, retry_errors, quiet):
    """Crawl the ledger starting from SEED account."""
    store = Store(ctx.obj["db"]); store.init_schema()
    client = LedgerClient(node)

    def _progress(ev):
        tag = " [leaf]" if ev["leaf"] else ""
        click.echo(
            f"[crawl] {ev['processed']:>5} done | {ev['queued']:>5} queued | "
            f"hop {ev['hop']} | {ev['address']} ({ev['tx_count']} tx){tag}",
            err=True)

    async def _run():
        if not await client.verify_full_history():
            click.echo("WARNING: node does not advertise full history; results may be partial.", err=True)
        try:
            await run_crawl(seed, store, client, workers=workers, max_hops=max_hops,
                            degree_cap=degree_cap, max_accounts=max_accounts, resume=resume,
                            retry_errors=retry_errors,
                            on_progress=None if quiet else _progress)
        finally:
            await client.close()

    asyncio.run(_run())
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
