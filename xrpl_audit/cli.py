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
        try:
            await run_crawl(seed, store, client, workers=workers, max_hops=max_hops,
                            degree_cap=degree_cap, max_accounts=max_accounts)
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

if __name__ == "__main__":
    cli()
