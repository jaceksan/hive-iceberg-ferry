from __future__ import annotations

import logging
import click

from .config import load_config
from .migrate import run_migration


@click.command()
@click.option(
    "-c", "--config",
    "config_path",
    default="config.yaml",
    show_default=True,
    help="Path to YAML config file.",
)
@click.option(
    "-t", "--tables",
    "table_list",
    default=None,
    help="Comma-separated list of tables (overrides config file).",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(config_path: str, table_list: str | None, verbose: bool) -> None:
    """Migrate Hive tables to Iceberg (S3 Tables / Glue / MinIO)."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    config = load_config(config_path)

    tables = None
    if table_list:
        tables = [t.strip() for t in table_list.split(",")]

    results = run_migration(config, tables)

    click.echo()
    click.echo("Migration Summary")
    click.echo("=" * 60)
    ok = fail = 0
    for r in results:
        if r["status"] == "success":
            ok += 1
            click.echo(
                f"  OK   {r['source']} -> {r['target']}  "
                f"({r['source_rows']} rows)"
            )
        else:
            fail += 1
            click.echo(f"  FAIL {r['source']}: {r.get('error', '?')}")
    click.echo("=" * 60)
    click.echo(f"  {ok} succeeded, {fail} failed")


if __name__ == "__main__":
    main()
