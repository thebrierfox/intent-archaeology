"""Command line interface for Intent Archaeology.

This module defines the top t‑level Typer application and aggregates
commands from the ingestion, analysis and report modules.  Invoke
`python -m ia.cli --help` to see available sub‑commands.
"""

import typer

from . import ingest as ingest_cmd
from . import analyze as analyze_cmd
from . import report as report_cmd


app = typer.Typer(name="ia", help="Intent Archaeology CLI")

# register subcommands from other modules
app.add_typer(ingest_cmd.app, name="ingest", help="Ingest conversation exports")
app.add_typer(analyze_cmd.app, name="analyze", help="Analyse ingested data")
app.add_typer(report_cmd.app, name="report", help="Generate reports from analysis")


if __name__ == "__main__":
    app()
