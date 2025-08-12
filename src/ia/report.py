"""Report generation for Intent Archaeology.

Reads the JSON output produced by the analysis phase and renders a
Markdown report using a Jinja2 template.  The template can be customised
by editing `templates/report.md.j2`.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

import typer
from jinja2 import Environment, FileSystemLoader, select_autoescape

app = typer.Typer(name="report")


@app.command()
def report(db_path: str = typer.Option(..., help="Path to SQLite DB (unused but kept for symmetry)"),
           findings: str = typer.Option(..., help="Path to analysis JSON file"),
           out: str = typer.Option(..., help="Path to write Markdown report")) -> None:
    """Generate a Markdown report from analysis findings."""
    # load analysis results
    with open(findings, "r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    # locate the templates directory relative to this file
    this_dir = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.abspath(os.path.join(this_dir, "..", "..", "templates"))
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=select_autoescape(["md"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tmpl = env.get_template("report.md.j2")
    rendered = tmpl.render(projects=data.get("projects", []))
    # ensure output directory exists
    out_path = os.path.abspath(out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f_out:
        f_out.write(rendered)
    typer.echo(f"Report written to {out}")
