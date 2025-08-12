"""Analysis pipeline for Intent Archaeology.

This module groups conversations into rudimentary projects and identifies
"ghost" problems (unfinished workflows or unresolved errors).  The baseline
implementation provided here performs a one conversation per project
association and a simple keyword search on the final user message to
detect errors.  More sophisticated clustering and ghost detection can be
layered on top of this by wiring in external embeddings or LLMs through
`configs/providers.yaml`.
"""

from __future__ import annotations

import json
import re
from typing import Dict, Any, List

import typer

from . import db as db_module

app = typer.Typer(name="analyze")

# Keywords that indicate a possible unresolved error in a user message
ERROR_KEYWORDS = [
    "error",
    "exception",
    "traceback",
    "failed",
    "403",
    "404",
    "not working",
    "doesn't work",
    "didn't work",
]


def _detect_ghosts(conversation_id: str, conn) -> List[Dict[str, Any]]:
    """Identify potential ghost problems in a conversation.

    Currently, this examines the last message in the conversation (as
    recorded by conversations.current_node) and checks if the author was
    the user and if the content contains error keywords.  If so, the
    conversation is flagged as having an unresolved problem.
    """
    cur = conn.cursor()
    convo = cur.execute(
        "SELECT current_node, title FROM conversations WHERE id = ?",
        (conversation_id,),
    ).fetchone()
    if not convo:
        return []
    current_node = convo["current_node"]
    if not current_node:
        return []
    # fetch node details
    node = cur.execute(
        "SELECT role, content FROM nodes WHERE id = ?",
        (current_node,),
    ).fetchone()
    ghosts: List[Dict[str, Any]] = []
    if node:
        role = node["role"] or ""
        content = node["content"] or ""
        if role.lower() == "user":
            lower_content = content.lower()
            for kw in ERROR_KEYWORDS:
                if kw in lower_content:
                    ghosts.append(
                        {
                            "description": f"Unresolved user message containing '{kw}'",
                            "evidence": [current_node],
                        }
                    )
                    break
    return ghosts


@app.command()
def analyze(db_path: str = typer.Option(..., help="Path to SQLite DB"), out: str = typer.Option(..., help="Path to output JSON file")) -> None:
    """Analyse the ingested data and produce a summary of projects and ghost problems."""
    conn = db_module.get_connection(db_path)
    cur = conn.cursor()
    projects: List[Dict[str, Any]] = []
    # each conversation is treated as a project in this baseline implementation
    for convo in cur.execute("SELECT id, title FROM conversations"):
        convo_id = convo["id"]
        title = convo["title"]
        # collect all node ids for evidence
        node_ids = [row["id"] for row in cur.execute(
            "SELECT id FROM nodes WHERE conversation_id = ?",
            (convo_id,),
        )]
        ghosts = _detect_ghosts(convo_id, conn)
        projects.append(
            {
                "project_id": convo_id,
                "title": title,
                "node_ids": node_ids,
                "ghost_problems": ghosts,
            }
        )
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"projects": projects}, f, ensure_ascii=False, indent=2)
    typer.echo(f"Analysis complete. Wrote {len(projects)} projects to {out}")
