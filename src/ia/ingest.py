"""Ingestion pipeline for Intent Archaeology.

This module provides a Typer CLI command to ingest a ChatGPT `conversations.json`
export into a local SQLite database.  Ingestion is incremental: each
conversation is fingerprinted by a hash of its ID, update time, mapping
length and current node.  If the fingerprint hasn't changed since the last
ingest, the conversation is skipped to avoid unnecessary work.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime
from typing import Dict, Any, Iterable

import typer
import ijson

from . import db

app = typer.Typer(name="ingest")


def _compute_fingerprint(convo: Dict[str, Any]) -> str:
    """Compute a stable hash summarising key properties of a conversation.

    The fingerprint combines the conversation ID, update time, mapping length
    and current node ID.  If any of these fields change, the fingerprint
    changes and ingestion will replace the stored data.
    """
    conv_id = convo.get("id", "")
    update_time = str(convo.get("update_time", ""))
    mapping_len = str(len(convo.get("mapping", {})))
    current_node = convo.get("current_node", "") or ""
    parts = f"{conv_id}|{update_time}|{mapping_len}|{current_node}"
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def _parse_nodes(convo: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Yield node dicts prepared for DB insertion.

    Only messages authored by user or assistant are stored; other roles
    (system/tool) are ignored for FTS and analysis.
    """
    mapping = convo.get("mapping", {})
    for node_id, node in mapping.items():
        message = node.get("message")
        if not message:
            # nodes without message payload (e.g., virtual root) are ignored
            continue
        author = message.get("author", {})
        role = author.get("role")
        content_obj = message.get("content", {})
        content_parts = content_obj.get("parts", []) if content_obj else []
        content = "\n\n".join(p for p in content_parts if p)
        create_time = None
        # some exports include a message-level create_time in metadata
        meta = message.get("metadata", {})
        if isinstance(meta, dict):
            create_time = meta.get("create_time") or meta.get("finish_time")
        yield {
            "id": node_id,
            "parent_id": node.get("parent"),
            "children": node.get("children", []) or [],
            "role": role,
            "content": content,
            "create_time": create_time,
        }


@app.command()
def ingest(input: str = typer.Option(..., help="Path to conversations.json file"), db_path: str = typer.Option(..., help="Path to the SQLite DB")) -> None:
    """Ingest conversations from a ChatGPT export into the database.

    This command opens the JSON file using ijson and processes each
    conversation one at a time.  Conversations with unchanged fingerprints
    since the last run are skipped.  New or updated conversations are
    parsed into nodes and edges and inserted into the database.  A log of
    the ingest run is recorded in the `ingest_runs` table.
    """
    input_path = os.path.abspath(input)
    conn = db.get_connection(db_path)
    db.init_db(conn)
    start_time = datetime.utcnow().isoformat()
    added = 0
    updated = 0
    skipped = 0
    # open and stream
    with open(input_path, "rb") as f:
        parser = ijson.items(f, "item")
        for convo in parser:
            conv_id = convo.get("id")
            if not conv_id:
                continue
            fingerprint = _compute_fingerprint(convo)
            existing_fp = db.get_fingerprint(conn, conv_id)
            if existing_fp and existing_fp == fingerprint:
                skipped += 1
                continue
            # if conversation existed but fingerprint changed, delete old rows
            if existing_fp:
                db.delete_conversation(conn, conv_id)
                updated += 1
            else:
                added += 1
            # insert conversation record
            db.upsert_conversation(conn, convo, fingerprint)
            # parse nodes and edges
            nodes_data = list(_parse_nodes(convo))
            db.bulk_insert_nodes(conn, conv_id, nodes_data)
    # record ingest run log
    conn.execute(
        """
        INSERT INTO ingest_runs (started_at, finished_at, input_path, added_conversations, updated_conversations, skipped_conversations)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (
            start_time,
            datetime.utcnow().isoformat(),
            input_path,
            added,
            updated,
            skipped,
        ),
    )
    conn.commit()
    typer.echo(f"Ingest complete. Added: {added}, updated: {updated}, skipped: {skipped}")


@app.command()
def init_db(db_path: str = typer.Option(..., help="Path to the SQLite DB")) -> None:
    """Initialise a fresh database file (creating tables if needed)."""
    conn = db.get_connection(db_path)
    db.init_db(conn)
    typer.echo(f"Initialised database at {db_path}")


@app.command()
def build(db_path: str = typer.Option(..., help="Path to the SQLite DB")) -> None:
    """Build auxiliary indices.  For this baseline implementation, all indices
    are created during initialisation, so this command simply triggers a
    VACUUM to reclaim space."""
    conn = db.get_connection(db_path)
    conn.execute("VACUUM;")
    conn.commit()
    typer.echo("Database optimised.")
