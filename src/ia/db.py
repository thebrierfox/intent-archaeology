"""Database helpers for Intent Archaeology.

All data is stored in a single SQLite database.  Conversations, nodes
and edges are normalised into separate tables and message content is
indexed via FTS5 for fast full text search.  Additional tables log
ingest runs and persist arbitrary state (e.g., last run timestamps).
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterable, Dict, Any


def get_connection(db_path: str) -> sqlite3.Connection:
    """Return a SQLite connection with a row factory that yields dictionaries.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        A `sqlite3.Connection` object.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # enable WAL for concurrency and performance
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Initialise schema on the provided connection if it doesn't already exist."""
    cur = conn.cursor()
    # conversations table stores metadata for each chat thread
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS conversations (
            id TEXT PRIMARY KEY,
            title TEXT,
            create_time REAL,
            update_time REAL,
            current_node TEXT,
            fingerprint TEXT
        );
        """
    )
    # nodes table stores each message node and its metadata
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            parent_id TEXT,
            role TEXT,
            content TEXT,
            create_time REAL,
            FOREIGN KEY(conversation_id) REFERENCES conversations(id)
        );
        """
    )
    # edges table stores parent/child relationships
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS edges (
            conversation_id TEXT NOT NULL,
            parent_id TEXT,
            child_id TEXT,
            PRIMARY KEY(conversation_id, parent_id, child_id),
            FOREIGN KEY(conversation_id) REFERENCES conversations(id),
            FOREIGN KEY(parent_id) REFERENCES nodes(id),
            FOREIGN KEY(child_id) REFERENCES nodes(id)
        );
        """
    )
    # FTS index for message content; a separate row per node
    cur.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(
            node_id,
            conversation_id,
            content,
            tokenize='porter'
        );
        """
    )
    # ingest_runs table records each ingest operation for auditing
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            input_path TEXT,
            added_conversations INTEGER,
            updated_conversations INTEGER,
            skipped_conversations INTEGER
        );
        """
    )
    # state table for storing arbitrary key/value pairs
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    conn.commit()


def upsert_conversation(conn: sqlite3.Connection, convo: Dict[str, Any], fingerprint: str) -> None:
    """Insert or replace a conversation record.

    The fingerprint is stored to support incremental ingestion; if the
    fingerprint hasn't changed since the last run, ingestion can skip
    processing the mapping entirely.

    Args:
        conn: Database connection.
        convo: Dict with keys id, title, create_time, update_time, current_node.
        fingerprint: A hash string summarising key properties of this
            conversation.
    """
    conn.execute(
        """
        INSERT INTO conversations (id, title, create_time, update_time, current_node, fingerprint)
        VALUES (:id, :title, :create_time, :update_time, :current_node, :fingerprint)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,
            create_time=excluded.create_time,
            update_time=excluded.update_time,
            current_node=excluded.current_node,
            fingerprint=excluded.fingerprint;
        """,
        {
            "id": convo["id"],
            "title": convo.get("title"),
            "create_time": convo.get("create_time"),
            "update_time": convo.get("update_time"),
            "current_node": convo.get("current_node"),
            "fingerprint": fingerprint,
        },
    )
    conn.commit()


def delete_conversation(conn: sqlite3.Connection, conversation_id: str) -> None:
    """Delete all rows associated with the conversation from nodes, edges and fts."""
    cur = conn.cursor()
    # delete from nodes and edges cascades due to foreign keys (but we still remove edges explicitly for clarity)
    cur.execute("DELETE FROM edges WHERE conversation_id = ?", (conversation_id,))
    cur.execute("DELETE FROM nodes WHERE conversation_id = ?", (conversation_id,))
    cur.execute("DELETE FROM fts_messages WHERE conversation_id = ?", (conversation_id,))
    cur.execute("DELETE FROM conversations WHERE id = ?", (conversation_id,))
    conn.commit()


def bulk_insert_nodes(conn: sqlite3.Connection, conversation_id: str, nodes_data: Iterable[Dict[str, Any]]) -> None:
    """Insert nodes and edges into the database.

    Args:
        conn: Database connection.
        conversation_id: Conversation ID for all nodes in this batch.
        nodes_data: Iterable of dicts with keys: id, parent_id, children (list), role,
            content, create_time.
    """
    cur = conn.cursor()
    # prepare data for nodes and fts
    node_rows = []
    fts_rows = []
    edge_rows = []
    for nd in nodes_data:
        node_rows.append(
            (
                nd["id"],
                conversation_id,
                nd.get("parent_id"),
                nd.get("role"),
                nd.get("content"),
                nd.get("create_time"),
            )
        )
        # insert into fts only if there's content
        if nd.get("content"):
            fts_rows.append((nd["id"], conversation_id, nd["content"]))
        # edges
        for child in nd.get("children", []):
            edge_rows.append((conversation_id, nd["id"], child))
    # insert into nodes
    cur.executemany(
        """
        INSERT OR REPLACE INTO nodes (id, conversation_id, parent_id, role, content, create_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        node_rows,
    )
    # insert into edges
    if edge_rows:
        cur.executemany(
            """
            INSERT OR REPLACE INTO edges (conversation_id, parent_id, child_id)
            VALUES (?, ?, ?)
            """,
            edge_rows,
        )
    # insert into fts
    if fts_rows:
        cur.executemany(
            "INSERT INTO fts_messages (node_id, conversation_id, content) VALUES (?, ?, ?)",
            fts_rows,
        )
    conn.commit()


def get_fingerprint(conn: sqlite3.Connection, conversation_id: str) -> str | None:
    """Return the stored fingerprint for a given conversation or None."""
    cur = conn.cursor()
    row = cur.execute(
        "SELECT fingerprint FROM conversations WHERE id = ?", (conversation_id,)
    ).fetchone()
    return row["fingerprint"] if row else None


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Persist a key/value pair in the state table."""
    conn.execute(
        "INSERT INTO state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


def get_state(conn: sqlite3.Connection, key: str) -> str | None:
    """Retrieve a value from the state table."""
    cur = conn.execute("SELECT value FROM state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else None
