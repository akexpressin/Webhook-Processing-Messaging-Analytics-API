import sqlite3
import os
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./app.db")


def get_db_path():
    return DATABASE_URL.replace("sqlite:///", "")


def get_connection():
    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            message_id TEXT PRIMARY KEY,
            from_msisdn TEXT NOT NULL,
            to_msisdn TEXT NOT NULL,
            ts TEXT NOT NULL,
            text TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def insert_message(message):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO messages (message_id, from_msisdn, to_msisdn, ts, text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            message.message_id,
            message.from_,
            message.to,
            message.ts,
            message.text,
            datetime.utcnow().isoformat() + "Z"
        ))
        conn.commit()
        return "created"
    except sqlite3.IntegrityError:
        # duplicate message_id
        return "duplicate"
    finally:
        conn.close()


def list_messages(limit, offset, from_msisdn=None, since=None, q=None):
    conn = get_connection()
    cursor = conn.cursor()

    conditions = []
    params = []

    if from_msisdn:
        conditions.append("from_msisdn = ?")
        params.append(from_msisdn)

    if since:
        conditions.append("ts >= ?")
        params.append(since)

    if q:
        conditions.append("LOWER(text) LIKE ?")
        params.append(f"%{q.lower()}%")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT message_id, from_msisdn, to_msisdn, ts, text
        FROM messages
        {where_clause}
        ORDER BY ts ASC, message_id ASC
        LIMIT ? OFFSET ?
    """

    cursor.execute(query, params + [limit, offset])
    rows = cursor.fetchall()

    conn.close()
    return rows


def count_messages(from_msisdn=None, since=None, q=None):
    conn = get_connection()
    cursor = conn.cursor()

    conditions = []
    params = []

    if from_msisdn:
        conditions.append("from_msisdn = ?")
        params.append(from_msisdn)

    if since:
        conditions.append("ts >= ?")
        params.append(since)

    if q:
        conditions.append("LOWER(text) LIKE ?")
        params.append(f"%{q.lower()}%")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"SELECT COUNT(*) FROM messages {where_clause}"
    cursor.execute(query, params)

    total = cursor.fetchone()[0]
    conn.close()
    return total


def get_basic_stats():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM messages")
    total_messages = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT from_msisdn) FROM messages")
    senders_count = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(ts), MAX(ts) FROM messages")
    row = cursor.fetchone()
    first_ts = row[0]
    last_ts = row[1]

    conn.close()

    return total_messages, senders_count, first_ts, last_ts


def get_messages_per_sender():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT from_msisdn, COUNT(*) as count
        FROM messages
        GROUP BY from_msisdn
        ORDER BY count DESC
        LIMIT 10
    """)

    rows = cursor.fetchall()
    conn.close()

    return [
        {"from": r["from_msisdn"], "count": r["count"]}
        for r in rows
    ]



