"""Sample Python repository with embedded SQL — fixture for app_impact tests."""

# Plain SQL — no findings expected.
FIND_ORDER = "SELECT id, total FROM orders WHERE id = :id"

# Oracle (+) outer join syntax — CRITICAL.
JOIN_CUSTOMER = """
    SELECT o.id, o.total, c.name
    FROM orders o, customers c
    WHERE o.customer_id = c.id(+)
"""

# Triple-single-quoted with NVL + ROWNUM — MEDIUM + HIGH.
RECENT_ORDERS = """
    SELECT id, NVL(notes, '<none>') AS notes
    FROM orders
    WHERE ROWNUM <= 50
    ORDER BY created_at DESC
"""

# Comment containing SQL keywords — must NOT be picked up as a fragment.
# SELECT this should never become a finding because it lives in a comment.

# Plain greeting — not SQL.
GREETING = "Hello"


def get_audit(cursor):
    # Touches a table that won't exist in the converted schema → CRITICAL.
    cursor.execute("SELECT * FROM legacy_audit WHERE event_at > SYSDATE - 30")


def call_proc(cursor):
    # PL/SQL block via cx_Oracle — picked up as SQL fragment.
    cursor.execute("BEGIN DBMS_SCHEDULER.CREATE_JOB(job_name => 'x'); END;")
