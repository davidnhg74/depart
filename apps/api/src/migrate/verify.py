"""Merkle-hash verification for data movement.

Row counts catch missing rows; they don't catch corrupted ones. Hashing
each row would catch corruption but doesn't tell you *where* the
corruption is. Merkle hashing — where each batch's hash combines into a
single root — gives you both:

  • Compare roots; if they match, the entire table is bit-identical.
  • If they differ, walk the tree to find the offending batch in
    O(log n) hashes instead of O(n) row comparisons.

This module provides the pure hash plumbing. The runner is responsible
for actually reading the rows from each side and feeding row-tuples in
batch-shaped chunks. Both sides must use the same row serializer and
the same batch boundaries (defined by the keyset cursor) — otherwise
the hashes diverge for innocent reasons.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, List, Sequence


# ─── Row + batch hashing ─────────────────────────────────────────────────────


def hash_row(values: Sequence) -> bytes:
    """SHA-256 over a canonical serialization of the row.

    The canonicalization deliberately **buckets** Python types so that
    cross-driver migrations agree on the hash. The motivating case:
    oracledb returns NUMBER as `int`, psycopg returns NUMERIC as
    `Decimal`. Same logical value, but `type(v).__name__` and `repr(v)`
    differ — so a naive type-name-plus-repr scheme reports a false
    verification failure for every row of every Oracle→PG migration.

    The bucket prefixes preserve the older invariant that values of
    *different logical kinds* still hash differently (e.g. the integer
    `1` and the string ``"1"`` must not collide). What changes is that
    values of the *same* logical kind now hash the same regardless of
    which Python type the driver chose to materialize them as.

    Canonical forms:
      • ``None``        → ``b"NULL"``
      • ``bool``        → ``b"B:T"`` / ``b"B:F"``
      • numeric (int / Decimal / float) → ``b"N:<canonical>"`` where
        the canonical is the value rendered as a fixed-point decimal
        string with trailing zeros stripped (so ``10``, ``Decimal(10)``,
        ``Decimal("10.0")``, and ``10.0`` all share one hash)
      • bytes-like     → ``b"X:<lowercase hex>"``
      • ``str``         → ``b"S:" + repr(v).encode()`` — repr() escapes
        embedded NULs so the NUL field separator below stays unambiguous
      • datetime types → ``b"DT:..."`` / ``b"D:..."`` / ``b"T:..."`` via
        isoformat (tz-aware values stay distinct from naive ones)
      • anything else  → ``b"R:<typename>:<repr>"`` — falls back to the
        legacy per-type-name repr, since we can't safely canonicalize
        what we don't recognize

    The row is prefixed with the column count to defeat the ambiguity
    between, say, ``(None,)`` and ``(None, None)`` collapsing to the
    same byte stream.

    LOB note: oracledb returns CLOB/BLOB as ``oracledb.LOB`` objects
    that need ``.read()`` to materialize. They currently fall into the
    ``R:`` fallback — which produces a fresh address every read —
    so verification will *not* match for tables with LOB columns.
    Fix is to materialize LOBs at row-read time in the runner; tracked
    separately.
    """
    parts: List[bytes] = [str(len(values)).encode()]
    for v in values:
        parts.append(_canonical(v))
    return hashlib.sha256(b"\x00".join(parts)).digest()


def _canonical(v) -> bytes:
    """Render a single column value as its canonical hash input. The
    type checks are ordered so that subclasses match their most
    specific bucket first — `bool` is checked before `int` because
    ``isinstance(True, int)`` is True and we want True/1 to differ."""
    if v is None:
        return b"NULL"
    if isinstance(v, bool):
        return b"B:T" if v else b"B:F"
    if isinstance(v, (int, Decimal, float)):
        return b"N:" + _canonical_numeric(v).encode()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return b"X:" + bytes(v).hex().encode()
    if isinstance(v, str):
        # repr() preserves the embedded-NUL safety the original code
        # relied on. Bare UTF-8 would collide on `"a\x00b"` vs the
        # NUL field separator we use below.
        return b"S:" + repr(v).encode()
    # datetime is a subclass of date — check the more-specific one
    # first, otherwise both naive datetimes land in the date bucket.
    if isinstance(v, _dt.datetime):
        return b"DT:" + v.isoformat().encode()
    if isinstance(v, _dt.date):
        return b"D:" + v.isoformat().encode()
    if isinstance(v, _dt.time):
        return b"T:" + v.isoformat().encode()
    # Unknown type — fall back to the legacy per-type repr. Anything
    # routed here will hash differently between drivers if the drivers
    # return different Python classes for the same column. That's a
    # signal the caller needs a coercer at the read layer.
    return b"R:" + type(v).__name__.encode() + b":" + repr(v).encode()


def _canonical_numeric(v) -> str:
    """Reduce int / Decimal / float to a single canonical decimal string.

    `Decimal.normalize()` strips trailing zeros but renders some values
    in exponent form (`Decimal('10').normalize() == Decimal('1E+1')`).
    We re-render with `format(_, 'f')` so the canonical form is always
    plain fixed-point. Zero is special-cased because normalize() of a
    high-exponent zero still keeps the exponent.
    """
    if isinstance(v, float):
        # `Decimal(float)` exposes the binary representation noise
        # (Decimal(0.1) == 0.1000000000000000055...). Going through str()
        # gives Decimal('0.1') — what the user actually wrote.
        d = Decimal(str(v))
    elif isinstance(v, Decimal):
        d = v
    else:  # int (and bool, but bool was filtered out above)
        d = Decimal(v)
    if d == 0:
        return "0"
    return format(d.normalize(), "f")


def hash_batch(rows: Iterable[Sequence]) -> bytes:
    """Compose row hashes into a single batch hash. We chain via SHA-256
    (`H(prev || H(row))`) so any reordering changes the result — which
    is correct, since both sides walk the table by the same keyset
    order."""
    h = hashlib.sha256()
    h.update(b"BATCH")  # domain separator
    for row in rows:
        h.update(hash_row(row))
    return h.digest()


# ─── Merkle tree over batches ────────────────────────────────────────────────


def merkle_root(batch_hashes: Sequence[bytes]) -> bytes:
    """Build a binary Merkle tree from `batch_hashes` and return the
    root. Empty input → SHA-256 of the empty marker. Odd-length levels
    duplicate the last hash (Bitcoin-style) — simple and avoids the
    distinct-empty-leaf trap."""
    if not batch_hashes:
        return hashlib.sha256(b"EMPTY").digest()

    level: List[bytes] = list(batch_hashes)
    while len(level) > 1:
        nxt: List[bytes] = []
        for i in range(0, len(level), 2):
            left = level[i]
            right = level[i + 1] if i + 1 < len(level) else left
            nxt.append(hashlib.sha256(left + right).digest())
        level = nxt
    return level[0]


@dataclass
class TableHash:
    """Per-table verification artifact. `row_count` lets the caller make
    a cheap pre-check; `root` is the Merkle root for the full bitwise
    check."""

    row_count: int
    root: bytes

    def matches(self, other: "TableHash") -> bool:
        return self.row_count == other.row_count and self.root == other.root


def hash_table(batches: Iterable[Iterable[Sequence]]) -> TableHash:
    """Compose batch-by-batch row data into a TableHash. Iterates
    `batches` once — works with generators that stream from the DB."""
    batch_hashes: List[bytes] = []
    total_rows = 0
    for batch in batches:
        materialized = list(batch)
        total_rows += len(materialized)
        batch_hashes.append(hash_batch(materialized))
    return TableHash(row_count=total_rows, root=merkle_root(batch_hashes))


# ─── Diff helper for forensic mode ───────────────────────────────────────────


def find_first_divergent_batch(
    src_batch_hashes: Sequence[bytes],
    dst_batch_hashes: Sequence[bytes],
) -> int | None:
    """When the roots differ, locate the first batch index whose hash
    doesn't match. Returns None if the sequences are identical (caller
    should then use this to bisect rows inside the bad batch)."""
    common = min(len(src_batch_hashes), len(dst_batch_hashes))
    for i in range(common):
        if src_batch_hashes[i] != dst_batch_hashes[i]:
            return i
    if len(src_batch_hashes) != len(dst_batch_hashes):
        # Length mismatch — the first surplus batch is the divergence.
        return common
    return None
