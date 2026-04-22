"""Interim-vs-ANTLR parser equivalence gate.

When `_generated/` exists, both parser implementations parse the same
corpus and we assert they agree on Module shape (object kinds, names,
construct tags). The test skips when generation hasn't run — local-dev
without Java should still pass `make test`.

Once the ANTLR path is the only one that ships, this file goes away.
"""
import pytest

from src.source.oracle import _visitor
from src.source.oracle.parser import parse_with_interim


pytestmark = pytest.mark.skipif(
    not _visitor.is_available(),
    reason="ANTLR _generated/ not present; run `make grammar` to enable.",
)


CORPUS = [
    pytest.param(
        "CREATE TABLE t (id NUMBER PRIMARY KEY);",
        id="simple-table",
    ),
    pytest.param(
        "CREATE OR REPLACE VIEW v AS SELECT * FROM t;",
        id="simple-view",
    ),
    pytest.param(
        """
        CREATE OR REPLACE PROCEDURE upsert AS
        BEGIN
            MERGE INTO t USING s ON (t.id = s.id)
            WHEN MATCHED THEN UPDATE SET t.x = s.x
            WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x);
        END;
        """,
        id="proc-with-merge",
    ),
    pytest.param(
        """
        CREATE OR REPLACE PROCEDURE org AS
        BEGIN
            SELECT id FROM emp START WITH mgr IS NULL CONNECT BY PRIOR id = mgr;
        END;
        """,
        id="proc-with-connect-by",
    ),
]


def _kinds(m) -> list:
    return sorted(o.kind.value for o in m.objects if o.name != "<module-constructs>")


def _construct_tags(m) -> set:
    tags = set()
    for o in m.objects:
        for r in getattr(o, "referenced_constructs", []):
            tags.add(r.tag.value)
    return tags


@pytest.mark.parametrize("source", CORPUS)
def test_object_kinds_agree(source):
    m_interim = parse_with_interim(source)
    m_antlr = _visitor.parse_with_antlr(source)
    assert _kinds(m_interim) == _kinds(m_antlr)


@pytest.mark.parametrize("source", CORPUS)
def test_construct_tags_agree(source):
    m_interim = parse_with_interim(source)
    m_antlr = _visitor.parse_with_antlr(source)
    assert _construct_tags(m_interim) == _construct_tags(m_antlr)
