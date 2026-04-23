-- Targets ConstructTag.INTERVAL — canonical mapping drops the
-- subtype qualifier (PG has a single INTERVAL type) and translates
-- the literal syntax.

CREATE OR REPLACE FUNCTION lease_end_date(
    p_start  IN DATE,
    p_term   IN INTERVAL YEAR(2) TO MONTH
) RETURN DATE IS
BEGIN
    RETURN p_start + p_term;
END lease_end_date;
/
