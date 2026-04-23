-- Targets ConstructTag.ROWNUM — canonical mapping is LIMIT/OFFSET
-- after ORDER BY (Postgres evaluates LIMIT after sort by construction).

CREATE OR REPLACE FUNCTION recent_hires_page(
    p_offset IN NUMBER,
    p_limit  IN NUMBER
) RETURN SYS_REFCURSOR IS
    c SYS_REFCURSOR;
BEGIN
    OPEN c FOR
        SELECT *
          FROM (
            SELECT e.*, ROWNUM rn
              FROM employees e
             ORDER BY hire_date DESC
          )
         WHERE rn BETWEEN p_offset + 1 AND p_offset + p_limit;
    RETURN c;
END recent_hires_page;
/
