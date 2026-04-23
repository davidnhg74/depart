-- Targets ConstructTag.OUTER_JOIN_PLUS — canonical mapping is ANSI
-- LEFT JOIN. The split between WHERE filter and ON join condition
-- is the conversion gotcha called out in the canonical reasoning.

CREATE OR REPLACE FUNCTION employees_without_dept_count RETURN NUMBER IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
      FROM employees e, departments d
     WHERE e.department_id = d.department_id(+)
       AND d.department_name IS NULL;
    RETURN v_count;
END employees_without_dept_count;
/
