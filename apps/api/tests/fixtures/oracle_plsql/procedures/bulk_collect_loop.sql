-- Targets ConstructTag.BULK_COLLECT — the canonical example translates
-- to a PG ARRAY subselect + FOREACH loop, with the recommendation to
-- prefer a set-based rewrite for >10K rows.

CREATE OR REPLACE PROCEDURE process_department_names(p_dept_id IN NUMBER) IS
    TYPE name_tab IS TABLE OF employees.last_name%TYPE;
    v_names name_tab;
BEGIN
    SELECT last_name BULK COLLECT INTO v_names
      FROM employees
     WHERE department_id = p_dept_id;

    FOR i IN 1..v_names.COUNT LOOP
        do_something(v_names(i));
    END LOOP;
END process_department_names;
/
