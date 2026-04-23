-- Targets ConstructTag.CONNECT_BY — canonical example becomes
-- WITH RECURSIVE CTE on the PG side.

CREATE OR REPLACE PROCEDURE print_org_subtree(p_root_id IN NUMBER) IS
BEGIN
    FOR rec IN (
        SELECT employee_id, last_name, LEVEL AS lvl
          FROM employees
         START WITH employee_id = p_root_id
         CONNECT BY PRIOR manager_id = employee_id
         ORDER SIBLINGS BY last_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(LPAD(' ', rec.lvl * 2) || rec.last_name);
    END LOOP;
END print_org_subtree;
/
