-- Targets ConstructTag.NVL — canonical mapping is COALESCE for NVL
-- and CASE WHEN ... IS NOT NULL THEN ... ELSE ... END for NVL2.

CREATE OR REPLACE FUNCTION effective_commission(
    p_emp_id IN employees.employee_id%TYPE
) RETURN NUMBER IS
    v_commission_pct employees.commission_pct%TYPE;
    v_manager_id     employees.manager_id%TYPE;
    v_status         VARCHAR2(20);
BEGIN
    SELECT commission_pct, manager_id
      INTO v_commission_pct, v_manager_id
      FROM employees
     WHERE employee_id = p_emp_id;

    -- Defaults via NVL — most common Oracle idiom in legacy code.
    v_commission_pct := NVL(v_commission_pct, 0);

    -- NVL2 — three-arg conditional based on NULL-ness.
    v_status := NVL2(v_manager_id, 'managed', 'unmanaged');

    RETURN v_commission_pct;
END effective_commission;
/
