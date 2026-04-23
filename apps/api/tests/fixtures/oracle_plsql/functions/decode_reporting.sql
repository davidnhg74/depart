-- Targets ConstructTag.DECODE — canonical mapping uses CASE expr
-- WHEN form. Includes the NULL-equality gotcha noted in the
-- canonical reasoning.

CREATE OR REPLACE FUNCTION department_label(
    p_dept_id IN departments.department_id%TYPE
) RETURN VARCHAR2 IS
BEGIN
    RETURN DECODE(p_dept_id,
                  10, 'Administration',
                  20, 'Engineering',
                  30, 'Sales',
                  40, 'Operations',
                  50, 'Logistics',
                      'Other');
END department_label;
/
