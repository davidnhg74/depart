-- Targets ConstructTag.MERGE — the canonical _EXAMPLES entry maps to
-- INSERT ... ON CONFLICT DO UPDATE. A real-world MERGE upsert pattern.

CREATE OR REPLACE PROCEDURE upsert_employee_audit(
    p_emp_id   IN employee_audit.emp_id%TYPE,
    p_org_path IN employee_audit.org_path%TYPE
) IS
BEGIN
    MERGE INTO employee_audit a
    USING (SELECT p_emp_id AS emp_id, p_org_path AS org_path FROM dual) s
       ON (a.emp_id = s.emp_id)
    WHEN MATCHED THEN
        UPDATE SET a.org_path = s.org_path,
                   a.updated_at = SYSDATE
    WHEN NOT MATCHED THEN
        INSERT (emp_id, org_path, updated_at)
        VALUES (s.emp_id, s.org_path, SYSDATE);
    COMMIT;
END upsert_employee_audit;
/
