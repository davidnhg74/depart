/**
 * HR-style sample DDL + PL/SQL used by the "Try the HR sample" button
 * on the /assess page. Deliberately includes:
 *
 *   - plain CREATE TABLEs with mixed NUMBER/VARCHAR2/DATE types (Tier A)
 *   - a MERGE (Tier B — classic conversion pain)
 *   - a CONNECT BY hierarchical query (Tier B)
 *   - an AUTONOMOUS_TRANSACTION pragma (Tier C — the one that always
 *     makes CTOs wince)
 *   - DBMS_SCHEDULER job creation (Tier C)
 *
 * so the first-visit UX shows real Tier B/C risk items instead of a
 * boring zero-risk assessment.
 */
export const HR_SAMPLE = `-- depart — HR sample schema
-- Oracle DDL + PL/SQL. Paste your own to run an assessment on your schema.

CREATE TABLE hr.departments (
    department_id   NUMBER(4)    NOT NULL PRIMARY KEY,
    department_name VARCHAR2(30) NOT NULL,
    manager_id      NUMBER(6),
    location_id     NUMBER(4)
);

CREATE TABLE hr.employees (
    employee_id    NUMBER(6)    NOT NULL PRIMARY KEY,
    first_name     VARCHAR2(20),
    last_name      VARCHAR2(25) NOT NULL,
    email          VARCHAR2(25) NOT NULL,
    hire_date      DATE         NOT NULL,
    salary         NUMBER(8,2),
    manager_id     NUMBER(6),
    department_id  NUMBER(4),
    CONSTRAINT emp_dept_fk FOREIGN KEY (department_id) REFERENCES hr.departments(department_id),
    CONSTRAINT emp_mgr_fk  FOREIGN KEY (manager_id)    REFERENCES hr.employees(employee_id)
);

-- Stored procedure demonstrating the constructs that actually matter
-- for a migration: MERGE, CONNECT BY hierarchical walk, and an
-- autonomous transaction for audit logging.
CREATE OR REPLACE PROCEDURE hr.sync_employee_audit (p_emp_id IN NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_org_path VARCHAR2(2000);
BEGIN
    -- Walk the management hierarchy to build an org-path string.
    SELECT LISTAGG(last_name, ' > ') WITHIN GROUP (ORDER BY LEVEL DESC)
      INTO v_org_path
      FROM hr.employees
      START WITH employee_id = p_emp_id
      CONNECT BY PRIOR manager_id = employee_id;

    -- Upsert into the audit log.
    MERGE INTO hr.employee_audit a
    USING (SELECT p_emp_id AS emp_id, v_org_path AS org_path FROM dual) s
       ON (a.emp_id = s.emp_id)
    WHEN MATCHED THEN
        UPDATE SET a.org_path = s.org_path, a.updated_at = SYSDATE
    WHEN NOT MATCHED THEN
        INSERT (emp_id, org_path, updated_at) VALUES (s.emp_id, s.org_path, SYSDATE);

    COMMIT;
END;
/

-- Scheduled job — rebuild the audit nightly at 02:00.
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'REBUILD_EMPLOYEE_AUDIT',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN hr.sync_employee_audit(NULL); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY;BYHOUR=2',
        enabled         => TRUE
    );
END;
/
`;
