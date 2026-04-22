-- Miniature HR-style schema used by the live Oracle introspector test.
--
-- The gvenzl/oracle-free image creates the APP_USER (hr) via env vars
-- before this script runs. Anything here executes as SYSTEM, so we
-- grant enough to make hr own + populate these tables, then switch
-- sessions.
--
-- Types are chosen to exercise the full introspection + type-mapping
-- matrix: NUMBER with various precisions/scales, VARCHAR2, DATE, and a
-- CLOB. FK edges are included so the planner's load-order logic is
-- exercised too.

GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO hr;

ALTER SESSION SET CURRENT_SCHEMA = HR;

CREATE TABLE hr.departments (
    department_id   NUMBER(4) NOT NULL PRIMARY KEY,
    department_name VARCHAR2(30) NOT NULL,
    manager_id      NUMBER(6),
    location_id     NUMBER(4)
);

CREATE TABLE hr.jobs (
    job_id     VARCHAR2(10) NOT NULL PRIMARY KEY,
    job_title  VARCHAR2(35) NOT NULL,
    min_salary NUMBER(6),
    max_salary NUMBER(6)
);

CREATE TABLE hr.employees (
    employee_id    NUMBER(6) NOT NULL PRIMARY KEY,
    first_name     VARCHAR2(20),
    last_name      VARCHAR2(25) NOT NULL,
    email          VARCHAR2(25) NOT NULL,
    hire_date      DATE NOT NULL,
    job_id         VARCHAR2(10) NOT NULL,
    salary         NUMBER(8,2),
    commission_pct NUMBER(2,2),
    manager_id     NUMBER(6),
    department_id  NUMBER(4),
    bio            CLOB,
    CONSTRAINT emp_dept_fk FOREIGN KEY (department_id) REFERENCES hr.departments(department_id),
    CONSTRAINT emp_job_fk  FOREIGN KEY (job_id)        REFERENCES hr.jobs(job_id),
    CONSTRAINT emp_mgr_fk  FOREIGN KEY (manager_id)    REFERENCES hr.employees(employee_id)
);

INSERT INTO hr.departments (department_id, department_name, manager_id, location_id) VALUES (10, 'Administration', NULL, 1700);
INSERT INTO hr.departments (department_id, department_name, manager_id, location_id) VALUES (20, 'Engineering',    NULL, 1800);
INSERT INTO hr.departments (department_id, department_name, manager_id, location_id) VALUES (30, 'Sales',          NULL, 1900);

INSERT INTO hr.jobs (job_id, job_title, min_salary, max_salary) VALUES ('AD_ASST',  'Administration Assistant', 30000, 60000);
INSERT INTO hr.jobs (job_id, job_title, min_salary, max_salary) VALUES ('IT_PROG',  'Programmer',               50000, 140000);
INSERT INTO hr.jobs (job_id, job_title, min_salary, max_salary) VALUES ('SA_REP',   'Sales Representative',     40000, 110000);

INSERT INTO hr.employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id) VALUES (1, 'Steven', 'King',    'SKING',  DATE '2020-01-15', 'IT_PROG',  100000, 20);
INSERT INTO hr.employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id, manager_id) VALUES (2, 'Neena',  'Kochhar', 'NKOCH',  DATE '2021-03-01', 'IT_PROG',   85000, 20, 1);
INSERT INTO hr.employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id, manager_id) VALUES (3, 'Lex',    'De Haan', 'LDEHAAN', DATE '2021-06-15', 'IT_PROG',   82000, 20, 1);
INSERT INTO hr.employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id, manager_id) VALUES (4, 'Alice',  'Ford',    'AFORD',  DATE '2022-09-01', 'SA_REP',    65000, 30, 1);
INSERT INTO hr.employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id, manager_id) VALUES (5, 'Bruce',  'Ernst',   'BERNST', DATE '2023-05-10', 'AD_ASST',   42000, 10, 1);

COMMIT;
