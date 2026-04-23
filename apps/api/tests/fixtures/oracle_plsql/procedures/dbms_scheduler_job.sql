-- Targets ConstructTag.DBMS_SCHEDULER — canonical translation uses
-- pg_cron's `cron.schedule(...)`.

BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'REBUILD_EMPLOYEE_AUDIT',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN hr.sync_employee_audit(NULL); END;',
        repeat_interval => 'FREQ=DAILY;BYHOUR=2',
        enabled         => TRUE
    );
END;
/
