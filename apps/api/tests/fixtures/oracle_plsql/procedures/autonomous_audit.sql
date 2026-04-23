-- Targets ConstructTag.AUTONOMOUS_TXN — the canonical example flags
-- this as needs-review (no PG equivalent; dblink or queue rewrite
-- required).

CREATE OR REPLACE PROCEDURE log_audit_async(p_msg VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO audit_log(msg, ts) VALUES (p_msg, SYSDATE);
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END log_audit_async;
/
