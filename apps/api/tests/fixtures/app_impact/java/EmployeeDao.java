// Sample Java DAO with a mix of clean and Oracle-specific SQL.
// Used by test_app_impact.py to verify extractor + classifier behavior.
package com.example.depart.fixtures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class EmployeeDao {

    // Plain SQL — should produce no findings.
    private static final String FIND_BY_ID =
        "SELECT id, name, email FROM employees WHERE id = ?";

    // Oracle ROWNUM pagination — Tier B finding (HIGH).
    private static final String TOP_TEN =
        "SELECT * FROM employees WHERE ROWNUM <= 10";

    // NVL + SYSDATE — two MEDIUM findings.
    private static final String UPSERT_ATTEMPT =
        "UPDATE employees SET updated_at = SYSDATE, name = NVL(?, name) WHERE id = ?";

    // Hierarchical query — Tier B finding (HIGH).
    public String orgChart() {
        return "SELECT employee_id, manager_id FROM employees " +
               "START WITH manager_id IS NULL " +
               "CONNECT BY PRIOR employee_id = manager_id";
    }

    // DUAL reference — CRITICAL system reference.
    public String currentTime() {
        return "SELECT SYSDATE FROM DUAL";
    }

    // DBMS_OUTPUT inside a procedural call — CRITICAL.
    public String enableTrace() {
        return "BEGIN DBMS_OUTPUT.PUT_LINE('starting'); END;";
    }

    // Database link — CRITICAL.
    public String pullRemote() {
        return "INSERT INTO local_t SELECT * FROM remote_t@prod_link";
    }

    // Java text block (Java 15+) with a MERGE — Tier B finding.
    public String upsertEmployee() {
        return """
            MERGE INTO employees e
            USING source s ON (e.id = s.id)
            WHEN MATCHED THEN UPDATE SET e.name = s.name
            WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
            """;
    }

    // Touches a table that won't be in the parsed schema fixture.
    public String fetchAuditEvents() {
        return "SELECT * FROM legacy_audit_events WHERE created_at > SYSDATE - 7";
    }

    // Not SQL — plain string. Should not generate findings.
    private static final String GREETING = "Hello, world";
}
