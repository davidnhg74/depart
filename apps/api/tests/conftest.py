import os

# Enable cloud routes for the test suite BEFORE any test imports
# `src.main`. Many existing tests (auth flow, billing, support) hit
# these endpoints; the product default is False but tests need the
# full surface mounted to assert behavior.
os.environ.setdefault("ENABLE_CLOUD_ROUTES", "true")

# Disable self-hosted auth by default for tests — the hundreds of
# existing tests don't authenticate, and adding Authorization headers
# everywhere would be enormous churn. A small, dedicated test file
# (`test_auth_local.py`) flips this back on at runtime to exercise the
# auth layer explicitly.
os.environ.setdefault("ENABLE_SELF_HOSTED_AUTH", "false")

import pytest
from pathlib import Path


# The bundled license public key at `src/license/public_key.pem` is
# the PRODUCTION public key — its matching private key lives off the
# laptop (see ~/.hafen-prod-keys/ and the launch runbook). Tests
# continue to mint tokens with the DEV private key at
# ~/.depart-keys/license_private_dev.pem; we patch the verifier at
# session start to accept dev-signed tokens. Production installs are
# unaffected (they always use the bundled prod key).
@pytest.fixture(autouse=True, scope="session")
def _use_dev_license_key_in_tests():
    dev_pub_path = Path.home() / ".depart-keys" / "license_public_dev.pem"
    if not dev_pub_path.exists():
        # No dev keys on this machine — license-gated tests will be
        # skipped via their own mint helpers; everything else runs.
        yield
        return

    from src.license import verifier as _verifier

    original = _verifier._load_public_key
    dev_pub_pem = dev_pub_path.read_text()
    _verifier._load_public_key = lambda: dev_pub_pem
    try:
        yield
    finally:
        _verifier._load_public_key = original


@pytest.fixture
def hr_schema_content():
    """Load Oracle HR schema for testing."""
    schema_path = Path(__file__).parent / "fixtures" / "hr_schema" / "hr_schema.sql"
    with open(schema_path, "r") as f:
        return f.read()


@pytest.fixture
def simple_procedure():
    """Simple procedure for basic testing."""
    return """
    CREATE OR REPLACE PROCEDURE simple_proc AS
    BEGIN
      NULL;
    END simple_proc;
    """


@pytest.fixture
def complex_plsql():
    """Complex PL/SQL with multiple constructs."""
    return """
    CREATE OR REPLACE PACKAGE complex_pkg AS
      PROCEDURE proc1;
      FUNCTION func1 RETURN NUMBER;
    END complex_pkg;
    /

    CREATE OR REPLACE PACKAGE BODY complex_pkg AS
      PROCEDURE proc1 AS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_id employees.employee_id%TYPE;
      BEGIN
        MERGE INTO employees e
        USING source_data s
        ON (e.employee_id = s.employee_id)
        WHEN MATCHED THEN
          UPDATE SET e.salary = s.salary
        WHEN NOT MATCHED THEN
          INSERT (employee_id, salary) VALUES (s.employee_id, s.salary);

        SELECT employee_id INTO v_id FROM employees
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id;
      END proc1;

      FUNCTION func1 RETURN NUMBER AS
      BEGIN
        RETURN 1;
      END func1;
    END complex_pkg;
    """
