-- Minimal schema fixture for app-impact cross-reference.
-- `employees`, `orders`, `customers` are known; `legacy_audit*` are NOT.

CREATE TABLE employees (
    id           NUMBER PRIMARY KEY,
    name         VARCHAR2(255),
    email        VARCHAR2(255),
    manager_id   NUMBER,
    created_at   DATE,
    updated_at   DATE
);

CREATE TABLE orders (
    id            NUMBER PRIMARY KEY,
    customer_id   NUMBER,
    total         NUMBER(12,2),
    notes         VARCHAR2(4000),
    created_at    DATE
);

CREATE TABLE customers (
    id     NUMBER PRIMARY KEY,
    name   VARCHAR2(255),
    email  VARCHAR2(255)
);
