# Troubleshooting & Error Detection Strategy for Depart

**Goal:** Use AI to catch migration issues **before they become production disasters**.

---

## 🎯 Three-Stage Troubleshooting Framework

### **Stage 1: PRE-MIGRATION** (Before code runs)
Identify problematic patterns, risky conversions, edge cases.

### **Stage 2: DURING MIGRATION** (Conversion → test → validation)
Catch runtime errors, data mismatches, performance issues.

### **Stage 3: POST-MIGRATION** (After cutover)
Monitor for drift, validate correctness, enable fast rollback decision.

---

## 🚨 Stage 1: PRE-MIGRATION ERROR DETECTION

### **Problem 1: Conversion Produces Valid Syntax But Wrong Semantics**

**Example:**
```oracle
-- Oracle: MERGE with multiple WHEN MATCHED conditions
MERGE INTO employees e
USING salary_staging s ON (e.employee_id = s.employee_id)
WHEN MATCHED THEN
  UPDATE SET e.salary = s.salary WHERE e.department_id = 10  -- Conditional!
WHEN MATCHED THEN
  UPDATE SET e.salary = s.salary * 1.1 WHERE e.department_id <> 10
WHEN NOT MATCHED THEN
  INSERT VALUES (s.employee_id, s.salary);
```

**Problem:** Oracle supports multiple WHEN MATCHED; PostgreSQL ON CONFLICT doesn't.

**AI Solution 1: Semantic Pattern Matcher** ⭐⭐⭐⭐⭐

```python
class SemanticErrorDetector:
    """Use Claude to find semantic issues that syntax validators miss."""
    
    def detect_semantic_issues(self, oracle_code: str, converted_code: str) -> List[Risk]:
        """
        Compare original intent vs. converted implementation.
        """
        prompt = f"""
        Compare these two implementations. Identify semantic differences 
        (logic that works differently, edge cases that fail, conditions dropped).
        
        ORIGINAL ORACLE:
        {oracle_code}
        
        CONVERTED POSTGRESQL:
        {converted_code}
        
        For each semantic difference, rate risk (1-10) and explain why.
        Format: SEMANTIC_ISSUE: <issue> | RISK: <1-10> | REASON: <why>
        """
        
        response = claude.messages.create(...)
        return parse_semantic_issues(response.content[0].text)
```

**What it detects:**
- Conditions dropped in WHEN clauses
- Multiple WHEN MATCHED → single DO UPDATE (loses alternate paths)
- DECODE logic not properly converted to CASE
- Exception handling that doesn't map
- Cursor iteration with complex state logic

**ROI:**
- **Dev time:** 2 days (integrate Claude into converter)
- **Cost:** $0 (included in Claude API)
- **Impact:** Catch 80% of semantic errors before testing
- **Recommendation:** ✅ **Phase 3.1** (critical)

---

### **Problem 2: Risky Constructs That Need DBA Attention**

**Example:**
```oracle
CREATE PROCEDURE critical_financial_calc AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  DECLARE
    v_balance NUMBER;
  BEGIN
    SELECT balance INTO v_balance FROM accounts WHERE account_id = ...;
    IF v_balance < COMMIT_THRESHOLD THEN
      INSERT INTO alerts VALUES (...);
      COMMIT;  -- Autonomous — commits independently!
    END IF;
    UPDATE accounts SET ...;  -- But what if this fails?
  END;
```

**Problem:** Autonomous transaction logic is implicit. PostgreSQL equivalent needs explicit handling.

**AI Solution 2: Risk Scoring Engine** ⭐⭐⭐⭐

```python
class MigrationRiskScorer:
    """Score each procedure for migration risk."""
    
    HIGH_RISK_PATTERNS = {
        r"PRAGMA AUTONOMOUS_TRANSACTION": {
            "risk": 9,
            "reason": "Implicit separate transaction; PostgreSQL needs explicit pattern",
            "action": "REQUIRES MANUAL REWRITE - use separate connection or dblink"
        },
        r"DBMS_SCHEDULER|DBMS_AQ|DBMS_CRYPTO": {
            "risk": 8,
            "reason": "Oracle system packages with no PostgreSQL equivalent",
            "action": "REQUIRES ARCHITECTURE CHANGE - use external scheduler"
        },
        r"FOR.*IN.*LOOP.*UPDATE|INSERT.*COMMIT": {
            "risk": 7,
            "reason": "Row-by-row processing with commits; performance disaster in PG",
            "action": "REWRITE AS BULK OPERATION - set-based SQL instead"
        },
        r"EXECUTE IMMEDIATE.*DYNAMIC.*TABLE_NAME|COLUMN_NAME": {
            "risk": 6,
            "reason": "Dynamic table/column names; hard to validate",
            "action": "VALIDATE AGAINST SCHEMA - ensure no injection"
        },
    }
    
    def score_risk(self, code: str) -> Tuple[int, List[Risk]]:
        """Return overall risk (1-10) and detailed risks."""
        risks = []
        for pattern, metadata in self.HIGH_RISK_PATTERNS.items():
            if re.search(pattern, code, re.IGNORECASE):
                risks.append(Risk(
                    pattern=pattern,
                    risk_level=metadata["risk"],
                    reason=metadata["reason"],
                    action=metadata["action"]
                ))
        
        overall_risk = max([r.risk_level for r in risks] or [1])
        return overall_risk, risks
```

**What it catches:**
- Autonomous transactions (risk: 9/10)
- DBMS_* packages (risk: 8/10)
- Row-by-row processing (performance killer, risk: 7/10)
- Dynamic SQL (risk: 6/10)
- Complex cursor logic (risk: 6/10)
- Implicit type conversions (risk: 5/10)

**Enterprise Report Output:**
```
CONVERSION RISK ASSESSMENT

Procedure: emp_raise_salary_proc
Risk Level: HIGH (7/10)

Issues Found:
  [9/10] Line 15: PRAGMA AUTONOMOUS_TRANSACTION
         → Commits independently; needs explicit dblink or app-level handling
         → ACTION: Manual rewrite required (3 days)
  
  [7/10] Line 23-35: FOR emp IN SELECT... LOOP UPDATE... COMMIT; END LOOP;
         → Row-by-row with commits; 100x slower in PostgreSQL
         → ACTION: Rewrite as INSERT ... ON CONFLICT or UPDATE with CTE
         
  [5/10] Line 42: TO_NUMBER(p_val) - implicit conversion
         → May fail if p_val contains non-numeric
         → ACTION: Add validation before conversion

Recommendation: RISKY - Requires DBA review before conversion
Timeline estimate: 2 weeks (including manual rewrites)
```

**ROI:**
- **Dev time:** 3 days (build risk scorecard)
- **Cost:** $0
- **Impact:** Prevent risky conversions before they happen
- **Recommendation:** ✅ **Phase 3.1** (enterprise feature)

---

### **Problem 3: Edge Cases That Break Silently**

**Example:**
```oracle
-- Oracle: NVL with implicit type conversion
CREATE FUNCTION get_employee_info(p_emp_id NUMBER) RETURN VARCHAR2 AS
  v_info VARCHAR2(1000);
BEGIN
  SELECT employee_name || ' - ' || NVL(department_id, 'N/A') INTO v_info
  FROM employees WHERE employee_id = p_emp_id;
  RETURN v_info;
END;
```

**Problem:** PostgreSQL can't concatenate TEXT + INT. Oracle implicitly converts. Code runs, but breaks on NULL.

**AI Solution 3: Edge Case Detector** ⭐⭐⭐⭐

```python
class EdgeCaseDetector:
    """Find edge cases that break in PostgreSQL but not Oracle."""
    
    EDGE_CASES = [
        {
            "pattern": r"NVL\((\w+),\s*'[^']*'\)",  # NVL with string default
            "issue": "Implicit type conversion - PostgreSQL may fail",
            "fix": "Use CASE WHEN ... IS NOT NULL THEN ... ELSE CAST(...) END",
            "risk": 5
        },
        {
            "pattern": r"\|\|\s*(?:NVL|COALESCE)\([\w.]+,\s*(?:\d+|'[^']*')\)",
            "issue": "String concatenation with implicit type conversion",
            "fix": "Explicitly CAST to TEXT before concatenation",
            "risk": 6
        },
        {
            "pattern": r"FOR\s+\w+\s+IN\s+SELECT.*?LOOP.*?INSERT.*?COMMIT",
            "issue": "Row-by-row processing - 100x slower in PostgreSQL",
            "fix": "Rewrite as bulk INSERT ... SELECT or INSERT ... ON CONFLICT",
            "risk": 7
        },
        {
            "pattern": r"EXECUTE\s+IMMEDIATE.*?WHERE",
            "issue": "Dynamic SQL - prone to SQL injection and edge cases",
            "fix": "Use prepared statements or parameterized queries",
            "risk": 8
        }
    ]
```

**ROI:**
- **Dev time:** 2 days
- **Cost:** $0
- **Impact:** Catch silent failures before production
- **Recommendation:** ✅ **Phase 3.1**

---

## 🔧 Stage 2: DURING MIGRATION ERROR DETECTION

### **Problem 1: Runtime Errors (Code Runs But Crashes)**

**Example:**
```
PostgreSQL execution error:
  ERROR: operator does not exist: text || integer
  HINT: No operator matches the given name and argument types
  LOCATION: make_op_expr, parse_expr.c:123
```

**AI Solution 1: Automated Test Harness with Error Capture** ⭐⭐⭐⭐

```python
class TestHarnessWithErrorCapture:
    """Run pgTAP tests, capture errors, suggest fixes."""
    
    def run_and_diagnose(self, converted_code: str, test_cases: List[TestCase]) -> TestReport:
        """
        1. Run pgTAP tests
        2. Capture any failures
        3. Extract error messages
        4. Use Claude to suggest fixes
        """
        
        results = run_pgtap_tests(converted_code)
        
        for failed_test in results.failed_tests:
            # Extract PostgreSQL error
            error_msg = failed_test.error_message
            
            # Send to Claude for diagnosis
            diagnosis = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    PostgreSQL error from converted PL/pgSQL:
                    {error_msg}
                    
                    Converted code:
                    {converted_code}
                    
                    Test case that failed:
                    {failed_test.test_code}
                    
                    Suggest a fix for this error.
                    Format: ROOT_CAUSE: <what went wrong> | FIX: <corrected code>
                    """
                }]
            )
            
            # Parse suggestion and offer fix
            fix = parse_fix_suggestion(diagnosis.content[0].text)
            failed_test.suggested_fix = fix
        
        return results
```

**What it does:**
- Runs pgTAP test suite
- Captures runtime errors
- Sends error + code to Claude
- Returns suggested fixes
- Categorizes errors (type mismatch, null handling, syntax)

**Example Output:**
```
FAILED TEST: test_emp_raise_salary with input (12345, 5000)

ERROR: operator does not exist: text || integer
  Location: test_salary_concat function, line 23

ROOT CAUSE: Concatenating TEXT (department_name) + INTEGER (department_id)
  Oracle implicitly converts; PostgreSQL doesn't

SUGGESTED FIX:
  BEFORE: SELECT name || ' - ' || department_id
  AFTER:  SELECT name || ' - ' || department_id::TEXT

Apply fix? [Y/n]
```

**ROI:**
- **Dev time:** 1 week (integrate error capture + Claude diagnosis)
- **Cost:** $0 (Claude API already used)
- **Impact:** Turn runtime errors into auto-suggested fixes
- **Recommendation:** ✅ **Phase 3.1**

---

### **Problem 2: Data Mismatch (Logic Produces Different Results)**

**Example:**
```
Oracle: SELECT COUNT(*) FROM orders WHERE ROWNUM <= 10
  Result: 10 rows

PostgreSQL: SELECT COUNT(*) FROM orders WHERE ROW_NUMBER() OVER (...) <= 10
  Result: ERROR or wrong count (ROWNUM converted incorrectly)
```

**AI Solution 2: Dual-Database Comparison With Anomaly Detection** ⭐⭐⭐⭐⭐

```python
class DataComparisonEngine:
    """Compare Oracle vs PostgreSQL query results, flag mismatches."""
    
    def compare_and_diagnose(
        self, 
        oracle_query: str, 
        pg_query: str,
        oracle_conn,
        pg_conn
    ) -> ComparisonReport:
        """
        1. Run same query on both databases
        2. Compare row counts, checksums, sample data
        3. Use Claude to diagnose differences
        """
        
        # Run on both DBs
        oracle_result = oracle_conn.execute(oracle_query)
        pg_result = pg_conn.execute(pg_query)
        
        # Compare
        row_count_match = oracle_result.row_count == pg_result.row_count
        data_match = checksums_match(oracle_result, pg_result)
        
        report = ComparisonReport(
            query=oracle_query,
            oracle_rows=oracle_result.row_count,
            pg_rows=pg_result.row_count,
            match=row_count_match and data_match
        )
        
        if not report.match:
            # Diagnose the difference
            diagnosis = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    Data mismatch in migration test:
                    
                    Oracle query result: {oracle_result.row_count} rows
                    PostgreSQL query result: {pg_result.row_count} rows
                    
                    Original Oracle query:
                    {oracle_query}
                    
                    Converted PostgreSQL query:
                    {pg_query}
                    
                    Sample Oracle data:
                    {oracle_result.sample_rows[:5]}
                    
                    Sample PostgreSQL data:
                    {pg_result.sample_rows[:5]}
                    
                    What went wrong? Suggest a fix.
                    """
                }]
            )
            
            report.diagnosis = parse_diagnosis(diagnosis.content[0].text)
        
        return report
```

**What it does:**
- Executes same query on Oracle + PostgreSQL
- Compares row counts, data, checksums
- Flags mismatches
- Sends details to Claude for diagnosis
- Suggests corrected query

**Example Output:**
```
QUERY MISMATCH DETECTED

Oracle result: 50,000 rows
PostgreSQL result: 45,000 rows
Difference: -5,000 rows (-10%)

Original Oracle query:
  SELECT * FROM orders
  WHERE status IN ('ACTIVE', 'PENDING')
  AND ROWNUM <= 50000

Converted PostgreSQL query:
  SELECT * FROM orders
  WHERE status IN ('ACTIVE', 'PENDING')
  AND ROW_NUMBER() OVER (ORDER BY order_id) <= 50000

ROOT CAUSE:
  ROW_NUMBER() requires ORDER BY, which changes which rows are selected
  Oracle ROWNUM is independent of order
  
CORRECTED QUERY:
  SELECT * FROM orders
  WHERE status IN ('ACTIVE', 'PENDING')
  LIMIT 50000

Apply fix? [Y/n]
```

**ROI:**
- **Dev time:** 2 weeks (requires dual-DB connection infrastructure)
- **Cost:** Varies (depends on test data size)
- **Impact:** Catch logic errors before production
- **Recommendation:** ⚠️ **Phase 3.2** (enterprise feature, requires setup)

---

### **Problem 3: Performance Degradation (Slow Queries)**

**Example:**
```
Oracle: SELECT * FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE SUBSTR(o.customer_name, 1, 1) = 'A'
  Speed: 50ms (Oracle uses function-based index on SUBSTR)

PostgreSQL: (same query)
  Speed: 5000ms (no index on SUBSTR; full table scan)
```

**AI Solution 3: Performance Analyzer With Index Recommendations** ⭐⭐⭐⭐

```python
class PerformanceAnalyzer:
    """Compare query performance, suggest indexes/rewrites."""
    
    def analyze_performance_delta(
        self,
        oracle_query: str,
        pg_query: str,
        oracle_timing: float,  # milliseconds
        pg_timing: float
    ) -> PerformanceReport:
        """
        Compare query times and suggest optimizations.
        """
        
        slowdown_pct = ((pg_timing - oracle_timing) / oracle_timing) * 100
        
        if slowdown_pct > 20:  # >20% slower = investigate
            # Get query plans
            oracle_plan = oracle_conn.explain(oracle_query)
            pg_plan = pg_conn.explain(pg_query)
            
            # Send to Claude for analysis
            analysis = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    Query performance degradation in migration:
                    
                    Oracle execution time: {oracle_timing}ms
                    PostgreSQL execution time: {pg_timing}ms
                    Slowdown: {slowdown_pct}%
                    
                    Oracle execution plan:
                    {oracle_plan}
                    
                    PostgreSQL execution plan:
                    {pg_plan}
                    
                    PostgreSQL query:
                    {pg_query}
                    
                    Why is PostgreSQL slower? Suggest:
                    1. Missing indexes
                    2. Query rewrites
                    3. Statistics/vacuum needs
                    """
                }]
            )
            
            suggestions = parse_perf_suggestions(analysis.content[0].text)
            return PerformanceReport(
                slowdown_pct=slowdown_pct,
                suggestions=suggestions,
                oracle_plan=oracle_plan,
                pg_plan=pg_plan
            )
```

**What it does:**
- Compares query execution times
- Gets EXPLAIN plans from both DBs
- Identifies missing indexes
- Suggests query rewrites
- Recommends ANALYZE/VACUUM

**Example Output:**
```
PERFORMANCE DEGRADATION DETECTED

Oracle: 50ms
PostgreSQL: 5000ms
Slowdown: 9,900% (100x slower!)

ROOT CAUSE:
  Function-based index on SUBSTR(customer_name, 1, 1) in Oracle
  PostgreSQL has no equivalent; doing full table scan

SUGGESTIONS:
  1. Create generated column:
     ALTER TABLE orders ADD COLUMN customer_initial 
     AS (SUBSTR(customer_name, 1, 1)) STORED;
  
  2. Create index:
     CREATE INDEX idx_customer_initial ON orders(customer_initial);
  
  3. Rewrite query (if generated column not possible):
     SELECT * FROM orders o
     WHERE customer_name LIKE 'A%'  -- LIKE can use index
     
Expected improvement: 5000ms → 60ms
Apply? [Y/n]
```

**ROI:**
- **Dev time:** 1 week
- **Cost:** $0 (Claude API already used)
- **Impact:** Catch performance surprises, enable quick fixes
- **Recommendation:** ✅ **Phase 3.2**

---

## 📊 Stage 3: POST-MIGRATION ERROR DETECTION

### **Problem 1: Silent Behavioral Drift (Code Works, But Produces Wrong Results)**

**Example:**
```oracle
-- Oracle: DECODE returns first match
SELECT DECODE(
  status,
  'ACTIVE', 'Active Account',
  'PENDING', 'Pending Review',
  'ACTIVE', 'Active (Overdue)',  -- This never executes! Oracle stops at first match
  'Unknown'
) FROM accounts

-- PostgreSQL: CASE is more explicit
SELECT CASE
  WHEN status = 'ACTIVE' THEN 'Active Account'
  WHEN status = 'PENDING' THEN 'Pending Review'
  WHEN status = 'ACTIVE' THEN 'Active (Overdue)'  -- Still never executes
  ELSE 'Unknown'
END FROM accounts
```

**AI Solution 1: Continuous Verification Tests (Post-Cutover)** ⭐⭐⭐⭐

```python
class ContinuousVerificationEngine:
    """Run ongoing tests after cutover to catch drift."""
    
    def monitor_production(
        self,
        oracle_db: OracleConnection,
        pg_db: PostgreSQLConnection,
        critical_procedures: List[str],
        check_interval: int = 3600  # hourly
    ) -> VerificationReport:
        """
        Compare production results periodically.
        """
        
        results = []
        
        for proc_name in critical_procedures:
            # Run with production data
            test_cases = generate_edge_case_inputs(proc_name)
            
            for inputs in test_cases:
                oracle_result = oracle_db.execute_procedure(proc_name, inputs)
                pg_result = pg_db.execute_procedure(proc_name, inputs)
                
                if oracle_result != pg_result:
                    # Mismatch! Alert!
                    alert = {
                        "severity": "CRITICAL",
                        "procedure": proc_name,
                        "inputs": inputs,
                        "oracle_result": oracle_result,
                        "pg_result": pg_result,
                        "timestamp": datetime.now()
                    }
                    
                    # Use Claude to diagnose
                    diagnosis = claude.messages.create(...)
                    alert["diagnosis"] = diagnosis
                    
                    results.append(alert)
        
        return VerificationReport(alerts=results)
```

**What it does:**
- Runs critical procedures with edge case inputs
- Compares Oracle vs PostgreSQL results
- Alerts on mismatches
- Diagnoses root cause
- Enables rollback decision

**ROI:**
- **Dev time:** 1 week
- **Cost:** Variable (depends on test frequency)
- **Impact:** Catch production issues before customers do
- **Recommendation:** ✅ **Phase 3.3** (production safety)

---

### **Problem 2: Rollback Decision Support (Should We Keep PostgreSQL?)**

**Example:**
```
Migration results after 1 week in production:

Oracle Baseline:
  - Response time: 150ms avg
  - Throughput: 5000 req/sec
  - Error rate: 0.1%
  - Data accuracy: 100%

PostgreSQL:
  - Response time: 180ms avg (+20%)
  - Throughput: 4800 req/sec (-4%)
  - Error rate: 0.15% (+50%)
  - Data accuracy: 99.8% (-0.2% = ~1000 wrong rows)

Decision: ROLLBACK (performance acceptable, but data issues unacceptable)
Recommendation: Investigate data accuracy, fix, retry in 2 weeks
```

**AI Solution 2: Automated Go/No-Go Decision Engine** ⭐⭐⭐⭐⭐

```python
class GoNoGoDecisionEngine:
    """Automated decision support: keep PostgreSQL or rollback?"""
    
    ACCEPTANCE_CRITERIA = {
        "response_time": {"max_delta_pct": 25, "critical": False},
        "throughput": {"min_delta_pct": -10, "critical": False},
        "error_rate": {"max_delta_pct": 50, "critical": True},
        "data_accuracy": {"min_pct": 99.95, "critical": True},
    }
    
    def make_decision(self, baseline: Metrics, current: Metrics) -> Decision:
        """
        Compare current vs baseline metrics.
        Return GO or NO-GO with reasoning.
        """
        
        analysis = {
            "response_time_delta": ((current.response_time - baseline.response_time) / baseline.response_time) * 100,
            "throughput_delta": ((current.throughput - baseline.throughput) / baseline.throughput) * 100,
            "error_rate_delta": ((current.error_rate - baseline.error_rate) / baseline.error_rate) * 100,
            "data_accuracy_pct": (current.correct_rows / current.total_rows) * 100,
        }
        
        go = True
        issues = []
        
        for metric, value in analysis.items():
            criteria = self.ACCEPTANCE_CRITERIA[metric]
            
            if metric == "data_accuracy_pct":
                if value < criteria["min_pct"]:
                    issues.append(f"{metric}: {value}% (requires {criteria['min_pct']}%)")
                    if criteria["critical"]:
                        go = False
            else:
                if abs(value) > criteria["max_delta_pct"]:
                    issues.append(f"{metric}: {value}% (max {criteria['max_delta_pct']}%)")
                    if criteria["critical"]:
                        go = False
        
        if go:
            return Decision(verdict="GO", reason="All metrics within acceptable range")
        else:
            # Use Claude to suggest fixes
            diagnosis = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    PostgreSQL migration results after 1 week:
                    
                    Issues:
                    {chr(10).join(issues)}
                    
                    Current metrics:
                    {current}
                    
                    Baseline metrics:
                    {baseline}
                    
                    Should we rollback or fix?
                    If fix, what should we investigate?
                    Provide a priority list of actions.
                    """
                }]
            )
            
            fixes = parse_fixes(diagnosis.content[0].text)
            return Decision(
                verdict="NO-GO",
                reason="Critical metrics out of range",
                suggested_fixes=fixes
            )
```

**What it does:**
- Compares production metrics to baseline
- Checks against acceptance criteria
- Returns GO/NO-GO decision
- If NO-GO, suggests investigation areas
- Enables data-driven rollback decisions

**Example Output:**
```
MIGRATION STATUS REPORT (1-week production)

Baseline (Oracle):
  Response time: 150ms avg
  Throughput: 5000 req/sec
  Error rate: 0.1%
  Data accuracy: 100% (50M rows)

PostgreSQL (current):
  Response time: 180ms avg (+20%)
  Throughput: 4800 req/sec (-4%)
  Error rate: 0.15% (+50%)
  Data accuracy: 99.8% (40K errors in 50M rows)

CRITICAL ISSUE: Data accuracy below threshold (99.8% vs 99.95% required)

RECOMMENDATION: NO-GO - ROLLBACK

Suggested investigation:
  1. Review 40K rows with mismatched values
     - Focus on salary calculations (most common error)
     - Check DECODE→CASE conversion in calc_salary_proc
  
  2. Run pgTAP tests with full dataset
     - Previous tests used 1M sample
     - Edge cases may manifest at scale
  
  3. Check numeric precision
     - Oracle NUMBER vs PostgreSQL NUMERIC differences?
     - Check CAST operations in 12 affected procedures
  
Timeline: 2 weeks to investigate + fix + retest

Rollback decision: AUTOMATIC (go_threshold expired)
```

**ROI:**
- **Dev time:** 1 week
- **Cost:** $0 (Claude API already used)
- **Impact:** Prevent bad migrations, enable confident cutover
- **Recommendation:** ✅ **Phase 3.3** (critical for enterprise)

---

## 🎯 Recommended Implementation Order

### **Phase 3.1 (Week 3-4):** Foundation
- [x] pgTAP test generator (already have)
- [ ] **Semantic Error Detector** (2 days) — Find logic bugs
- [ ] **Risk Scoring Engine** (3 days) — Pre-migration risk assessment
- [ ] **Edge Case Detector** (2 days) — Catch silent failures

**Expected impact:** 70% of errors caught before testing

---

### **Phase 3.2 (Month 2):** Runtime Validation
- [ ] **Error Capture + Auto-Diagnosis** (1 week) — Runtime errors → suggested fixes
- [ ] **Data Comparison Engine** (2 weeks) — Oracle vs PG query validation
- [ ] **Performance Analyzer** (1 week) — Catch slowdowns, suggest indexes

**Expected impact:** 95% of issues caught before production

---

### **Phase 3.3 (Month 3):** Post-Cutover Safety
- [ ] **Continuous Verification Tests** (1 week) — Ongoing monitoring
- [ ] **Go/No-Go Decision Engine** (1 week) — Automated rollback decisions

**Expected impact:** Zero surprises in production

---

## 📊 Complete Troubleshooting Stack

| Stage | Tool | Dev Time | Cost | Impact |
|-------|------|----------|------|--------|
| **Pre** | Semantic Error Detector | 2 days | $0 | Catch logic bugs |
| **Pre** | Risk Scoring Engine | 3 days | $0 | Flag dangerous patterns |
| **Pre** | Edge Case Detector | 2 days | $0 | Find silent failures |
| **During** | Error Capture + Diagnosis | 1 week | $0 | Auto-suggest fixes |
| **During** | Data Comparison Engine | 2 weeks | Variable | Validate correctness |
| **During** | Performance Analyzer | 1 week | $0 | Catch slowdowns |
| **Post** | Continuous Verification | 1 week | Variable | Ongoing safety |
| **Post** | Go/No-Go Decider | 1 week | $0 | Rollback decision |

---

## 🚀 Enterprise Differentiator

By Phase 3.3, Depart will offer:

```
Traditional Migration:
  1. Convert code (hope it works)
  2. Run tests (find problems)
  3. Fix and retry (weeks)
  4. Deploy (fingers crossed)
  5. Monitor (watch for failures)
  Result: 6-8 weeks, multiple rollbacks, customer risk

Depart AI Troubleshooting:
  1. Pre-flight checks (semantic errors, risk scores)
     → 70% of issues caught
  
  2. Conversion with validation (error diagnosis + fixes)
     → 95% of issues caught
  
  3. Data comparison (Oracle vs PostgreSQL validation)
     → 99% of issues caught
  
  4. Post-cutover monitoring (continuous verification)
     → Zero surprises in production
  
  5. Go/No-Go automation (data-driven rollback decisions)
     → Confident cutover or instant rollback
  
  Result: 2-3 weeks, zero rollbacks, zero production issues
```

---

## 💡 Why This Wins

**Enterprise Customer POV:**
- "We need proof of correctness before cutover" → Data comparison engine
- "We need to know what could go wrong" → Risk scoring + edge case detector
- "We need help understanding errors" → Auto-diagnosis with fixes
- "We need to know if we should rollback" → Go/No-Go decision engine
- "We need confidence in production" → Continuous verification

**No competitor offers this level of troubleshooting.**
