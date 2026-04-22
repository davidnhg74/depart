# Performance Benchmarking & Troubleshooting Strategy

**Problem:** Enterprise migrations fail because PostgreSQL is "too slow."  
**Root causes:**
- Oracle uses function-based indexes; PostgreSQL doesn't (missing indexes)
- Oracle optimizer hints; PostgreSQL has no hints (different plans)
- Oracle NLS_SORT affects ORDER BY; PostgreSQL uses COLLATE (wrong sorting)
- Oracle full table scans cached in memory; PostgreSQL starts cold
- Missing stats or autovacuum not tuned (stale data)

**Solution:** AI-powered performance benchmarking + continuous optimization

---

## 🎯 Three-Stage Performance Strategy

### **Stage 1: PRE-MIGRATION BENCHMARKING** (Establish baseline)

#### **Goal:** Know expected performance before conversion

#### **Problem 1: "We don't have production workload to test with"**

#### **AI Solution 1: Intelligent Workload Generator** ⭐⭐⭐⭐⭐

```python
class WorkloadGenerator:
    """Generate realistic Oracle workload from schema + code analysis."""
    
    def generate_workload(oracle_schema: Schema, procedures: List[str]) -> Workload:
        """
        Analyze procedures → extract query patterns → generate test workload.
        """
        
        # Step 1: Extract query patterns from procedures
        patterns = []
        for proc in procedures:
            queries = extract_sql(proc)
            for query in queries:
                pattern = {
                    "type": classify_query(query),  # SELECT, INSERT, UPDATE, MERGE
                    "tables": extract_tables(query),
                    "joins": count_joins(query),
                    "where_conditions": extract_conditions(query),
                    "aggregations": extract_aggregations(query),
                    "frequency": estimate_frequency(proc)  # called 100x/day?
                }
                patterns.append(pattern)
        
        # Step 2: Analyze data distribution from schema
        cardinality = analyze_schema(oracle_schema)
        # employees table: 500K rows, hire_date ranges 2010-2024
        # orders table: 50M rows, status: 30% ACTIVE, 50% CLOSED, 20% PENDING
        
        # Step 3: Generate test data matching distribution
        test_data = generate_data(cardinality, size_gb=10)  # 10GB test dataset
        
        # Step 4: Create workload script
        workload_script = f"""
        -- Generated workload from {len(procedures)} procedures
        -- Expected: {sum(p['frequency'] for p in patterns)}/day in production
        
        {render_workload(patterns, test_data)}
        """
        
        return workload_script
```

**What it does:**
- Parse all procedures in package
- Extract query patterns (SELECT/INSERT/UPDATE, joins, aggregations)
- Analyze schema cardinality
- Generate realistic test data (~10GB)
- Create benchmark workload script

**Example:**
```
Input: Oracle HR package (50 procedures, 8 tables)
Output: 10GB test dataset + workload script with:
  - 500K employees (varying hire dates)
  - 50M salary history records
  - 200K departments
  - Same query patterns as original procedures
  - Realistic concurrency (50 concurrent sessions)
  - Frequency-weighted queries
```

**ROI:**
- **Dev time:** 2 days
- **Cost:** $0 (Claude + schema analysis)
- **Impact:** Know expected performance BEFORE migration
- **Recommendation:** ✅ **Phase 3.1** (foundational)

---

#### **Problem 2: "We don't know what 'acceptable' performance is"**

#### **AI Solution 2: Baseline Profiler + Performance Targets** ⭐⭐⭐⭐

```python
class PerformanceBaseliner:
    """Establish Oracle baseline + set PostgreSQL targets."""
    
    def establish_baseline(oracle_workload: Workload) -> BaselineReport:
        """
        Run workload against Oracle, measure everything.
        """
        
        metrics = {
            "queries": {},
            "overall": {}
        }
        
        # Run each query, measure performance
        for query in oracle_workload.queries:
            result = {
                "execution_time_ms": measure_time(query),
                "rows_returned": measure_rows(query),
                "buffer_gets": measure_buffer_hits(query),
                "disk_reads": measure_disk_io(query),
                "cpu_ms": measure_cpu(query),
                "memory_mb": measure_memory(query),
                "plan": get_execution_plan(query),
            }
            metrics["queries"][query.id] = result
        
        # Overall workload metrics
        metrics["overall"] = {
            "total_queries": len(oracle_workload.queries),
            "total_time_ms": sum(q["execution_time_ms"] for q in metrics["queries"].values()),
            "avg_response_time_ms": avg(q["execution_time_ms"] for q in metrics["queries"].values()),
            "p99_response_time_ms": percentile(metrics["queries"].values(), 0.99),
            "throughput_queries_per_sec": measure_throughput(oracle_workload),
            "concurrency_50_response_time": measure_under_load(oracle_workload, 50),
        }
        
        return BaselineReport(metrics)
    
    def set_postgresql_targets(baseline: BaselineReport) -> TargetReport:
        """
        Convert Oracle baseline to PostgreSQL targets.
        """
        
        # Rule of thumb: allow 20% slowdown for most queries
        # Allow 50% slowdown for complex analytical queries
        # Allow 10% improvement for simple transactional queries
        
        targets = {}
        for query_id, oracle_metrics in baseline.queries.items():
            query_complexity = classify_complexity(query_id)
            
            if query_complexity == "SIMPLE":
                target_time = oracle_metrics["execution_time_ms"] * 1.1  # 10% slower acceptable
            elif query_complexity == "COMPLEX_ANALYTICAL":
                target_time = oracle_metrics["execution_time_ms"] * 1.5  # 50% slower acceptable
            else:
                target_time = oracle_metrics["execution_time_ms"] * 1.2  # 20% slower acceptable
            
            targets[query_id] = {
                "oracle_baseline_ms": oracle_metrics["execution_time_ms"],
                "target_ms": target_time,
                "max_acceptable_ms": target_time * 1.1,  # 10% margin
                "acceptable": True
            }
        
        return TargetReport(targets)
```

**Example Output:**
```
PERFORMANCE BASELINE REPORT

Oracle Baseline (10GB workload, 50 concurrent users):
  
  SELECT queries (1000 total):
    - Avg: 45ms
    - P99: 200ms
    - P99.9: 500ms
  
  INSERT/UPDATE queries (500 total):
    - Avg: 12ms
    - P99: 45ms
  
  Complex analytical queries (50 total):
    - Avg: 2500ms
    - P99: 8000ms
  
  Overall throughput: 2500 queries/sec
  Under 50 concurrent users: 3200ms avg response time

POSTGRESQL TARGETS:
  ✓ Simple SELECTs: <50ms (Oracle 45ms + 10% tolerance)
  ✓ INSERT/UPDATE: <15ms (Oracle 12ms + 25% tolerance)
  ⚠️ Complex analytical: <3750ms (Oracle 2500ms + 50% tolerance)
  ✓ Overall throughput: >2000 queries/sec (10% degradation acceptable)
  ✓ Concurrent 50 users: <3500ms (Oracle 3200ms + 10% tolerance)
```

**ROI:**
- **Dev time:** 2 days
- **Cost:** $0
- **Impact:** Know exactly what "acceptable" means
- **Recommendation:** ✅ **Phase 3.1**

---

### **Stage 2: DURING MIGRATION PERFORMANCE COMPARISON**

#### **Problem 1: "PostgreSQL query is slow, but we don't know why"**

#### **AI Solution 1: Execution Plan Analyzer + Index Recommender** ⭐⭐⭐⭐⭐

```python
class ExecutionPlanAnalyzer:
    """Compare Oracle vs PostgreSQL plans, recommend indexes."""
    
    def compare_plans(oracle_query: str, pg_query: str) -> PlanAnalysis:
        """
        Get execution plans from both, analyze differences, recommend fixes.
        """
        
        oracle_plan = oracle_conn.explain(oracle_query)
        pg_plan = pg_conn.explain(pg_query)
        
        analysis = {
            "oracle": parse_plan(oracle_plan),
            "postgresql": parse_plan(pg_plan),
        }
        
        # Use Claude to diagnose differences
        diagnosis = claude.messages.create(
            model="claude-sonnet",
            messages=[{
                "role": "user",
                "content": f"""
                Compare these execution plans. Why is PostgreSQL slower?
                
                ORACLE PLAN:
                {oracle_plan}
                
                POSTGRESQL PLAN:
                {pg_plan}
                
                Specifically:
                1. What's the difference in join order/strategy?
                2. What indexes exist in Oracle but not PostgreSQL?
                3. Why is PostgreSQL doing full scans where Oracle uses index?
                4. What indexes should we create?
                5. Can we rewrite the query to be faster?
                
                Format:
                DIAGNOSIS: <root cause>
                MISSING_INDEXES: [list]
                SUGGESTED_REWRITES: [list]
                RISK: <low/medium/high>
                """
            }]
        )
        
        findings = parse_diagnosis(diagnosis.content[0].text)
        
        return PlanAnalysis(
            oracle_plan=analysis["oracle"],
            pg_plan=analysis["postgresql"],
            diagnosis=findings
        )
    
    def recommend_indexes(pg_plan: Plan, workload: Workload) -> IndexRecommendations:
        """
        Analyze slow queries → recommend indexes.
        """
        
        recommendations = []
        
        for slow_query in workload.slow_queries:
            plan = pg_conn.explain(slow_query)
            
            # Use Claude to analyze
            rec = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    Slow PostgreSQL query (slow: {slow_query.execution_time}ms):
                    
                    {slow_query.sql}
                    
                    Execution plan:
                    {plan}
                    
                    Recommend indexes to speed this up.
                    Format:
                    INDEX: <CREATE INDEX statement>
                    EXPECTED_SPEEDUP: <X ms to Y ms>
                    RISK: <low/medium/high>
                    """
                }]
            )
            
            recommendations.append(parse_recommendation(rec.content[0].text))
        
        # Prioritize by expected speedup
        recommendations.sort(key=lambda x: x.expected_speedup, reverse=True)
        
        return recommendations
```

**Example Output:**
```
EXECUTION PLAN ANALYSIS

Query: SELECT * FROM orders o
       JOIN order_items oi ON o.order_id = oi.order_id
       WHERE o.customer_id = 42
       AND SUBSTR(o.order_date, 1, 7) = '2024-01'

Oracle plan (45ms):
  Table access by index rowid: orders (customer_id index)
    Index range scan: idx_customer_id
  Table access by index rowid: order_items (order_id index)
    Index range scan: idx_order_id

PostgreSQL plan (2300ms):
  Hash join
    Seq scan: orders (full table scan!)
    Seq scan: order_items

DIAGNOSIS:
  PostgreSQL is doing full table scans instead of index lookups.
  This is because of the SUBSTR(order_date, 1, 7) = '2024-01' condition.
  Oracle has a function-based index on this SUBSTR.
  PostgreSQL has no index on the function.

MISSING INDEXES:
  1. CREATE INDEX idx_orders_customer_id ON orders(customer_id);
     Expected speedup: 2300ms → 60ms
     
  2. CREATE INDEX idx_order_date_month ON orders(SUBSTR(order_date, 1, 7));
     OR: ALTER TABLE orders ADD COLUMN order_month AS (SUBSTR(order_date, 1, 7));
         CREATE INDEX idx_order_month ON orders(order_month);
     Expected speedup: 2300ms → 45ms (match Oracle)

SUGGESTED REWRITES:
  1. Avoid SUBSTR in WHERE clause:
     BEFORE: WHERE SUBSTR(o.order_date, 1, 7) = '2024-01'
     AFTER:  WHERE o.order_date >= '2024-01-01' 
             AND o.order_date < '2024-02-01'
     Expected speedup: uses date range index

RISK: Low (indexes are safe)

Apply recommendations? [Y/n]
```

**ROI:**
- **Dev time:** 1 week
- **Cost:** $0
- **Impact:** Turn "PostgreSQL is slow" into actionable fixes
- **Recommendation:** ✅ **Phase 3.2** (high-value)

---

#### **Problem 2: "We created 50 indexes but performance didn't improve"**

#### **AI Solution 2: Index Impact Analyzer** ⭐⭐⭐⭐

```python
class IndexImpactAnalyzer:
    """Measure actual impact of each index."""
    
    def measure_index_impact(query: str, index: str) -> ImpactReport:
        """
        Run query WITH and WITHOUT index, measure difference.
        """
        
        # Get current query plan (with index)
        with_index_time = measure_query_time(query)
        with_index_plan = pg_conn.explain(query)
        
        # Disable index
        pg_conn.execute(f"ALTER INDEX {index} UNUSABLE")
        
        # Measure WITHOUT index
        without_index_time = measure_query_time(query)
        without_index_plan = pg_conn.explain(query)
        
        # Re-enable index
        pg_conn.execute(f"ALTER INDEX {index} REBUILD")
        
        improvement_ms = without_index_time - with_index_time
        improvement_pct = (improvement_ms / without_index_time) * 100
        
        report = ImpactReport(
            index=index,
            with_index_ms=with_index_time,
            without_index_ms=without_index_time,
            improvement_ms=improvement_ms,
            improvement_pct=improvement_pct,
            with_plan=with_index_plan,
            without_plan=without_index_plan,
            recommendation="KEEP" if improvement_pct > 10 else "REMOVE"
        )
        
        return report
    
    def analyze_all_indexes(slow_queries: List[Query]) -> IndexAnalysisReport:
        """
        Measure impact of every index on slow queries.
        """
        
        results = []
        unused_indexes = []
        
        for index in list_all_indexes():
            used = False
            total_impact = 0
            
            for query in slow_queries:
                impact = self.measure_index_impact(query, index)
                if impact.improvement_pct > 10:
                    used = True
                    total_impact += impact.improvement_ms
            
            results.append({
                "index": index,
                "total_impact_ms": total_impact,
                "used": used,
                "recommendation": "KEEP" if used else "REMOVE (bloat)"
            })
            
            if not used:
                unused_indexes.append(index)
        
        return IndexAnalysisReport(
            results=sorted(results, key=lambda x: x["total_impact_ms"], reverse=True),
            unused_indexes=unused_indexes,
            recommendation=f"Remove {len(unused_indexes)} unused indexes, "
                          f"saving {estimate_storage_saved(unused_indexes)} disk space"
        )
```

**Example Output:**
```
INDEX IMPACT ANALYSIS

Tested 50 indexes against 20 slow queries:

Top 10 Impactful Indexes:
  1. idx_customer_id: -1200ms impact (remove = 12x slower)
  2. idx_orders_date: -950ms impact
  3. idx_order_items_qty: -450ms impact
  ... (impactful indexes)

Unused Indexes (0ms impact, bloat):
  1. idx_customer_legacy (created 2023, never used)
  2. idx_orders_status_old (superseded by idx_status_date)
  3. idx_duplicate_address (8GB, only saves 2ms on 1 query)
  ... (30 unused indexes found)

RECOMMENDATION:
  ✓ KEEP: 15 indexes (provide >50ms improvement each)
  ⚠️ REVIEW: 5 indexes (marginal improvement, 100-500ms impact)
  ✗ DROP: 30 indexes (unused, 50GB total bloat)
  
Expected savings: 50GB disk space, faster writes (fewer indexes to maintain)
```

**ROI:**
- **Dev time:** 3 days
- **Cost:** $0
- **Impact:** Remove index bloat, keep only useful ones
- **Recommendation:** ⚠️ **Phase 3.2**

---

#### **Problem 3: "We tuned the database, but don't know if it helped"**

#### **AI Solution 3: Performance Regression Detector** ⭐⭐⭐⭐

```python
class RegressionDetector:
    """Detect performance regressions over time."""
    
    def compare_benchmarks(baseline: BenchmarkRun, current: BenchmarkRun) -> RegressionReport:
        """
        Compare two benchmark runs, flag regressions.
        """
        
        regressions = []
        improvements = []
        
        for query_id in baseline.queries.keys():
            baseline_time = baseline.queries[query_id]["execution_time_ms"]
            current_time = current.queries[query_id]["execution_time_ms"]
            
            delta_pct = ((current_time - baseline_time) / baseline_time) * 100
            
            if delta_pct > 10:  # >10% slower = regression
                regressions.append({
                    "query_id": query_id,
                    "baseline_ms": baseline_time,
                    "current_ms": current_time,
                    "delta_pct": delta_pct,
                    "severity": "CRITICAL" if delta_pct > 50 else "WARNING"
                })
            elif delta_pct < -10:  # >10% faster = improvement
                improvements.append({
                    "query_id": query_id,
                    "baseline_ms": baseline_time,
                    "current_ms": current_time,
                    "delta_pct": delta_pct,
                })
        
        report = RegressionReport(
            baseline_time=baseline.timestamp,
            current_time=current.timestamp,
            regressions=regressions,
            improvements=improvements,
            verdict="OK" if len(regressions) == 0 else "REGRESSION DETECTED"
        )
        
        if regressions:
            # Use Claude to diagnose what changed
            diagnosis = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    Performance regression detected:
                    
                    Baseline: {baseline.timestamp}
                    Current: {current.timestamp}
                    
                    Queries that got slower:
                    {format_regressions(regressions)}
                    
                    What might have changed?
                    - Indexes added/removed?
                    - Stats stale?
                    - Configuration changed?
                    - Schema changed?
                    - Concurrency increased?
                    
                    Diagnose what happened and suggest fixes.
                    """
                }]
            )
            
            report.diagnosis = parse_diagnosis(diagnosis.content[0].text)
        
        return report
```

**Example Output:**
```
PERFORMANCE REGRESSION REPORT

Baseline: 2024-01-15 (before index tuning)
Current: 2024-01-22 (after changes)

REGRESSIONS (got slower):
  ⚠️ Query emp_raise_salary: 150ms → 220ms (+47%)
  ⚠️ Query find_overdue_payments: 800ms → 1200ms (+50%)
  ⚠️ Query monthly_reconciliation: 5000ms → 8500ms (+70%)

IMPROVEMENTS (got faster):
  ✓ Query list_employees: 250ms → 180ms (-28%)
  ✓ Query fetch_salary_history: 400ms → 220ms (-45%)

DIAGNOSIS:
  What changed since baseline?
  
  Likely causes:
  1. Stats are stale (not re-analyzed after index creation)
     → ANALYZE; to recollect statistics
  
  2. Indexes on wrong columns
     → emp_raise_salary regression correlates with new index on department_id
     → But WHERE clause filters on salary, not department_id
     → Need index on salary instead
  
  3. Concurrent load increased
     → Check pg_stat_statements for lock contention
  
  RECOMMENDED FIXES:
  1. Run: ANALYZE; (5 minutes)
  2. Re-examine indexes on emp_raise_salary
  3. Check VACUUM settings (may be behind)
  4. Monitor lock waits: SELECT * FROM pg_locks WHERE granted = false;
  
Expected improvement: 220ms → 160ms after fixes
```

**ROI:**
- **Dev time:** 2 days
- **Cost:** $0
- **Impact:** Know if optimizations actually worked
- **Recommendation:** ⚠️ **Phase 3.2**

---

### **Stage 3: POST-MIGRATION CONTINUOUS PERFORMANCE MONITORING**

#### **Problem 1: "Everything was fast in testing, but slow in production"**

#### **AI Solution 1: Production Workload Analyzer** ⭐⭐⭐⭐⭐

```python
class ProductionWorkloadAnalyzer:
    """Real-time analysis of production queries."""
    
    def analyze_production_queries(pg_connection) -> WorkloadReport:
        """
        Use pg_stat_statements to analyze real production workload.
        """
        
        # Query PostgreSQL's built-in statistics
        stats = pg_connection.query("""
            SELECT
                query,
                calls,
                total_time,
                mean_time,
                max_time,
                rows
            FROM pg_stat_statements
            WHERE query NOT LIKE '%pg_stat%'
            ORDER BY total_time DESC
            LIMIT 100;
        """)
        
        analysis = {
            "slowest_queries": [],
            "most_called_queries": [],
            "queries_with_high_variance": [],  # Inconsistent performance
            "sequential_scans": []
        }
        
        for query in stats:
            # Slowest queries
            if query["mean_time"] > 1000:  # >1 second
                analysis["slowest_queries"].append({
                    "query": query["query"],
                    "mean_time_ms": query["mean_time"],
                    "max_time_ms": query["max_time"],
                    "calls": query["calls"]
                })
            
            # High variance (sometimes fast, sometimes slow)
            variance = query["max_time"] / query["mean_time"]
            if variance > 5:  # Max 5x slower than average
                analysis["queries_with_high_variance"].append({
                    "query": query["query"],
                    "mean_time_ms": query["mean_time"],
                    "max_time_ms": query["max_time"],
                    "variance_ratio": variance
                })
        
        # Use Claude to prioritize
        priorities = claude.messages.create(
            model="claude-sonnet",
            messages=[{
                "role": "user",
                "content": f"""
                Analyze this production workload. Prioritize optimization by impact.
                
                Slowest queries:
                {format_queries(analysis['slowest_queries'])}
                
                Queries with high variance (inconsistent):
                {format_queries(analysis['queries_with_high_variance'])}
                
                Production volume: {sum(q['calls'] for q in stats)}/day
                
                Which queries should we optimize first (biggest impact)?
                Format:
                PRIORITY: <1-10>
                QUERY: <query>
                IMPACT: <estimated time saved>
                ACTION: <suggested fix>
                """
            }]
        )
        
        analysis["priorities"] = parse_priorities(priorities.content[0].text)
        
        return analysis
```

**Example Output:**
```
PRODUCTION WORKLOAD ANALYSIS (1-week production)

Top 20 queries by total time consumed:

CRITICAL (>10 seconds total/day):
  1. emp_raise_salary
     - Called: 1000x/day
     - Mean: 150ms, Max: 2300ms
     - Total time: 150 seconds/day
     - Variance: 15x (sometimes fast, sometimes very slow)
     - ACTION: Investigate lock contention, add index on salary

  2. find_overdue_accounts
     - Called: 5000x/day
     - Mean: 80ms, Max: 4000ms
     - Total time: 400 seconds/day
     - Variance: 50x (huge variance = lock waits)
     - ACTION: Check for table locks, add concurrency control

MEDIUM (1-10 seconds/day):
  3. monthly_reconciliation
  4. fetch_salary_history
  5. ... (rest of top 20)

HIGHEST IMPACT OPTIMIZATIONS:
  1. Fix emp_raise_salary (150s/day saved)
     - Add index on salary column
     - Check for blocking locks (SELECT * FROM pg_locks WHERE NOT granted)
     - Expected: 150ms → 50ms

  2. Fix find_overdue_accounts (400s/day saved)
     - Partition accounts table (too many lock waits)
     - Or use advisory locks instead of table locks
     - Expected: 80ms → 20ms

Expected total improvement: 550 seconds/day (9 minutes)
```

**ROI:**
- **Dev time:** 2 days
- **Cost:** $0 (PostgreSQL built-in pg_stat_statements)
- **Impact:** Focus optimization on highest-impact queries
- **Recommendation:** ✅ **Phase 3.3** (production safety)

---

#### **Problem 2: "Performance degrades over time (no code changes)"**

#### **AI Solution 2: Anomaly Detection + Auto-Tuning** ⭐⭐⭐⭐

```python
class PerformanceAnomalyDetector:
    """Detect performance degradation, suggest automatic fixes."""
    
    def detect_anomalies(baseline_metrics: Metrics, current_metrics: Metrics) -> AnomalyReport:
        """
        Compare metrics to baseline, flag degradation.
        """
        
        anomalies = []
        
        # Check response time
        if current_metrics.avg_response_time > baseline_metrics.avg_response_time * 1.2:
            anomalies.append({
                "type": "RESPONSE_TIME_DEGRADATION",
                "baseline_ms": baseline_metrics.avg_response_time,
                "current_ms": current_metrics.avg_response_time,
                "delta_pct": ((current_metrics.avg_response_time - baseline_metrics.avg_response_time) 
                             / baseline_metrics.avg_response_time) * 100,
                "severity": "CRITICAL" if delta_pct > 50 else "WARNING"
            })
        
        # Check disk usage (may indicate bloat)
        if current_metrics.disk_usage > baseline_metrics.disk_usage * 1.5:
            anomalies.append({
                "type": "DISK_BLOAT",
                "baseline_gb": baseline_metrics.disk_usage / (1024**3),
                "current_gb": current_metrics.disk_usage / (1024**3),
                "suggestion": "Run VACUUM FULL; and REINDEX; to reclaim space"
            })
        
        # Check table/index bloat
        bloat_stats = analyze_bloat()
        if bloat_stats.wasted_space > baseline_metrics.wasted_space * 2:
            anomalies.append({
                "type": "TABLE_BLOAT",
                "wasted_space_mb": bloat_stats.wasted_space / (1024**2),
                "suggestion": f"Run VACUUM ANALYZE; on {len(bloat_stats.bloated_tables)} tables"
            })
        
        # Check stats freshness
        stats_age = get_stats_age()
        if stats_age > 24 * 60:  # >24 hours old
            anomalies.append({
                "type": "STALE_STATISTICS",
                "stats_age_hours": stats_age / 60,
                "suggestion": "Run ANALYZE; to recollect statistics"
            })
        
        # If anomalies detected, use Claude to diagnose
        if anomalies:
            diagnosis = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    PostgreSQL performance degradation detected:
                    
                    Baseline metrics (1 week ago):
                    {format_metrics(baseline_metrics)}
                    
                    Current metrics:
                    {format_metrics(current_metrics)}
                    
                    Anomalies detected:
                    {format_anomalies(anomalies)}
                    
                    Diagnose what happened and suggest fixes.
                    Prioritize by impact (biggest speedup first).
                    """
                }]
            )
            
            diagnosis = parse_diagnosis(diagnosis.content[0].text)
            
            return AnomalyReport(
                anomalies=anomalies,
                diagnosis=diagnosis,
                auto_fixes=suggest_auto_fixes(diagnosis)
            )
```

**Example Output:**
```
PERFORMANCE DEGRADATION DETECTED

Baseline (1 week ago):
  Avg response time: 150ms
  Throughput: 2500 queries/sec
  Disk usage: 500GB
  Stats age: 1 day

Current:
  Avg response time: 350ms (+133%)
  Throughput: 1800 queries/sec (-28%)
  Disk usage: 750GB (+50%)
  Stats age: 15 days

ANOMALIES:
  ⚠️ Response time increased 133%
  ⚠️ Throughput decreased 28%
  ⚠️ Disk bloat +250GB
  ⚠️ Statistics are 15 days old (should be <1 day)

ROOT CAUSES (by Claude diagnosis):
  1. Statistics are stale (15 days old)
     → Planner is making bad decisions based on old data
     → VACUUM ANALYZE not running regularly
     → Impact: -200ms (biggest factor)
  
  2. Table bloat from dead tuples (not vacuumed)
     → 250GB wasted space
     → Seq scans slower (reading more dead tuples)
     → Impact: -100ms
  
  3. Missing statistics on new columns
     → Two new columns added, no index or stats
     → Queries using new columns doing full scans
     → Impact: -50ms

RECOMMENDED FIXES (in priority order):
  1. IMMEDIATE: Run ANALYZE;
     - Recollect statistics (15 min)
     - Expected improvement: -200ms
     - Risk: LOW (read-only operation)
  
  2. IMMEDIATE: Configure autovacuum
     - Edit postgresql.conf:
       autovacuum = on
       autovacuum_naptime = 10s  (increased from 60s)
       autovacuum_vacuum_scale_factor = 0.05
     - Expected improvement: prevents future bloat
  
  3. URGENT: VACUUM FULL; (30 min, downtime)
     - Reclaim 250GB of wasted space
     - Expected improvement: -100ms
     - Risk: Locks all tables (plan downtime window)
     - Can be deferred to maintenance window
  
  4. Create index on new columns
     - CREATE INDEX idx_new_col1 ON employees(new_col1);
     - ANALYZE;
     - Expected improvement: -50ms

AUTO-FIX SCRIPT (can run safely now):
  ANALYZE;
  REINDEX INDEX idx_customer_id;
  REINDEX INDEX idx_employee_salary;
  -- Then VACUUM FULL; during maintenance window

Expected result after fixes: 350ms → 120ms
```

**ROI:**
- **Dev time:** 2 days
- **Cost:** $0
- **Impact:** Auto-detect and fix degradation
- **Recommendation:** ✅ **Phase 3.3** (production health)

---

#### **Problem 3: "We need proof performance meets SLA before cutover"**

#### **AI Solution 3: SLA Validation Engine** ⭐⭐⭐⭐⭐

```python
class SLAValidator:
    """Validate that PostgreSQL meets production SLA targets."""
    
    def validate_sla(pg_workload: BenchmarkRun, sla_targets: SLATargets) -> SLAReport:
        """
        Compare benchmark results to SLA targets.
        """
        
        results = {
            "passed": [],
            "failed": [],
            "warnings": []
        }
        
        # Response time SLA
        if pg_workload.avg_response_time <= sla_targets.max_avg_response_time:
            results["passed"].append({
                "metric": "Average Response Time",
                "target": f"<{sla_targets.max_avg_response_time}ms",
                "actual": f"{pg_workload.avg_response_time}ms",
                "status": "PASS"
            })
        else:
            results["failed"].append({
                "metric": "Average Response Time",
                "target": f"<{sla_targets.max_avg_response_time}ms",
                "actual": f"{pg_workload.avg_response_time}ms",
                "gap": pg_workload.avg_response_time - sla_targets.max_avg_response_time,
                "status": "FAIL"
            })
        
        # P99 response time SLA
        p99_time = percentile(pg_workload.query_times, 0.99)
        if p99_time <= sla_targets.max_p99_response_time:
            results["passed"].append({
                "metric": "P99 Response Time",
                "target": f"<{sla_targets.max_p99_response_time}ms",
                "actual": f"{p99_time}ms",
                "status": "PASS"
            })
        else:
            results["failed"].append({
                "metric": "P99 Response Time",
                "target": f"<{sla_targets.max_p99_response_time}ms",
                "actual": f"{p99_time}ms",
                "gap": p99_time - sla_targets.max_p99_response_time,
                "status": "FAIL"
            })
        
        # Throughput SLA
        if pg_workload.throughput >= sla_targets.min_throughput:
            results["passed"].append({
                "metric": "Throughput",
                "target": f">{sla_targets.min_throughput} queries/sec",
                "actual": f"{pg_workload.throughput} queries/sec",
                "status": "PASS"
            })
        else:
            results["failed"].append({
                "metric": "Throughput",
                "target": f">{sla_targets.min_throughput} queries/sec",
                "actual": f"{pg_workload.throughput} queries/sec",
                "shortfall": sla_targets.min_throughput - pg_workload.throughput,
                "status": "FAIL"
            })
        
        # If any failures, use Claude to suggest remediation
        if results["failed"]:
            remediation = claude.messages.create(
                model="claude-sonnet",
                messages=[{
                    "role": "user",
                    "content": f"""
                    PostgreSQL performance testing failed SLA targets:
                    
                    Failed metrics:
                    {format_failures(results['failed'])}
                    
                    Suggest remediation:
                    1. Quick fixes (can apply before cutover)
                    2. Medium-term fixes (within 2 weeks post-cutover)
                    3. Long-term fixes (infrastructure changes)
                    
                    What's the minimum work to pass SLA?
                    """
                }]
            )
            
            results["remediation"] = parse_remediation(remediation.content[0].text)
        
        return SLAReport(
            passed=results["passed"],
            failed=results["failed"],
            remediation=results.get("remediation"),
            verdict="PASS" if len(results["failed"]) == 0 else "FAIL"
        )
```

**Example Output:**
```
SLA VALIDATION REPORT

Production SLA Targets:
  ✓ Average response time: <200ms
  ✓ P99 response time: <1000ms
  ✓ Throughput: >2000 queries/sec
  ✓ Error rate: <0.1%
  ✓ Data accuracy: 100%

PostgreSQL Benchmark Results:
  ✗ Average response time: 280ms (80ms OVER target)
  ✗ P99 response time: 1200ms (200ms OVER target)
  ✓ Throughput: 2100 queries/sec
  ✓ Error rate: 0.05%
  ✓ Data accuracy: 100%

VERDICT: FAIL - Do not cutover until response times improve

Failed metrics:
  1. Avg response time: 280ms vs 200ms target (-80ms gap)
  2. P99 response time: 1200ms vs 1000ms target (-200ms gap)

ROOT CAUSE ANALYSIS (by Claude):
  The response time overages are caused by:
  1. Missing indexes on 5 frequently-used queries
  2. VACUUM maintenance not keeping up with load
  3. 12 expensive queries doing full table scans

REMEDIATION (in priority order):

QUICK FIXES (can apply immediately, 2 hours):
  1. Create missing indexes (5 indexes):
     - idx_orders_date_range
     - idx_customer_active
     - idx_salary_bucket
     - idx_status_type
     - idx_reconciled_flag
     Expected improvement: -60ms
  
  2. Run ANALYZE; to recollect statistics
     Expected improvement: -20ms

MEDIUM-TERM (within 1 week post-cutover):
  1. Query rewrites on 3 expensive queries
     Expected improvement: -30ms
  
  2. Partition orders table (50M rows)
     Expected improvement: -20ms

FINAL RESULT:
  Before: 280ms avg, 1200ms P99
  After quick fixes: 220ms avg, 1160ms P99 (not quite passing)
  After medium-term: 180ms avg, 950ms P99 (PASS)

RECOMMENDATION:
  ⚠️ Apply quick fixes now (should be close to SLA)
  ✓ Then cutover to PostgreSQL
  ✓ Apply medium-term fixes during week 1 (fully pass SLA)
  ✓ Use this time to validate data/functionality
```

**ROI:**
- **Dev time:** 1 week
- **Cost:** $0
- **Impact:** Confidence in meeting production SLA
- **Recommendation:** ✅ **Phase 3.3** (critical)

---

## 📊 Complete Performance Benchmarking Stack

| Stage | Tool | Dev Time | Cost | Impact |
|-------|------|----------|------|--------|
| **Pre** | Workload Generator | 2 days | $0 | Realistic test scenarios |
| **Pre** | Baseline Profiler | 2 days | $0 | Know acceptable performance |
| **During** | Execution Plan Analyzer | 1 week | $0 | Turn slow queries into fixes |
| **During** | Index Impact Analyzer | 3 days | $0 | Keep good indexes, drop bloat |
| **During** | Regression Detector | 2 days | $0 | Know if optimizations worked |
| **Post** | Production Workload Analyzer | 2 days | $0 | Real-time query optimization |
| **Post** | Anomaly Detector + Auto-Tuning | 2 days | $0 | Detect degradation, auto-fix |
| **Post** | SLA Validator | 1 week | $0 | Prove you meet production targets |

---

## 🚀 Competitive Advantage

**Traditional Migration:**
```
Test shows 150ms response time
Production is 1200ms (8x slower!)
"PostgreSQL is slow" → Rollback

Why? Missing indexes, stale stats, 
unexpected query patterns in real workload
```

**Depart Performance Benchmarking:**
```
Stage 1: Generate realistic workload from procedures
Stage 2: Establish Oracle baseline (150ms avg)
Stage 3: Compare PostgreSQL → identify missing indexes
Stage 4: Add indexes → validate meets SLA
Stage 5: Deploy with confidence + monitor continuously
Stage 6: Auto-detect degradation, auto-fix

Production: 180ms avg (matches Oracle)
Status: PASS
```

---

## 📋 Implementation Roadmap

| Phase | Tools | Timeline | Impact |
|-------|-------|----------|--------|
| **3.1** | Workload generator, baseline profiler | 1 week | Know expected performance |
| **3.2** | Plan analyzer, index recommender, regression detector | 2 weeks | Identify + fix slow queries |
| **3.3** | Production analyzer, anomaly detector, SLA validator | 2 weeks | Continuous safety + confidence |

---

## 💡 Key Insight

**Performance issues are the #1 reason migrations fail.**

By building AI-powered performance benchmarking + monitoring from day 1, you:
- Establish expected baseline BEFORE conversion
- Identify + fix performance gaps BEFORE production
- Continuously monitor + auto-tune in production
- Provide data-driven go/no-go decisions
- Prevent rollbacks from "PostgreSQL is slow"

**This is your biggest differentiator.**
