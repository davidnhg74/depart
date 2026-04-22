# Cost Savings Calculator — Integration Guide

## Overview

The Cost Savings Calculator is a complete feature for ROI justification. Users can input their database configuration and receive detailed cost breakdowns, ROI analysis, and payback period calculations.

## Architecture

### Backend
- **File**: `apps/api/src/cost_calculator/calculator.py`
- **API Endpoint**: `POST /api/v3/cost-analysis`
- **Calculations**: Oracle vs PostgreSQL costs, migration expenses, 5-year ROI

### Frontend
- **Page**: `apps/web/app/pricing/page.tsx` (URL: `/pricing`)
- **Component**: `apps/web/app/components/CostCalculator.tsx`
- **Navigation**: Global navigation bar integrated into all pages

## Database Costs Model

### Oracle Annual Costs
| Component | Formula | Default |
|-----------|---------|---------|
| License | $47,500 × (cores / 2) | $47,500/2-core pack |
| Support | 22% of license | $10,450/year |
| Infrastructure | Cloud: $5K–$50K/month; On-prem: $5K–$150K/year | Varies by size |
| Storage | $2K–$50K/year | Size-dependent |
| DBA Salary | $120,000 × FTE | $120,000/FTE |

### PostgreSQL Annual Costs
| Component | Formula | Default |
|-----------|---------|---------|
| License | FREE | $0 |
| Support | $10K × num_databases | $10,000/year |
| Infrastructure | 40% of Oracle cloud cost | ~$2K–$20K/month |
| Storage | 30% savings vs Oracle | $1.5K–$35K/year |
| DBA Salary | $100K × FTE × 0.85 | $85,000/FTE |

### Migration Costs (One-time)
| Item | Formula |
|------|---------|
| Depart License | $25K + ($8K × additional DBs) |
| DBA Consulting | 40–150 hours × $250/hr |
| Testing & Validation | $5K–$50K (size-dependent) |
| Data Migration Service | $2K–$30K |
| Cutover Support | $2K–$20K |
| Training | $5K |

## ROI Metrics

The calculator returns:
- **Annual Savings (Year 1)**: Savings minus migration investment
- **Annual Savings (Year 2+)**: Pure recurring savings
- **Payback Period**: Months to recover migration costs
- **5-Year ROI**: Percentage return over 5 years
- **5-Year Savings**: Total cumulative savings

## API Request/Response

### Request
```json
POST /api/v3/cost-analysis
{
  "database_size": "medium",        // "small" | "medium" | "large" | "enterprise"
  "deployment_type": "cloud_aws",   // "onprem" | "cloud_aws" | "cloud_azure" | "cloud_gcp"
  "num_databases": 1,               // Integer 1-10
  "num_oracle_cores": 4,            // Integer 2+, even
  "num_dba_fte": 1.0                // Float 0.5+
}
```

### Response
```json
{
  "status": "success",
  "analysis": {
    "oracle_breakdown": {
      "license_cost_per_year": 47500,
      "support_cost_per_year": 10450,
      "infrastructure_cost_per_year": 60000,
      "storage_cost_per_year": 5000,
      "dba_salary_cost_per_year": 120000,
      "total_annual_cost": 242950
    },
    "postgres_breakdown": {
      "license_cost_per_year": 0,
      "support_cost_per_year": 10000,
      "infrastructure_cost_per_year": 28800,
      "storage_cost_per_year": 3500,
      "dba_salary_cost_per_year": 85000,
      "total_annual_cost": 127300
    },
    "annual_savings_year1": 90650,    // After migration costs
    "annual_savings_year2_plus": 115650,
    "payback_months": 2.6,
    "roi_percent": 239.3,
    "five_year_savings": 451250
  },
  "summary": {
    "annual_savings_year1": "$90,650",
    "annual_savings_year2_plus": "$115,650",
    "payback_months": "2.6",
    "roi_percent": "239%",
    "five_year_savings": "$451,250"
  }
}
```

## UI Components

### Pricing Page (`/pricing`)
Landing page with:
- Hero section explaining ROI calculator
- Interactive CostCalculator component

### CostCalculator Component
**Input Form:**
- Database Size dropdown (Small, Medium, Large, Enterprise)
- Deployment Type dropdown (On-Prem, AWS, Azure, GCP)
- Number of Databases (1-10)
- Oracle CPU Cores (2+, step 2)
- DBA FTEs (0.5+, step 0.5)

**Results Display:**
- 4 summary cards: Annual Savings, Payback Period, 5-Year ROI, 5-Year Savings
- Cost breakdown tables: Oracle vs PostgreSQL side-by-side
- Year 1 projection with migration investment impact
- CTA button: "Start Migration"

### Navigation
Global navigation bar on all pages:
- Analyzer (home page)
- Converter (conversion UI)
- ROI Calculator (pricing page)

## Testing Instructions

### 1. Manual Testing (Browser)
```bash
# Start Docker Compose (API + Frontend)
docker compose up

# Navigate to http://localhost:3000/pricing
# Enter test values and click "Calculate Savings"
# Verify results display correctly
```

### 2. API Testing (curl)
```bash
curl -X POST http://localhost:8000/api/v3/cost-analysis \
  -H "Content-Type: application/json" \
  -d '{
    "database_size": "medium",
    "deployment_type": "cloud_aws",
    "num_databases": 1,
    "num_oracle_cores": 4,
    "num_dba_fte": 1.0
  }'
```

### 3. Test Cases

#### Case 1: Small Cloud Database
- Database Size: Small
- Deployment: AWS
- Expected: ~$30K annual savings, 3-4 month payback

#### Case 2: Large On-Prem Enterprise
- Database Size: Enterprise
- Deployment: On-Prem
- Cores: 32
- Expected: ~$500K+ annual savings, <1 month payback

#### Case 3: Multiple Databases
- Number of Databases: 5
- Cores: 8 per database
- Expected: Additional complexity, higher consulting costs

## Integration Checklist

- [x] Backend calculator logic (`calculator.py`)
- [x] API endpoint wired in `main.py`
- [x] Frontend component created
- [x] Pricing page created
- [x] Navigation integrated
- [x] API URL configured with environment variable
- [x] Committed to git

## Next Steps

1. **Testing**: Deploy to staging and test with real data
2. **Analytics**: Track calculator usage, most requested scenarios
3. **Refinement**: Adjust pricing assumptions based on customer feedback
4. **Integration**: Link calculator results → conversion workflow
5. **PDF Export**: Add ability to download ROI report as PDF

## File Structure
```
depart/
├── apps/
│   ├── api/src/
│   │   └── cost_calculator/
│   │       ├── __init__.py
│   │       └── calculator.py (400 lines)
│   └── web/app/
│       ├── layout.tsx (updated with Navigation)
│       ├── page.tsx (updated with ROI CTA)
│       ├── pricing/
│       │   └── page.tsx (new)
│       └── components/
│           ├── Navigation.tsx (new)
│           └── CostCalculator.tsx (refactored for env var)
└── COST_CALCULATOR_INTEGRATION.md (this file)
```

## Key Assumptions

1. **Oracle Licensing**: Per-core 2-core pack model at $47,500/year
2. **Support Rates**: Oracle 22%, PostgreSQL 5% of infrastructure
3. **DBA Salary Reduction**: PostgreSQL DBAs 15% cheaper due to lower complexity
4. **Infrastructure Costs**: PostgreSQL 35-40% of Oracle, storage 30% more efficient
5. **Migration Investment**: $25K base + $8K per additional database
6. **Payback Calculation**: Based on monthly savings, excluding Year 1 investment impact

## Customization

To adjust pricing assumptions, edit `apps/api/src/cost_calculator/calculator.py`:
- `ORACLE_LICENSE_PER_CORE`: License cost per 2-core pack
- `ORACLE_SUPPORT_PERCENT`: Support as percentage of license
- `ORACLE_DBA_SALARY`: DBA salary benchmark
- `AWS_DATABASE_MONTHLY`: Cloud infrastructure costs
- `DEPART_MIGRATION_FEE`: Base migration cost
- etc.

All values are tunable constants at the top of the `CostCalculator` class.
