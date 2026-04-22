"""
Cost savings calculator for Oracle → PostgreSQL migrations.
Shows customers exact ROI and justifies project budget.
"""

from pydantic import BaseModel
from typing import Optional, Dict, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DatabaseSize(str, Enum):
    """Database size categories."""
    SMALL = "small"          # < 100 GB
    MEDIUM = "medium"        # 100 GB - 1 TB
    LARGE = "large"          # 1 TB - 10 TB
    ENTERPRISE = "enterprise"  # > 10 TB


class DeploymentType(str, Enum):
    """Deployment type."""
    ONPREM = "onprem"
    CLOUD_AWS = "cloud_aws"
    CLOUD_AZURE = "cloud_azure"
    CLOUD_GCP = "cloud_gcp"


class OracleCostBreakdown(BaseModel):
    """Breakdown of Oracle costs."""
    license_cost_per_year: float
    support_cost_per_year: float
    infrastructure_cost_per_year: float
    storage_cost_per_year: float
    dba_salary_cost_per_year: float
    total_annual_cost: float


class PostgresCostBreakdown(BaseModel):
    """Breakdown of PostgreSQL costs."""
    license_cost_per_year: float
    support_cost_per_year: float
    infrastructure_cost_per_year: float
    storage_cost_per_year: float
    dba_salary_cost_per_year: float
    total_annual_cost: float


class MigrationCosts(BaseModel):
    """Migration-related costs (one-time)."""
    depart_license_fee: float  # Depart migration service
    dba_consulting_hours: int
    dba_consulting_cost: float
    testing_and_validation: float
    data_migration_service: float
    cutover_support: float
    training: float
    total_migration_cost: float


class CostAnalysis(BaseModel):
    """Complete cost analysis and ROI."""
    oracle_breakdown: OracleCostBreakdown
    postgres_breakdown: PostgresCostBreakdown
    migration_costs: MigrationCosts

    # Savings
    annual_savings_year1: float
    annual_savings_year2_plus: float
    payback_months: float
    roi_percent: float
    five_year_savings: float

    # Details
    database_size: DatabaseSize
    deployment_type: DeploymentType
    num_databases: int
    assumptions: Dict[str, str]


class CostCalculator:
    """Calculate cost savings for Oracle → PostgreSQL migration."""

    # Oracle licensing prices (2026 estimates)
    ORACLE_LICENSE_PER_CORE = 47_500  # per 2-core pack, per year
    ORACLE_SUPPORT_PERCENT = 0.22  # 22% of license cost
    ORACLE_DBA_SALARY = 120_000  # per FTE per year

    # PostgreSQL costs
    POSTGRES_SUPPORT_PERCENT = 0.05  # 5% of infrastructure (very cheap)
    POSTGRES_DBA_SALARY = 100_000  # per FTE per year (slightly less specialized)

    # Depart pricing
    DEPART_MIGRATION_FEE = 25_000  # one-time for first migration
    DEPART_ADDITIONAL_DB_FEE = 8_000  # per additional database

    # Consulting
    DBA_CONSULTING_RATE = 250  # per hour

    # Cloud infrastructure (AWS pricing, 2026)
    AWS_DATABASE_MONTHLY = {
        DatabaseSize.SMALL: 1_000,      # < 100 GB
        DatabaseSize.MEDIUM: 5_000,     # 100 GB - 1 TB
        DatabaseSize.LARGE: 20_000,     # 1 TB - 10 TB
        DatabaseSize.ENTERPRISE: 50_000,  # > 10 TB
    }

    # On-prem infrastructure (estimated annual)
    ONPREM_INFRASTRUCTURE_ANNUAL = {
        DatabaseSize.SMALL: 5_000,
        DatabaseSize.MEDIUM: 15_000,
        DatabaseSize.LARGE: 50_000,
        DatabaseSize.ENTERPRISE: 150_000,
    }

    def __init__(
        self,
        database_size: DatabaseSize = DatabaseSize.MEDIUM,
        deployment_type: DeploymentType = DeploymentType.CLOUD_AWS,
        num_databases: int = 1,
        num_oracle_cores: int = 4,
        num_dba_fte: float = 1.0,
    ):
        """
        Initialize calculator.

        Args:
            database_size: Size of database (SMALL, MEDIUM, LARGE, ENTERPRISE)
            deployment_type: Where database runs (on-prem, AWS, Azure, GCP)
            num_databases: Number of databases to migrate
            num_oracle_cores: Number of Oracle CPU cores licensed
            num_dba_fte: Number of DBA FTEs maintaining database
        """
        self.database_size = database_size
        self.deployment_type = deployment_type
        self.num_databases = num_databases
        self.num_oracle_cores = num_oracle_cores
        self.num_dba_fte = num_dba_fte

    def calculate_oracle_costs(self) -> OracleCostBreakdown:
        """Calculate annual Oracle costs."""
        # License: per 2-core pack
        num_2core_packs = max(1, self.num_oracle_cores // 2)
        license_cost = num_2core_packs * self.ORACLE_LICENSE_PER_CORE

        # Support: 22% of license
        support_cost = license_cost * self.ORACLE_SUPPORT_PERCENT

        # DBA salary
        dba_cost = self.ORACLE_DBA_SALARY * self.num_dba_fte

        # Infrastructure (cloud or on-prem)
        if self.deployment_type == DeploymentType.ONPREM:
            infra_cost = self.ONPREM_INFRASTRUCTURE_ANNUAL.get(
                self.database_size, 50_000
            )
        else:
            # Cloud: use AWS as baseline
            monthly_cost = self.AWS_DATABASE_MONTHLY.get(
                self.database_size, 20_000
            )
            infra_cost = monthly_cost * 12

        # Storage (often separate line item)
        storage_cost = {
            DatabaseSize.SMALL: 2_000,
            DatabaseSize.MEDIUM: 5_000,
            DatabaseSize.LARGE: 20_000,
            DatabaseSize.ENTERPRISE: 50_000,
        }.get(self.database_size, 5_000)

        total = license_cost + support_cost + infra_cost + storage_cost + dba_cost

        return OracleCostBreakdown(
            license_cost_per_year=license_cost,
            support_cost_per_year=support_cost,
            infrastructure_cost_per_year=infra_cost,
            storage_cost_per_year=storage_cost,
            dba_salary_cost_per_year=dba_cost,
            total_annual_cost=total,
        )

    def calculate_postgres_costs(self) -> PostgresCostBreakdown:
        """Calculate annual PostgreSQL costs."""
        # PostgreSQL license: FREE
        license_cost = 0

        # Support: Depart + community (much cheaper)
        # For simplicity: flat $10K/year per database
        support_cost = 10_000 * self.num_databases

        # Infrastructure: 30-40% cheaper than Oracle
        if self.deployment_type == DeploymentType.ONPREM:
            oracle_infra = self.ONPREM_INFRASTRUCTURE_ANNUAL.get(
                self.database_size, 50_000
            )
            infra_cost = oracle_infra * 0.35  # 35% of Oracle cost
        else:
            # Cloud: PostgreSQL RDS is cheaper
            monthly_base = self.AWS_DATABASE_MONTHLY.get(
                self.database_size, 20_000
            )
            infra_cost = monthly_base * 12 * 0.40  # 40% of Oracle cloud cost

        # Storage: PostgreSQL more efficient (30% less)
        storage_cost = {
            DatabaseSize.SMALL: 1_500,
            DatabaseSize.MEDIUM: 3_500,
            DatabaseSize.LARGE: 14_000,
            DatabaseSize.ENTERPRISE: 35_000,
        }.get(self.database_size, 3_500)

        # DBA salary: slightly lower (PostgreSQL less complex)
        dba_cost = (self.POSTGRES_DBA_SALARY * self.num_dba_fte) * 0.85

        total = license_cost + support_cost + infra_cost + storage_cost + dba_cost

        return PostgresCostBreakdown(
            license_cost_per_year=license_cost,
            support_cost_per_year=support_cost,
            infrastructure_cost_per_year=infra_cost,
            storage_cost_per_year=storage_cost,
            dba_salary_cost_per_year=dba_cost,
            total_annual_cost=total,
        )

    def calculate_migration_costs(self) -> MigrationCosts:
        """Calculate one-time migration costs."""
        # Depart license fee
        depart_fee = self.DEPART_MIGRATION_FEE
        if self.num_databases > 1:
            depart_fee += (self.num_databases - 1) * self.DEPART_ADDITIONAL_DB_FEE

        # DBA consulting (varies by complexity)
        # Estimate: 40-100 hours per database
        consulting_hours = {
            DatabaseSize.SMALL: 40 * self.num_databases,
            DatabaseSize.MEDIUM: 60 * self.num_databases,
            DatabaseSize.LARGE: 100 * self.num_databases,
            DatabaseSize.ENTERPRISE: 150 * self.num_databases,
        }.get(self.database_size, 60 * self.num_databases)

        consulting_cost = consulting_hours * self.DBA_CONSULTING_RATE

        # Testing and validation
        testing_cost = {
            DatabaseSize.SMALL: 5_000,
            DatabaseSize.MEDIUM: 10_000,
            DatabaseSize.LARGE: 25_000,
            DatabaseSize.ENTERPRISE: 50_000,
        }.get(self.database_size, 10_000)

        # Data migration service (if not DIY)
        migration_service = {
            DatabaseSize.SMALL: 2_000,
            DatabaseSize.MEDIUM: 5_000,
            DatabaseSize.LARGE: 15_000,
            DatabaseSize.ENTERPRISE: 30_000,
        }.get(self.database_size, 5_000)

        # Cutover support (24-hour coverage)
        cutover_support = {
            DatabaseSize.SMALL: 2_000,
            DatabaseSize.MEDIUM: 5_000,
            DatabaseSize.LARGE: 10_000,
            DatabaseSize.ENTERPRISE: 20_000,
        }.get(self.database_size, 5_000)

        # Training
        training_cost = 5_000

        total = (
            depart_fee
            + consulting_cost
            + testing_cost
            + migration_service
            + cutover_support
            + training_cost
        )

        return MigrationCosts(
            depart_license_fee=depart_fee,
            dba_consulting_hours=int(consulting_hours),
            dba_consulting_cost=consulting_cost,
            testing_and_validation=testing_cost,
            data_migration_service=migration_service,
            cutover_support=cutover_support,
            training=training_cost,
            total_migration_cost=total,
        )

    def analyze(self) -> CostAnalysis:
        """Run complete cost analysis."""
        oracle = self.calculate_oracle_costs()
        postgres = self.calculate_postgres_costs()
        migration = self.calculate_migration_costs()

        # Year 1 savings: reduce by migration costs
        annual_savings_year1 = (
            oracle.total_annual_cost - postgres.total_annual_cost - migration.total_migration_cost
        )

        # Year 2+ savings: full annual savings
        annual_savings_year2_plus = oracle.total_annual_cost - postgres.total_annual_cost

        # Payback period (months until migration cost is recovered)
        monthly_savings = annual_savings_year2_plus / 12
        if monthly_savings > 0:
            payback_months = migration.total_migration_cost / monthly_savings
        else:
            payback_months = 0

        # ROI (5 year perspective)
        five_year_postgres_cost = (
            postgres.total_annual_cost * 5 + migration.total_migration_cost
        )
        five_year_oracle_cost = oracle.total_annual_cost * 5
        five_year_savings = five_year_oracle_cost - five_year_postgres_cost

        roi_percent = (five_year_savings / five_year_oracle_cost) * 100 if five_year_oracle_cost > 0 else 0

        return CostAnalysis(
            oracle_breakdown=oracle,
            postgres_breakdown=postgres,
            migration_costs=migration,
            annual_savings_year1=annual_savings_year1,
            annual_savings_year2_plus=annual_savings_year2_plus,
            payback_months=payback_months,
            roi_percent=roi_percent,
            five_year_savings=five_year_savings,
            database_size=self.database_size,
            deployment_type=self.deployment_type,
            num_databases=self.num_databases,
            assumptions={
                "oracle_cores": str(self.num_oracle_cores),
                "dba_fte": str(self.num_dba_fte),
                "deployment": self.deployment_type.value,
                "database_count": str(self.num_databases),
            },
        )
