# Data Model Documentation

## Entity Relationship Diagram

```
┌─────────────────────────────────────┐
│          CUSTOMERS                  │
├─────────────────────────────────────┤
│ PK: customer_id (VARCHAR)           │
│     name (VARCHAR)                  │
│     signup_date (DATE)              │
│     tier (VARCHAR)                  │
│     status (VARCHAR)                │
│     industry (VARCHAR)              │
│     region (VARCHAR)                │
└─────────────┬───────────────────────┘
              │ 1:N
              ├─────────────────────────┐
              │                         │
              │ 1:N                     │ 1:N
              ▼                         ▼
┌─────────────────────────────────┐  ┌─────────────────────────────────┐
│       CREDIT_USAGE              │  │         BILLING                 │
├─────────────────────────────────┤  ├─────────────────────────────────┤
│ PK: usage_id (VARCHAR)          │  │ PK: billing_id (VARCHAR)        │
│ FK: customer_id (VARCHAR)       │  │ FK: customer_id (VARCHAR)       │
│     date (DATE)                 │  │     billing_date (DATE)         │
│     credits_used (DECIMAL)      │  │     amount (DECIMAL)            │
│     feature (VARCHAR)           │  │     period (VARCHAR)            │
│     execution_time_ms (INTEGER) │  │     status (VARCHAR)            │
└─────────────────────────────────┘  │     currency (VARCHAR)          │
                                     └─────────────────────────────────┘
```

## Table Descriptions

### CUSTOMERS
**Purpose**: Store customer master data

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| customer_id | VARCHAR(50) | Unique customer identifier (PK) | CUST_0001 |
| name | VARCHAR(255) | Customer company name | Acme Corp 1 |
| signup_date | DATE | Date customer signed up | 2023-10-11 |
| tier | VARCHAR(50) | Subscription tier | Free, Basic, Pro, Enterprise |
| status | VARCHAR(50) | Account status | Active, Churned |
| industry | VARCHAR(100) | Customer industry | Technology, Finance, Healthcare |
| region | VARCHAR(100) | Geographic region | North America, Europe, Asia Pacific |

**Sample Data:**
```
CUST_0001, Acme Corp 1, 2023-10-11, Free, Active, Manufacturing, Asia Pacific
CUST_0002, TechStart Inc 2, 2023-11-12, Enterprise, Active, Manufacturing, Asia Pacific
```

**Business Rules:**
- customer_id is unique and auto-generated
- Tiers: Free (no cost), Basic ($99-199), Pro ($499-999), Enterprise ($2000-10000)
- Status: Active (80%), Churned (20%)
- 5 industries: Technology, Finance, Healthcare, Retail, Manufacturing
- 4 regions: North America, Europe, Asia Pacific, Latin America

---

### CREDIT_USAGE
**Purpose**: Track credit consumption by customers

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| usage_id | VARCHAR(50) | Unique usage record ID (PK) | USG_00001 |
| customer_id | VARCHAR(50) | Reference to customer (FK) | CUST_0096 |
| date | DATE | Date of credit usage | 2023-04-05 |
| credits_used | DECIMAL(18,2) | Number of credits consumed | 4744.82 |
| feature | VARCHAR(100) | Feature/service used | ML_Training |
| execution_time_ms | INTEGER | Execution time in milliseconds | 1805 |

**Sample Data:**
```
USG_00001, CUST_0096, 2023-04-05, 4744.82, ML_Training, 1805
USG_00003, CUST_0098, 2024-03-16, 39194.33, Data_Transfer, 3567
```

**Business Rules:**
- usage_id is unique and auto-generated
- customer_id must reference valid customer
- Credits scale by tier: Free (1x), Basic (5x), Pro (20x), Enterprise (100x)
- 6 features: API_Calls, Storage, Compute, Analytics, ML_Training, Data_Transfer
- Only active customers generate regular usage (80% of records)
- Execution time ranges: 100-5000ms

---

### BILLING
**Purpose**: Store billing and payment information

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| billing_id | VARCHAR(50) | Unique billing record ID (PK) | BILL_00001 |
| customer_id | VARCHAR(50) | Reference to customer (FK) | CUST_0057 |
| billing_date | DATE | Date of billing | 2024-09-22 |
| amount | DECIMAL(18,2) | Billing amount | 6558.52 |
| period | VARCHAR(10) | Billing period (YYYY-MM) | 2024-09 |
| status | VARCHAR(50) | Payment status | Paid, Pending, Overdue |
| currency | VARCHAR(10) | Currency code | USD |

**Sample Data:**
```
BILL_00001, CUST_0057, 2024-09-22, 6558.52, 2024-09, Paid, USD
BILL_00002, CUST_0091, 2024-11-21, 166.66, 2024-11, Paid, USD
```

**Business Rules:**
- billing_id is unique and auto-generated
- customer_id must reference valid customer
- Amount varies by tier:
  - Free: $0
  - Basic: $99-199/month
  - Pro: $499-999/month
  - Enterprise: $2000-10000/month
- Payment status: Paid (majority), Pending, Overdue
- All amounts in USD

---

## Analytical Views

### customer_health_scores
**Purpose**: Calculate customer health scores based on multiple factors

**Calculation:**
```
Health Score (0-100) = 
  Usage Recency Score (0-40) +
  Usage Frequency Score (0-30) +
  Revenue Score (0-30)
```

**Health Categories:**
- Healthy: 80-100
- At Risk: 60-79
- Needs Attention: 40-59
- Critical: 0-39

**Churn Risk:**
- High Risk: 60+ days inactive
- Medium Risk: 30+ days inactive
- Low Risk: Active usage

---

### mom_credit_trends
**Purpose**: Track month-over-month credit consumption trends

**Key Metrics:**
- monthly_credits: Total credits used in month
- prev_month_credits: Previous month's usage
- mom_change: Absolute change
- mom_change_pct: Percentage change

**Trend Categories:**
- Growing Fast: +20% or more
- Growing: Positive growth
- Stable: Minimal change
- Declining: Negative growth
- Declining Fast: -20% or more

**Uses LAG window function** to compare sequential months.

---

### customer_segments
**Purpose**: Segment customers by usage patterns

**Segments (NTILE quartiles):**
- Power User (Q4): Top 25% by usage
- High User (Q3): 50-75th percentile
- Medium User (Q2): 25-50th percentile
- Low User (Q1): Bottom 25%

---

### revenue_by_tier_region
**Purpose**: Revenue analysis by tier and region

**Metrics:**
- customer_count: Total customers
- active_customers: Active customers
- total_revenue: Sum of revenue
- avg_transaction_amount: Average billing
- revenue_per_customer: ARPU

---

### feature_usage_analysis
**Purpose**: Track feature adoption and usage

**Metrics:**
- usage_count: Number of uses
- unique_customers: Distinct users
- total_credits: Credits consumed
- avg_credits_per_use: Average cost
- avg_execution_time_ms: Performance

---

### churn_indicators
**Purpose**: Identify at-risk customers

**Red Flags:**
- inactive_30_days: No activity in 30+ days
- low_engagement: Less than 5 active days
- low_usage: Less than 100 credits

**Churn Risk Score (0-3):** Sum of red flags

---

### billing_health
**Purpose**: Monitor payment health

**Metrics:**
- total_invoices: All invoices
- paid_invoices: Paid count
- pending_invoices: Pending count
- overdue_invoices: Overdue count
- outstanding_amount: Unpaid total

**Payment Health:**
- Good: No overdue, few pending
- Fair: Multiple pending
- Poor: Has overdue invoices

---

## Data Relationships

### One-to-Many Relationships

1. **CUSTOMERS → CREDIT_USAGE**
   - One customer can have many usage records
   - Foreign key: credit_usage.customer_id → customers.customer_id

2. **CUSTOMERS → BILLING**
   - One customer can have many billing records
   - Foreign key: billing.customer_id → customers.customer_id

### Referential Integrity

- Foreign key constraints enforce data integrity
- Cannot delete customer with existing usage/billing records
- CASCADE or SET NULL options available if needed

---

## Sample Queries

### Get customer with usage and billing
```sql
SELECT 
    c.*,
    COUNT(DISTINCT cu.usage_id) as usage_count,
    SUM(cu.credits_used) as total_credits,
    COUNT(DISTINCT b.billing_id) as billing_count,
    SUM(b.amount) as total_revenue
FROM customers c
LEFT JOIN credit_usage cu ON c.customer_id = cu.customer_id
LEFT JOIN billing b ON c.customer_id = b.customer_id
WHERE c.customer_id = 'CUST_0001'
GROUP BY c.customer_id, c.name, c.signup_date, c.tier, c.status, c.industry, c.region;
```

### Get top customers by credits
```sql
SELECT 
    c.customer_id,
    c.name,
    c.tier,
    SUM(cu.credits_used) as total_credits
FROM customers c
JOIN credit_usage cu ON c.customer_id = cu.customer_id
GROUP BY c.customer_id, c.name, c.tier
ORDER BY total_credits DESC
LIMIT 10;
```

---

## Data Volume

- **Customers**: 100 records
- **Credit Usage**: 400+ records (varies due to active customer filtering)
- **Billing**: 200 records

**Total data size**: ~42 KB (CSV files)

---

## Data Quality

### Validation Rules
- All IDs are unique
- All foreign keys reference valid records
- Dates are in valid format (YYYY-MM-DD)
- Numeric fields have appropriate precision
- No NULL values in required fields

### Data Generation
- Random but realistic data
- Proper relationships maintained
- Configurable data volume
- Reproducible with seed
