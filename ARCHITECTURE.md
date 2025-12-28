# Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Snowflake SaaS Analytics Pipeline                │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  Data Generation │
└────────┬─────────┘
         │
         │ scripts/generate_synthetic_data.py
         │
         v
┌─────────────────────────────────────────┐
│         CSV Files (data/)               │
│  • customers.csv                        │
│  • credit_usage.csv                     │
│  • billing.csv                          │
└────────┬────────────────────────────────┘
         │
         │ scripts/snowflake_setup.py
         │ (snowflake-connector-python)
         v
┌─────────────────────────────────────────────────────────────┐
│                   Snowflake Database                         │
│  ┌────────────────────────────────────────────────────┐    │
│  │ Database: SAAS_ANALYTICS                           │    │
│  │ Schema: PUBLIC                                     │    │
│  │                                                    │    │
│  │ Tables:                                            │    │
│  │  • customers                                       │    │
│  │  • credit_usage                                    │    │
│  │  • billing                                         │    │
│  │                                                    │    │
│  │ Views (sql/analytics_queries.sql):                │    │
│  │  • customer_health_scores                         │    │
│  │  • mom_credit_trends                              │    │
│  │  • customer_segments                              │    │
│  │  • revenue_by_tier_region                         │    │
│  │  • feature_usage_analysis                         │    │
│  │  • churn_indicators                               │    │
│  │  • billing_health                                 │    │
│  └────────────────────────────────────────────────────┘    │
└────────┬────────────────────────────────────────────────────┘
         │
         │ Snowpark Processing
         │ (snowflake-snowpark-python)
         │
         v
┌─────────────────────────────────────────────────────────────┐
│              Streamlit Dashboard (dashboard/app.py)         │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Visualizations (Plotly):                          │    │
│  │  • Key Metrics (customers, revenue, credits)       │    │
│  │  • Revenue Analysis (tier, region, trends)         │    │
│  │  • Usage Analysis (features, tiers)                │    │
│  │  • MoM Trends (customer comparison)                │    │
│  │  • Customer Health (scores, categories)            │    │
│  │                                                    │    │
│  │  Features:                                         │    │
│  │  • Interactive filters                             │    │
│  │  • Real-time data refresh                          │    │
│  │  • Demo mode (no Snowflake needed)                 │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
         │
         v
┌─────────────────────────────────────────┐
│         User Browser                    │
│      (http://localhost:8501)            │
└─────────────────────────────────────────┘
```

## Data Flow

```
1. Generate Data
   └─> scripts/generate_synthetic_data.py
       └─> Creates CSV files in data/

2. Load to Snowflake
   └─> scripts/snowflake_setup.py
       ├─> Creates database & schema
       ├─> Creates stage
       ├─> Creates tables
       ├─> Uploads CSVs to stage (PUT)
       ├─> Loads data (COPY INTO)
       └─> Processes with Snowpark

3. Analytics
   └─> sql/analytics_queries.sql
       └─> Creates analytical views

4. Visualization
   └─> dashboard/app.py
       ├─> Connects to Snowflake
       ├─> Queries views
       ├─> Generates charts (Plotly)
       └─> Displays in Streamlit
```

## Key Components

### 1. Data Generation Layer
- **Purpose**: Generate realistic synthetic data for testing
- **Technology**: Python with CSV library
- **Output**: 3 CSV files with relational data

### 2. Data Ingestion Layer
- **Purpose**: Load data into Snowflake
- **Technology**: snowflake-connector-python
- **Features**: 
  - Automated database setup
  - File staging
  - Bulk loading with COPY
  - Foreign key constraints

### 3. Data Processing Layer
- **Purpose**: Transform and aggregate data
- **Technology**: Snowpark (Python API for Snowflake)
- **Features**:
  - DataFrame operations
  - Aggregations
  - Joins
  - View creation

### 4. Analytics Layer
- **Purpose**: Business intelligence queries
- **Technology**: SQL (Snowflake SQL)
- **Queries**:
  - Customer health scoring (multi-factor)
  - MoM trends (LAG window functions)
  - Segmentation (NTILE)
  - Revenue analysis (GROUP BY)
  - Churn prediction (CASE statements)

### 5. Visualization Layer
- **Purpose**: Interactive dashboard
- **Technology**: Streamlit + Plotly
- **Features**:
  - Real-time data refresh
  - Interactive charts
  - Filters and drill-downs
  - Responsive layout

## Technology Stack

```
┌─────────────────────────────────────────┐
│           Frontend                      │
│  • Streamlit (Dashboard UI)             │
│  • Plotly (Interactive Charts)          │
└─────────────────────────────────────────┘
                  ↕
┌─────────────────────────────────────────┐
│         Data Processing                 │
│  • Snowpark (Python API)                │
│  • Pandas (DataFrames)                  │
└─────────────────────────────────────────┘
                  ↕
┌─────────────────────────────────────────┐
│          Data Warehouse                 │
│  • Snowflake (Cloud DW)                 │
│  • SQL Analytics                        │
└─────────────────────────────────────────┘
                  ↕
┌─────────────────────────────────────────┐
│           Data Storage                  │
│  • CSV Files (Synthetic Data)           │
│  • Snowflake Tables                     │
└─────────────────────────────────────────┘
```

## Customer Health Score Algorithm

```
Health Score = Usage Recency Score (0-40)
             + Usage Frequency Score (0-30)
             + Revenue Score (0-30)
             = Total (0-100)

Categories:
  • Healthy: 80-100
  • At Risk: 60-79
  • Needs Attention: 40-59
  • Critical: 0-39

Churn Risk:
  • High Risk: No usage in 60+ days
  • Medium Risk: No usage in 30+ days
  • Low Risk: Active usage
```

## Usage Patterns

### Development Workflow
```
1. Setup environment → ./setup.sh
2. Configure credentials → edit .env
3. Generate data → python scripts/generate_synthetic_data.py
4. Load to Snowflake → python scripts/snowflake_setup.py
5. Run dashboard → streamlit run dashboard/app.py
```

### Production Workflow
```
1. Schedule data ingestion (e.g., daily)
2. Run Snowpark transformations
3. Update analytical views
4. Dashboard auto-refreshes
5. Monitor customer health scores
6. Alert on churn risks
```

## Security Considerations

```
┌─────────────────────────────────────────┐
│      Security Best Practices            │
├─────────────────────────────────────────┤
│  ✓ Environment variables for creds      │
│  ✓ .env excluded from git               │
│  ✓ Snowflake RBAC                       │
│  ✓ Connection encryption                │
│  ✓ No hardcoded passwords               │
│  ✓ Minimal privilege principle          │
└─────────────────────────────────────────┘
```

## Extensibility

The architecture supports easy extensions:

- **Add new data sources**: Extend data generation or connect to APIs
- **Add new metrics**: Create new SQL views
- **Add new visualizations**: Extend Streamlit dashboard
- **Add ML models**: Use Snowpark ML for predictions
- **Add alerts**: Integrate with notification services
- **Add authentication**: Add Streamlit auth or SSO
