# Quick Start Guide

This guide will help you get the Snowflake SaaS Analytics Pipeline up and running quickly.

## Prerequisites

Before you begin, make sure you have:

1. **Python 3.8+** installed on your system
2. **Snowflake account** (free trial available at https://signup.snowflake.com/)
3. **Git** for cloning the repository
4. **Snowflake credentials**: account identifier, username, and password

## Quick Setup (5 minutes)

### Option 1: Automated Setup (Linux/Mac)

```bash
# Clone the repository
git clone https://github.com/Jalani77/ForYou.git
cd ForYou

# Run the setup script
./setup.sh

# Edit .env with your credentials
nano .env

# Source environment variables
source .env

# Setup Snowflake (creates database, loads data)
python3 scripts/snowflake_setup.py

# Launch dashboard
streamlit run dashboard/app.py
```

### Option 2: Manual Setup (All Platforms)

```bash
# Clone the repository
git clone https://github.com/Jalani77/ForYou.git
cd ForYou

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Generate synthetic data
python scripts/generate_synthetic_data.py

# Configure Snowflake credentials
cp config.env.template .env
# Edit .env with your credentials

# Set environment variables
# On Linux/Mac:
export $(cat .env | xargs)
# On Windows (PowerShell):
Get-Content .env | ForEach-Object { $var = $_.Split('='); [Environment]::SetEnvironmentVariable($var[0], $var[1], 'Process') }

# Setup Snowflake database
python scripts/snowflake_setup.py

# Launch dashboard
streamlit run dashboard/app.py
```

## Finding Your Snowflake Account Identifier

Your Snowflake account identifier is in your Snowflake URL:

```
https://<account_identifier>.snowflakecomputing.com
```

For example, if your URL is:
```
https://xy12345.us-east-1.aws.snowflakecomputing.com
```

Your account identifier is: `xy12345.us-east-1.aws`

## What Gets Created in Snowflake

The setup script creates:

1. **Database**: `SAAS_ANALYTICS`
2. **Schema**: `PUBLIC`
3. **Tables**:
   - `customers` - Customer information
   - `credit_usage` - Credit usage transactions
   - `billing` - Billing records
4. **Stage**: `saas_data_stage` - For CSV file uploads
5. **Views**:
   - `customer_health_scores` - Customer health metrics
   - `mom_credit_trends` - Month-over-month trends
   - `customer_segments` - Customer segmentation
   - `revenue_by_tier_region` - Revenue analysis
   - `feature_usage_analysis` - Feature usage stats
   - `churn_indicators` - Churn risk indicators
   - `billing_health` - Billing health metrics

## Dashboard Features

Once the dashboard is running (usually at http://localhost:8501), you can:

1. **View Key Metrics**: See total customers, revenue, and credit usage
2. **Analyze Revenue**: Break down by tier and region
3. **Track Usage**: Monitor credit consumption by feature and tier
4. **MoM Trends**: Compare customer usage over time
5. **Customer Health**: Identify at-risk customers

## Demo Mode

If you don't have Snowflake credentials yet, the dashboard can run in **demo mode** with synthetic data. Just skip the Snowflake setup step and launch the dashboard directly:

```bash
streamlit run dashboard/app.py
```

## Troubleshooting

### "Module not found" errors
- Make sure you activated the virtual environment
- Run `pip install -r requirements.txt` again

### "Connection refused" from Snowflake
- Check your account identifier format
- Verify username and password
- Ensure your IP is not blocked by network policies

### "Database already exists" warnings
- This is normal on subsequent runs
- The setup script uses `CREATE OR REPLACE` statements

### Dashboard shows "Demo Mode"
- Snowflake credentials are not set or invalid
- Check your `.env` file and environment variables

## Next Steps

After setup, you can:

1. **Explore the SQL queries**: Check `sql/analytics_queries.sql`
2. **Customize the data**: Modify `scripts/generate_synthetic_data.py`
3. **Add more visualizations**: Edit `dashboard/app.py`
4. **Run queries in Snowflake**: Use Snowflake web UI or snowsql CLI

## Getting Help

- Review the main [README.md](README.md) for detailed documentation
- Check [Snowflake Documentation](https://docs.snowflake.com/)
- Review [Streamlit Documentation](https://docs.streamlit.io/)

## Security Reminder

⚠️ **Never commit your `.env` file or credentials to version control!**

The `.gitignore` file is already configured to exclude it, but always double-check before committing.
