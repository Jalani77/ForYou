# ForYou - Snowflake SaaS Analytics Pipeline

A comprehensive Snowflake-based analytics pipeline for SaaS business metrics, including customer health scoring, revenue analysis, and usage tracking.

## 🚀 Features

- **Synthetic Data Generation**: Generate realistic CSV files for customers, credit usage, and billing
- **Snowflake Integration**: Automated database setup, data staging, and loading using snowflake-connector-python
- **Snowpark Processing**: Advanced data processing using Snowflake's Snowpark library
- **Customer Health Scoring**: Calculate customer health scores based on usage patterns and engagement
- **MoM Analysis**: Track month-over-month credit consumption trends
- **Interactive Dashboard**: Streamlit-based dashboard with rich visualizations using Plotly
- **Comprehensive Analytics**: Pre-built SQL queries for customer segmentation, revenue analysis, and churn prediction

## 📁 Project Structure

```
ForYou/
├── data/                      # Generated CSV files
│   ├── customers.csv
│   ├── credit_usage.csv
│   └── billing.csv
├── scripts/                   # Python scripts
│   ├── generate_synthetic_data.py
│   └── snowflake_setup.py
├── sql/                       # SQL analytics queries
│   └── analytics_queries.sql
├── dashboard/                 # Streamlit dashboard
│   └── app.py
├── requirements.txt          # Python dependencies
├── config.env.template       # Configuration template
├── .gitignore               # Git ignore file
└── README.md                # This file
```

## 🛠️ Setup

### Prerequisites

- Python 3.8 or higher
- Snowflake account (trial account works fine)
- pip package manager

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Jalani77/ForYou.git
   cd ForYou
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Snowflake credentials**
   ```bash
   cp config.env.template .env
   # Edit .env and add your Snowflake credentials
   ```

   Or set environment variables directly:
   ```bash
   export SNOWFLAKE_ACCOUNT=your_account_identifier
   export SNOWFLAKE_USER=your_username
   export SNOWFLAKE_PASSWORD=your_password
   export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   export SNOWFLAKE_DATABASE=SAAS_ANALYTICS
   export SNOWFLAKE_SCHEMA=PUBLIC
   export SNOWFLAKE_ROLE=ACCOUNTADMIN
   ```

## 📊 Usage

### Step 1: Generate Synthetic Data

Generate realistic CSV files for testing:

```bash
python scripts/generate_synthetic_data.py
```

This creates three CSV files in the `data/` directory:
- `customers.csv` - Customer information (100 records)
- `credit_usage.csv` - Credit usage transactions (500+ records)
- `billing.csv` - Billing records (200+ records)

### Step 2: Setup Snowflake Database

Run the Snowflake setup script to:
- Create database and schema
- Create staging area
- Create tables
- Load data from CSV files
- Process data using Snowpark

```bash
python scripts/snowflake_setup.py
```

### Step 3: Run SQL Analytics Queries

Connect to Snowflake and run the pre-built analytics queries:

```bash
snowsql -a your_account -u your_username -f sql/analytics_queries.sql
```

Or use the Snowflake web interface to execute queries from `sql/analytics_queries.sql`.

### Step 4: Launch Dashboard

Start the Streamlit dashboard:

```bash
streamlit run dashboard/app.py
```

The dashboard will open in your browser at `http://localhost:8501`.

## 📈 Analytics Queries

The SQL file includes comprehensive analytics views:

1. **Customer Health Scores** - Multi-factor health scoring based on usage, recency, and revenue
2. **MoM Credit Trends** - Month-over-month credit consumption analysis
3. **Customer Segmentation** - Automatic customer segmentation by usage patterns
4. **Revenue Analysis** - Revenue breakdown by tier and region
5. **Feature Usage Analysis** - Track which features are most used
6. **Churn Indicators** - Early warning system for potential churn
7. **Billing Health** - Monitor payment status and billing issues

## 🎨 Dashboard Features

The Streamlit dashboard provides:

- **Key Metrics**: Total customers, active customers, revenue, and credits used
- **Revenue Analysis**: 
  - Revenue by customer tier
  - Revenue distribution by region
  - Monthly revenue trends
- **Usage Analysis**:
  - Credits used by feature
  - Usage distribution by tier
- **MoM Trends**:
  - Interactive customer comparison
  - Month-over-month change percentages
- **Customer Health**:
  - Health score distribution
  - Health category breakdown
  - Health vs. revenue correlation
  - Top customers table

## 🔒 Security Best Practices

- Never commit `.env` file or actual credentials to version control
- Use Snowflake role-based access control
- Rotate credentials regularly
- Use separate accounts for development and production
- Enable multi-factor authentication on Snowflake

## 🧪 Testing

The project includes synthetic data generation for testing without real customer data. The dashboard can run in demo mode if Snowflake credentials are not available.

## 📝 Snowpark Features

The project uses Snowpark for:
- Reading tables as DataFrames
- Aggregating customer metrics
- Joining datasets
- Creating views for analytics

Snowpark provides a Pythonic API for data processing directly in Snowflake, reducing data movement and improving performance.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is open source and available under the MIT License.

## 🙋 Support

For issues or questions:
- Open an issue on GitHub
- Check Snowflake documentation: https://docs.snowflake.com/
- Review Streamlit docs: https://docs.streamlit.io/

## 🎯 Next Steps

Potential enhancements:
- Add predictive churn modeling with ML
- Implement real-time alerting for at-risk customers
- Add more advanced Snowpark transformations
- Create scheduled reports
- Add user authentication to dashboard
- Implement A/B testing analytics
- Add cohort analysis

---

**Built with ❤️ using Snowflake, Snowpark, and Streamlit**