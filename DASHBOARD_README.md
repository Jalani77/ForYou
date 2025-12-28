# SaaS Analytics Dashboard - Enterprise Edition

## 📊 Production-Grade Snowflake Analytics Platform

A sophisticated, enterprise-level analytics dashboard built with **Snowflake**, **Snowpark for Python**, and **Streamlit**. Features advanced business intelligence capabilities including customer health scoring, predictive churn analysis, and real-time interactive visualizations.

---

## 🎯 Key Features

### Advanced Analytics
- **Customer Health Scoring**: Weighted multi-factor health assessment (Revenue 30%, Usage 30%, Engagement 20%, Payment Reliability 20%)
- **Predictive Churn Analysis**: 30-day rolling window trend analysis to identify at-risk customers
- **Dynamic Filtering**: Real-time reactive filters for date ranges, customer tiers, and status
- **Credit Consumption Trends**: Multi-line time-series visualization of top customers
- **Revenue Analytics**: Comprehensive revenue breakdowns by tier, region, and customer segments

### Technical Excellence
- **Modular Architecture**: Separation of concerns with dedicated modules for:
  - Connection management (`config/`)
  - Data transformation layer (`data_layer/`)
  - Visualization components (`components/`)
- **Enterprise-Grade Error Handling**: Comprehensive exception handling with fallback to demo mode
- **Professional Dark Theme UI**: Custom CSS with polished, non-standard design
- **Snowpark Integration**: Leverages Snowflake's Python-native data processing
- **Comprehensive Logging**: Application-wide logging for monitoring and debugging

---

## 🏗️ Architecture

```
dashboard/
├── app_refactored.py              # Main application entry point
├── config/
│   ├── __init__.py
│   └── snowflake_connection.py    # Connection manager with singleton pattern
├── data_layer/
│   ├── __init__.py
│   └── data_transformation.py     # Business logic and data operations
└── components/
    ├── __init__.py
    └── visualizations.py          # Reusable visualization components
```

### Design Patterns
- **Singleton Pattern**: Connection manager for efficient resource usage
- **Factory Pattern**: Visualization component creation
- **Service Layer**: Data transformation abstraction
- **Separation of Concerns**: Clear boundaries between UI, logic, and data access

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Snowflake account (optional - app runs in demo mode without credentials)
- Modern web browser

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/Jalani77/ForYou.git
cd ForYou
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure Snowflake credentials** (optional)
```bash
# Create .env file or set environment variables
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=SAAS_ANALYTICS
export SNOWFLAKE_SCHEMA=PUBLIC
```

4. **Launch the dashboard**
```bash
streamlit run dashboard/app_refactored.py
# Or with custom port:
python -m streamlit run dashboard/app_refactored.py --server.port 8501 --server.address 0.0.0.0
```

5. **Access the dashboard**
```
http://localhost:8501
```

---

## 💻 Dashboard Features

### Interactive Sidebar Filters
- **📅 Date Range Selector**: Date input widgets with quick preset buttons (Last 30/60/90 days, Last Year)
- **🎯 Customer Tier Filter**: Multi-select dropdown for Free, Basic, Pro, Enterprise tiers
- **📊 Status Toggles**: Active/Inactive customer filters
- **⚙️ Advanced Filters**: Churn risk threshold and minimum health score sliders

### Key Performance Indicators
- Total Customers with activity percentage
- Active Customer count
- Total Revenue with average per customer
- Credit utilization metrics

### Visualizations

#### 1. Credit Consumption Trends
- **Multi-line time-series chart** showing daily credit usage for top 10 customers
- Interactive hover details with customer name, date, and credit amount
- Professional dark theme color palette

#### 2. Customer Health Distribution
- **Segmented bar chart** displaying health categories (Healthy, At Risk, Critical) by tier
- Color-coded risk levels (Green, Yellow, Red)
- Top 5 healthiest customers data table

#### 3. Churn Risk Analysis
- **Segmented bar chart** showing churn risk distribution (High, Medium, Low, Healthy)
- Based on 30-day rolling credit usage trends
- Alerts table for top 5 at-risk customers with usage trend percentages

#### 4. Revenue Analysis
- **Donut chart** for revenue distribution by customer tier
- **Heatmap** showing revenue by tier × region intersection
- Interactive tooltips with detailed breakdowns

#### 5. Top Customers Table
- Clean data table showing top 5 customers by credit utilization
- Includes tier, region, total credits, active days, and last usage date

---

## 📐 Data Model & Business Logic

### Customer Health Score Calculation
```python
Health Score = (Revenue Score × 0.30) + 
               (Usage Score × 0.30) + 
               (Engagement Score × 0.20) + 
               (Payment Reliability × 0.20)
```

**Categories:**
- **Healthy**: Score ≥ 80
- **At Risk**: 60 ≤ Score < 80
- **Critical**: Score < 60

### Churn Prediction Algorithm
```python
# 30-day rolling window comparison
Current Avg Usage vs. Previous Avg Usage
Usage Trend % = ((Current - Previous) / Previous) × 100

Churn Risk:
- High Risk: Usage decline > 30%
- Medium Risk: Usage decline 15-30%
- Low Risk: Usage decline 0-15%
- Healthy: Usage stable or growing
```

---

## 🎨 UI/UX Design

### Professional Dark Theme
- **Primary Color**: `#00D4FF` (Cyan)
- **Secondary**: `#FF6B9D` (Pink)
- **Success**: `#00E396` (Green)
- **Warning**: `#FEB019` (Orange)
- **Danger**: `#FF4560` (Red)
- **Background**: Dark gradient (`#1a1a2e` to `#16213e`)

### Custom Components
- Gradient metric cards with icons
- Hover effects on interactive elements
- Polished button styling with shadows
- Responsive layout for all screen sizes

---

## 🔧 Configuration

### Environment Variables
```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=username
SNOWFLAKE_PASSWORD=password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SAAS_ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=SYSADMIN
```

### Demo Mode
If Snowflake credentials are not provided, the application automatically switches to **demo mode** with synthetic data for testing and development purposes.

---

## 📊 Sample SQL Queries

### Customer Health Scores
```sql
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.name,
        c.tier,
        -- Revenue score (0-100)
        CASE 
            WHEN SUM(b.amount) >= 10000 THEN 100
            WHEN SUM(b.amount) >= 5000 THEN 80
            WHEN SUM(b.amount) >= 1000 THEN 60
            ELSE 40
        END as revenue_score,
        -- Usage, engagement, payment scores...
    FROM customers c
    LEFT JOIN billing b ON c.customer_id = b.customer_id
    LEFT JOIN credit_usage cu ON c.customer_id = cu.customer_id
    GROUP BY c.customer_id, c.name, c.tier
)
SELECT 
    customer_id,
    name,
    ROUND((revenue_score * 0.30) + (usage_score * 0.30) + 
          (engagement_score * 0.20) + (payment_score * 0.20)) as health_score
FROM customer_metrics
ORDER BY health_score DESC;
```

### Churn Predictions
```sql
WITH daily_usage AS (
    SELECT 
        customer_id,
        DATE_TRUNC('day', usage_date) as usage_date,
        SUM(credits_used) as daily_credits
    FROM credit_usage
    WHERE usage_date >= DATEADD('day', -60, CURRENT_DATE())
    GROUP BY customer_id, DATE_TRUNC('day', usage_date)
),
windowed_usage AS (
    SELECT 
        customer_id,
        AVG(daily_credits) OVER (
            PARTITION BY customer_id 
            ORDER BY usage_date 
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) as rolling_avg_current,
        AVG(daily_credits) OVER (
            PARTITION BY customer_id 
            ORDER BY usage_date 
            ROWS BETWEEN 60 PRECEDING AND 30 PRECEDING
        ) as rolling_avg_previous
    FROM daily_usage
)
SELECT 
    customer_id,
    ((rolling_avg_current - rolling_avg_previous) / rolling_avg_previous * 100) as usage_trend_pct,
    CASE 
        WHEN usage_trend_pct <= -30 THEN 'High Risk'
        WHEN usage_trend_pct <= -15 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as churn_risk_category
FROM windowed_usage;
```

---

## 🧪 Testing

### Manual Testing Checklist
- [x] Application launches successfully
- [x] Demo mode works without credentials
- [x] Date range filters update visualizations
- [x] Tier filters affect all charts
- [x] Status toggles work correctly
- [x] Quick preset buttons function
- [x] All visualizations render properly
- [x] Error handling displays appropriately
- [x] Responsive design on different screen sizes

### Performance Optimization
- Streamlit caching for connection manager (`@st.cache_resource`)
- Efficient Snowpark query execution
- Minimized re-renders with session state management

---

## 📝 API Documentation

### SnowflakeConnectionManager
```python
class SnowflakeConnectionManager:
    """Enterprise-grade connection manager with singleton pattern"""
    
    def establish_connection() -> bool:
        """Establish Snowflake connection with error handling"""
    
    def get_snowpark_session() -> Optional[Session]:
        """Create Snowpark session for advanced operations"""
    
    def test_connection() -> bool:
        """Test active connection status"""
```

### DataTransformationLayer
```python
class DataTransformationLayer:
    """Business logic and data transformation layer"""
    
    def get_filtered_customers(start_date, end_date, tiers, statuses) -> pd.DataFrame:
        """Get customers with dynamic filters"""
    
    def calculate_customer_health_scores() -> pd.DataFrame:
        """Calculate weighted health scores"""
    
    def calculate_churn_predictions(window_days=30) -> pd.DataFrame:
        """Predict churn risk based on usage trends"""
```

### VisualizationComponents
```python
class VisualizationComponents:
    """Professional visualization components"""
    
    @staticmethod
    def create_multi_line_chart(df, x_col, y_col, group_col) -> go.Figure:
        """Create multi-line trend chart"""
    
    @staticmethod
    def create_segmented_bar_chart(df, x_col, y_col, segment_col) -> go.Figure:
        """Create segmented bar chart for risk analysis"""
```

---

## 🤝 Contributing

### Development Workflow
1. Fork the repository
2. Create a feature branch
3. Implement changes with comprehensive error handling
4. Add documentation and type hints
5. Test thoroughly (with and without Snowflake connection)
6. Submit pull request with detailed description

### Code Style
- Follow PEP 8 guidelines
- Use type hints for function parameters
- Add docstrings to all functions and classes
- Include error handling for external dependencies

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 👥 Authors

- **Senior Data Engineer** - Initial architecture and implementation
- **Analytics Developer** - Business logic and visualizations

---

## 🙏 Acknowledgments

- Snowflake for powerful cloud data platform
- Streamlit for elegant web app framework
- Plotly for interactive visualizations
- Python community for excellent libraries

---

## 📧 Support

For issues, questions, or contributions:
- **GitHub Issues**: https://github.com/Jalani77/ForYou/issues
- **Pull Requests**: https://github.com/Jalani77/ForYou/pulls

---

**Built with ❤️ using Snowflake, Python, and Streamlit**
