"""
Streamlit Dashboard for Snowflake SaaS Analytics
Visualizes revenue, usage, and customer health metrics.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta

# Try to import Snowflake connector
try:
    import snowflake.connector
    from snowflake.snowpark import Session
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    st.warning("⚠ Snowflake connector not available. Using demo data mode.")


# Page configuration
st.set_page_config(
    page_title="SaaS Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 1px 1px 3px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)


class SnowflakeDashboard:
    """Manages Snowflake connection and data retrieval for dashboard."""
    
    def __init__(self):
        self.conn = None
        self.session = None
        self.demo_mode = False
    
    def connect(self):
        """Connect to Snowflake."""
        if not SNOWFLAKE_AVAILABLE:
            self.demo_mode = True
            return False
        
        try:
            account = os.getenv("SNOWFLAKE_ACCOUNT")
            user = os.getenv("SNOWFLAKE_USER")
            password = os.getenv("SNOWFLAKE_PASSWORD")
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
            database = os.getenv("SNOWFLAKE_DATABASE", "SAAS_ANALYTICS")
            schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
            
            if not all([account, user, password]):
                self.demo_mode = True
                return False
            
            self.conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema
            )
            return True
        except Exception as e:
            st.error(f"Error connecting to Snowflake: {e}")
            self.demo_mode = True
            return False
    
    def query(self, sql):
        """Execute SQL query and return DataFrame."""
        if self.demo_mode or not self.conn:
            return self.get_demo_data(sql)
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            st.error(f"Query error: {e}")
            return pd.DataFrame()
    
    def get_demo_data(self, sql):
        """Generate demo data for testing without Snowflake connection."""
        import random
        
        if "customer_health_scores" in sql.lower():
            data = {
                'CUSTOMER_ID': [f'CUST_{i:04d}' for i in range(1, 21)],
                'NAME': [f'Customer {i}' for i in range(1, 21)],
                'TIER': [random.choice(['Free', 'Basic', 'Pro', 'Enterprise']) for _ in range(20)],
                'TOTAL_CREDITS_USED': [random.uniform(1000, 50000) for _ in range(20)],
                'HEALTH_SCORE': [random.randint(40, 100) for _ in range(20)],
                'HEALTH_CATEGORY': [random.choice(['Healthy', 'At Risk', 'Needs Attention']) for _ in range(20)],
                'TOTAL_REVENUE': [random.uniform(0, 10000) for _ in range(20)],
            }
        elif "mom_credit_trends" in sql.lower():
            months = pd.date_range(start='2023-01', end='2024-12', freq='MS')
            data = {
                'USAGE_MONTH': months.tolist() * 5,
                'CUSTOMER_ID': [f'CUST_{i:04d}' for i in range(1, 6)] * len(months),
                'MONTHLY_CREDITS': [random.uniform(1000, 10000) for _ in range(len(months) * 5)],
                'MOM_CHANGE_PCT': [random.uniform(-30, 50) for _ in range(len(months) * 5)],
            }
        elif "revenue_by_tier_region" in sql.lower():
            data = {
                'TIER': ['Free', 'Basic', 'Pro', 'Enterprise'] * 4,
                'REGION': ['North America', 'Europe', 'Asia Pacific', 'Latin America'] * 4,
                'TOTAL_REVENUE': [random.uniform(1000, 100000) for _ in range(16)],
                'CUSTOMER_COUNT': [random.randint(5, 50) for _ in range(16)],
                'ACTIVE_CUSTOMERS': [random.randint(3, 45) for _ in range(16)],
            }
        elif "feature_usage_analysis" in sql.lower():
            features = ['API_Calls', 'Storage', 'Compute', 'Analytics', 'ML_Training', 'Data_Transfer']
            data = {
                'FEATURE': features * 4,
                'TIER': ['Free', 'Basic', 'Pro', 'Enterprise'] * len(features),
                'TOTAL_CREDITS': [random.uniform(500, 50000) for _ in range(len(features) * 4)],
                'USAGE_COUNT': [random.randint(10, 1000) for _ in range(len(features) * 4)],
            }
        elif "billing" in sql.lower() and "sum" in sql.lower():
            months = pd.date_range(start='2023-01', end='2024-12', freq='MS')
            data = {
                'PERIOD': [m.strftime('%Y-%m') for m in months],
                'TOTAL_AMOUNT': [random.uniform(10000, 100000) for _ in months],
            }
        else:
            data = {'MESSAGE': ['Demo data not available for this query']}
        
        return pd.DataFrame(data)
    
    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()


@st.cache_resource
def get_dashboard():
    """Get or create dashboard instance."""
    dashboard = SnowflakeDashboard()
    dashboard.connect()
    return dashboard


def display_header():
    """Display dashboard header."""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.title("📊 SaaS Analytics Dashboard")
        st.markdown("**Real-time insights into customer health, revenue, and usage**")
    
    with col2:
        if dashboard.demo_mode:
            st.warning("🔶 Demo Mode")
        else:
            st.success("✅ Connected")


def display_kpi_metrics(dashboard):
    """Display key performance indicators."""
    st.subheader("📈 Key Metrics")
    
    # Query for KPIs
    total_customers = dashboard.query("SELECT COUNT(*) as count FROM customers")
    active_customers = dashboard.query("SELECT COUNT(*) as count FROM customers WHERE status = 'Active'")
    total_revenue = dashboard.query("SELECT SUM(amount) as total FROM billing WHERE status = 'Paid'")
    total_credits = dashboard.query("SELECT SUM(credits_used) as total FROM credit_usage")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        customers = total_customers['COUNT'].iloc[0] if not total_customers.empty else 100
        st.metric("Total Customers", f"{customers:,.0f}")
    
    with col2:
        active = active_customers['COUNT'].iloc[0] if not active_customers.empty else 80
        # Calculate retention rate, avoid division by zero
        retention_pct = (active / customers * 100) if customers > 0 else 0
        st.metric("Active Customers", f"{active:,.0f}", delta=f"{retention_pct:.1f}%")
    
    with col3:
        revenue = total_revenue['TOTAL'].iloc[0] if not total_revenue.empty else 125000
        st.metric("Total Revenue", f"${revenue:,.0f}")
    
    with col4:
        credits = total_credits['TOTAL'].iloc[0] if not total_credits.empty else 500000
        st.metric("Total Credits Used", f"{credits:,.0f}")


def display_revenue_analysis(dashboard):
    """Display revenue analysis visualizations."""
    st.subheader("💰 Revenue Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by Tier
        revenue_by_tier = dashboard.query("""
            SELECT tier, SUM(total_revenue) as revenue, SUM(customer_count) as customers
            FROM revenue_by_tier_region
            GROUP BY tier
            ORDER BY revenue DESC
        """)
        
        if not revenue_by_tier.empty:
            fig = px.bar(revenue_by_tier, x='TIER', y='REVENUE', 
                        title='Revenue by Customer Tier',
                        labels={'REVENUE': 'Total Revenue ($)', 'TIER': 'Customer Tier'},
                        color='REVENUE',
                        color_continuous_scale='Blues')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data available")
    
    with col2:
        # Revenue by Region
        revenue_by_region = dashboard.query("""
            SELECT region, SUM(total_revenue) as revenue
            FROM revenue_by_tier_region
            GROUP BY region
            ORDER BY revenue DESC
        """)
        
        if not revenue_by_region.empty:
            fig = px.pie(revenue_by_region, values='REVENUE', names='REGION',
                        title='Revenue Distribution by Region',
                        color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data available")
    
    # Monthly Revenue Trend
    monthly_revenue = dashboard.query("""
        SELECT period, SUM(amount) as total_amount
        FROM billing
        WHERE status = 'Paid'
        GROUP BY period
        ORDER BY period
    """)
    
    if not monthly_revenue.empty:
        fig = px.line(monthly_revenue, x='PERIOD', y='TOTAL_AMOUNT',
                     title='Monthly Revenue Trend',
                     labels={'TOTAL_AMOUNT': 'Revenue ($)', 'PERIOD': 'Month'},
                     markers=True)
        fig.update_traces(line_color='#1f77b4', line_width=3)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No monthly revenue data available")


def display_usage_analysis(dashboard):
    """Display usage analysis visualizations."""
    st.subheader("🔥 Credit Usage Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Feature usage
        feature_usage = dashboard.query("""
            SELECT feature, SUM(total_credits) as credits, SUM(usage_count) as usage
            FROM feature_usage_analysis
            GROUP BY feature
            ORDER BY credits DESC
        """)
        
        if not feature_usage.empty:
            fig = px.bar(feature_usage, x='FEATURE', y='CREDITS',
                        title='Credits Used by Feature',
                        labels={'CREDITS': 'Total Credits', 'FEATURE': 'Feature'},
                        color='CREDITS',
                        color_continuous_scale='Viridis')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No feature usage data available")
    
    with col2:
        # Usage by tier
        usage_by_tier = dashboard.query("""
            SELECT c.tier, SUM(cu.credits_used) as total_credits
            FROM customers c
            JOIN credit_usage cu ON c.customer_id = cu.customer_id
            GROUP BY c.tier
            ORDER BY total_credits DESC
        """)
        
        if not usage_by_tier.empty:
            fig = px.pie(usage_by_tier, values='TOTAL_CREDITS', names='TIER',
                        title='Credit Distribution by Tier',
                        color_discrete_sequence=px.colors.sequential.Oranges)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No usage data available")


def display_mom_trends(dashboard):
    """Display Month-over-Month trends."""
    st.subheader("📊 Month-over-Month Credit Trends")
    
    # Get top customers by credit usage
    top_customers = dashboard.query("""
        SELECT DISTINCT customer_id, name
        FROM mom_credit_trends
        ORDER BY customer_id
        LIMIT 10
    """)
    
    if not top_customers.empty:
        selected_customers = st.multiselect(
            "Select customers to compare:",
            options=top_customers['CUSTOMER_ID'].tolist(),
            default=top_customers['CUSTOMER_ID'].tolist()[:5] if len(top_customers) >= 5 else top_customers['CUSTOMER_ID'].tolist()
        )
        
        if selected_customers:
            # Query MoM trends for selected customers
            customer_list = "','".join(selected_customers)
            mom_data = dashboard.query(f"""
                SELECT customer_id, name, usage_month, monthly_credits, mom_change_pct
                FROM mom_credit_trends
                WHERE customer_id IN ('{customer_list}')
                ORDER BY usage_month
            """)
            
            if not mom_data.empty:
                # Create line chart for MoM trends
                fig = px.line(mom_data, x='USAGE_MONTH', y='MONTHLY_CREDITS',
                            color='NAME',
                            title='Monthly Credit Usage Trends',
                            labels={'MONTHLY_CREDITS': 'Credits Used', 'USAGE_MONTH': 'Month'},
                            markers=True)
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
                
                # Display MoM change percentage
                if 'MOM_CHANGE_PCT' in mom_data.columns:
                    fig2 = px.bar(mom_data, x='USAGE_MONTH', y='MOM_CHANGE_PCT',
                                color='NAME',
                                title='Month-over-Month Change (%)',
                                labels={'MOM_CHANGE_PCT': 'MoM Change (%)', 'USAGE_MONTH': 'Month'},
                                barmode='group')
                    fig2.update_layout(height=400)
                    st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No MoM trend data available for selected customers")
    else:
        st.info("No customer data available for MoM analysis")


def display_customer_health(dashboard):
    """Display customer health scores."""
    st.subheader("🏥 Customer Health Dashboard")
    
    health_data = dashboard.query("""
        SELECT *
        FROM customer_health_scores
        WHERE status = 'Active'
        ORDER BY health_score DESC
        LIMIT 50
    """)
    
    if not health_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Health score distribution
            fig = px.histogram(health_data, x='HEALTH_SCORE',
                             title='Customer Health Score Distribution',
                             labels={'HEALTH_SCORE': 'Health Score'},
                             nbins=20,
                             color_discrete_sequence=['#2ecc71'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Health category breakdown
            health_category = health_data['HEALTH_CATEGORY'].value_counts().reset_index()
            health_category.columns = ['Category', 'Count']
            
            fig = px.pie(health_category, values='Count', names='Category',
                        title='Customer Health Categories',
                        color_discrete_sequence=px.colors.sequential.RdYlGn_r)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Health score vs Revenue scatter plot
        fig = px.scatter(health_data, x='HEALTH_SCORE', y='TOTAL_REVENUE',
                        color='TIER', size='TOTAL_CREDITS_USED',
                        hover_data=['NAME'],
                        title='Customer Health Score vs Revenue',
                        labels={'HEALTH_SCORE': 'Health Score', 'TOTAL_REVENUE': 'Total Revenue ($)'},
                        color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Display health data table
        st.markdown("### Top Customers by Health Score")
        display_cols = ['NAME', 'TIER', 'HEALTH_SCORE', 'HEALTH_CATEGORY', 'TOTAL_CREDITS_USED', 'TOTAL_REVENUE']
        display_cols = [col for col in display_cols if col in health_data.columns]
        st.dataframe(health_data[display_cols].head(20), use_container_width=True)
    else:
        st.info("No customer health data available")


def main():
    """Main dashboard function."""
    global dashboard
    dashboard = get_dashboard()
    
    # Display header
    display_header()
    
    # Sidebar filters
    with st.sidebar:
        st.header("⚙️ Filters")
        
        # Date range filter
        date_range = st.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=365), datetime.now()),
            max_value=datetime.now()
        )
        
        # Tier filter
        tiers = st.multiselect(
            "Customer Tier",
            options=["Free", "Basic", "Pro", "Enterprise"],
            default=["Free", "Basic", "Pro", "Enterprise"]
        )
        
        # Status filter
        status = st.multiselect(
            "Customer Status",
            options=["Active", "Churned"],
            default=["Active"]
        )
        
        st.markdown("---")
        st.markdown("### 📚 About")
        st.markdown("""
        This dashboard provides real-time insights into:
        - Customer health scores
        - Revenue trends
        - Credit usage patterns
        - MoM consumption trends
        
        **Data Source:** Snowflake
        """)
    
    # Main content
    display_kpi_metrics(dashboard)
    
    st.markdown("---")
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["💰 Revenue", "🔥 Usage", "📊 MoM Trends", "🏥 Customer Health"])
    
    with tab1:
        display_revenue_analysis(dashboard)
    
    with tab2:
        display_usage_analysis(dashboard)
    
    with tab3:
        display_mom_trends(dashboard)
    
    with tab4:
        display_customer_health(dashboard)
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "SaaS Analytics Dashboard | Powered by Snowflake & Streamlit"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
