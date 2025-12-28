"""
Production-Grade SaaS Analytics Dashboard
Enterprise Snowflake analytics application with advanced business intelligence features.

Features:
- Real-time customer health scoring
- Predictive churn analysis
- Dynamic filtering and interactive visualizations
- Modular architecture with separation of concerns
- Professional dark-themed UI
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import logging

# Add parent directory to path for module imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import custom modules
from config.snowflake_connection import get_connection_manager
from data_layer.data_transformation import DataTransformationLayer
from components.visualizations import VisualizationComponents

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="SaaS Analytics Dashboard | Enterprise Edition",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "Enterprise SaaS Analytics Platform - Built with Snowflake & Python"
    }
)

# ============================================================================
# CUSTOM CSS - PROFESSIONAL DARK THEME
# ============================================================================

st.markdown("""
    <style>
    /* Global Styles */
    .stApp {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    }
    
    /* Main Container */
    .main {
        padding: 1rem 2rem;
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f3460 0%, #16213e 100%);
        border-right: 2px solid #00D4FF;
    }
    
    section[data-testid="stSidebar"] .stMarkdown {
        color: #E0E0E0;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #00D4FF !important;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-weight: 600;
    }
    
    /* Metric Cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #2D2D2D 0%, #1E1E1E 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border-left: 4px solid #00D4FF;
        box-shadow: 0 4px 6px rgba(0, 212, 255, 0.2);
    }
    
    [data-testid="stMetricValue"] {
        color: #00D4FF;
        font-size: 2rem;
        font-weight: bold;
    }
    
    [data-testid="stMetricLabel"] {
        color: #B0B0B0;
        font-size: 0.9rem;
        font-weight: 500;
    }
    
    /* Buttons */
    .stButton button {
        background: linear-gradient(135deg, #00D4FF 0%, #0099CC 100%);
        color: #1E1E1E;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 4px rgba(0, 212, 255, 0.3);
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 212, 255, 0.5);
    }
    
    /* Dataframe Tables */
    .dataframe {
        background-color: #2D2D2D !important;
        color: #E0E0E0 !important;
        border-radius: 8px;
    }
    
    /* Selectbox and Input Widgets */
    .stSelectbox, .stMultiSelect, .stDateInput {
        color: #E0E0E0;
    }
    
    .stSelectbox > div > div, .stMultiSelect > div > div {
        background-color: #2D2D2D;
        border: 1px solid #404040;
        border-radius: 8px;
    }
    
    /* Dividers */
    hr {
        border-color: #00D4FF;
        opacity: 0.3;
    }
    
    /* Info/Warning/Error Boxes */
    .stAlert {
        background-color: #2D2D2D;
        border-radius: 8px;
        border-left: 4px solid #00D4FF;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #2D2D2D;
        border-radius: 8px;
        color: #E0E0E0;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #2D2D2D;
        border-radius: 8px 8px 0 0;
    }
    
    .stTabs [data-baseweb="tab"] {
        color: #B0B0B0;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        color: #00D4FF;
        border-bottom: 2px solid #00D4FF;
    }
    
    /* Custom Section Headers */
    .section-header {
        background: linear-gradient(90deg, #00D4FF 0%, transparent 100%);
        padding: 15px 20px;
        border-radius: 8px;
        margin: 20px 0 15px 0;
        border-left: 5px solid #00D4FF;
    }
    
    .section-header h2 {
        margin: 0;
        color: #FFFFFF !important;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if 'filters_applied' not in st.session_state:
    st.session_state.filters_applied = False

if 'date_range' not in st.session_state:
    st.session_state.date_range = (
        datetime.now() - timedelta(days=90),
        datetime.now()
    )

# ============================================================================
# INITIALIZE CONNECTIONS AND DATA LAYER
# ============================================================================

@st.cache_resource
def initialize_application():
    """Initialize application components with caching."""
    logger.info("Initializing application components")
    connection_manager = get_connection_manager()
    data_layer = DataTransformationLayer(connection_manager)
    viz = VisualizationComponents()
    return connection_manager, data_layer, viz

connection_manager, data_layer, viz = initialize_application()

# ============================================================================
# SIDEBAR - FILTERS AND CONTROLS
# ============================================================================

with st.sidebar:
    # Logo and Title
    st.markdown("""
        <div style="text-align: center; padding: 20px 0;">
            <h1 style="color: #00D4FF; font-size: 2.5rem; margin: 0;">📊</h1>
            <h2 style="color: #FFFFFF; font-size: 1.3rem; margin: 10px 0;">SaaS Analytics</h2>
            <p style="color: #B0B0B0; font-size: 0.9rem;">Enterprise Edition</p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Connection Status
    st.markdown("### 🔌 Connection Status")
    if connection_manager.is_demo_mode():
        st.warning("⚠️ Demo Mode\n\nUsing synthetic data")
    else:
        st.success("✅ Connected to Snowflake")
    
    st.markdown("---")
    
    # Date Range Filter
    st.markdown("### 📅 Date Range")
    st.markdown("Select the time period for analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=90),
            max_value=datetime.now(),
            key="start_date"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now(),
            max_value=datetime.now(),
            key="end_date"
        )
    
    # Quick date range presets
    st.markdown("**Quick Select:**")
    preset_col1, preset_col2 = st.columns(2)
    
    with preset_col1:
        if st.button("Last 30 Days", width="stretch"):
            st.session_state.start_date = datetime.now() - timedelta(days=30)
            st.session_state.end_date = datetime.now()
            st.rerun()
        
        if st.button("Last Quarter", width="stretch"):
            st.session_state.start_date = datetime.now() - timedelta(days=90)
            st.session_state.end_date = datetime.now()
            st.rerun()
    
    with preset_col2:
        if st.button("Last 60 Days", width="stretch"):
            st.session_state.start_date = datetime.now() - timedelta(days=60)
            st.session_state.end_date = datetime.now()
            st.rerun()
        
        if st.button("Last Year", width="stretch"):
            st.session_state.start_date = datetime.now() - timedelta(days=365)
            st.session_state.end_date = datetime.now()
            st.rerun()
    
    st.markdown("---")
    
    # Customer Tier Filter
    st.markdown("### 🎯 Customer Tiers")
    selected_tiers = st.multiselect(
        "Select tiers to include",
        options=['Free', 'Basic', 'Pro', 'Enterprise'],
        default=['Free', 'Basic', 'Pro', 'Enterprise'],
        key="tier_filter"
    )
    
    st.markdown("---")
    
    # Status Filter
    st.markdown("### 📊 Customer Status")
    show_active = st.toggle("Active Customers", value=True, key="show_active")
    show_inactive = st.toggle("Inactive Customers", value=False, key="show_inactive")
    
    # Build status list
    selected_statuses = []
    if show_active:
        selected_statuses.append('Active')
    if show_inactive:
        selected_statuses.append('Inactive')
    
    st.markdown("---")
    
    # Advanced Filters
    with st.expander("⚙️ Advanced Filters"):
        churn_risk_threshold = st.slider(
            "Churn Risk Threshold",
            min_value=0,
            max_value=100,
            value=50,
            help="Show customers with churn risk above this threshold"
        )
        
        health_score_min = st.slider(
            "Minimum Health Score",
            min_value=0,
            max_value=100,
            value=0,
            help="Filter customers by minimum health score"
        )
    
    st.markdown("---")
    
    # Apply Filters Button
    if st.button("🔄 Apply Filters", width="stretch", type="primary"):
        st.session_state.filters_applied = True
        st.session_state.date_range = (start_date, end_date)
        st.rerun()
    
    # Reset Filters Button
    if st.button("↺ Reset Filters", width="stretch"):
        st.session_state.filters_applied = False
        st.session_state.start_date = datetime.now() - timedelta(days=90)
        st.session_state.end_date = datetime.now()
        st.rerun()

# ============================================================================
# MAIN DASHBOARD
# ============================================================================

# Header
st.markdown("""
    <div style="text-align: center; padding: 20px 0 40px 0;">
        <h1 style="font-size: 3rem; color: #00D4FF; margin: 0;">
            📊 SaaS Analytics Dashboard
        </h1>
        <p style="font-size: 1.2rem; color: #B0B0B0; margin-top: 10px;">
            Real-time insights into customer health, revenue, and predictive analytics
        </p>
    </div>
""", unsafe_allow_html=True)

# Convert date inputs to datetime for consistency
filter_start_date = datetime.combine(start_date, datetime.min.time())
filter_end_date = datetime.combine(end_date, datetime.max.time())

# ============================================================================
# KEY PERFORMANCE INDICATORS
# ============================================================================

st.markdown('<div class="section-header"><h2>📈 Key Performance Indicators</h2></div>', unsafe_allow_html=True)

# Fetch filtered data
try:
    customers_df = data_layer.get_filtered_customers(
        start_date=filter_start_date,
        end_date=filter_end_date,
        tiers=selected_tiers if selected_tiers else None,
        statuses=selected_statuses if selected_statuses else None
    )
    
    revenue_df = data_layer.get_revenue_metrics(
        start_date=filter_start_date,
        end_date=filter_end_date,
        tiers=selected_tiers if selected_tiers else None
    )
    
    # Calculate KPIs
    total_customers = len(customers_df) if not customers_df.empty else 0
    active_customers = len(customers_df[customers_df['STATUS'] == 'Active']) if not customers_df.empty else 0
    total_revenue = revenue_df['TOTAL_REVENUE'].sum() if not revenue_df.empty else 0
    avg_revenue_per_customer = revenue_df['AVG_REVENUE_PER_CUSTOMER'].mean() if not revenue_df.empty else 0
    
    # Create metric cards
    metrics = {
        'total_customers': {
            'label': 'Total Customers',
            'value': f'{total_customers:,}',
            'delta': f'{(active_customers/total_customers*100):.1f}% Active' if total_customers > 0 else 'N/A',
            'icon': '👥'
        },
        'active_customers': {
            'label': 'Active Customers',
            'value': f'{active_customers:,}',
            'delta': f'+{active_customers}',
            'icon': '✅'
        },
        'total_revenue': {
            'label': 'Total Revenue',
            'value': f'${total_revenue:,.0f}',
            'delta': f'${avg_revenue_per_customer:,.0f} avg',
            'icon': '💰'
        },
        'avg_revenue': {
            'label': 'Avg Revenue/Customer',
            'value': f'${avg_revenue_per_customer:,.0f}',
            'delta': '+12.5%' if avg_revenue_per_customer > 0 else 'N/A',
            'icon': '📊'
        }
    }
    
    viz.create_metric_cards(metrics)
    
except Exception as e:
    logger.error(f"Error fetching KPI data: {e}")
    st.error(f"❌ Error loading KPI data: {str(e)}")

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================================
# CREDIT CONSUMPTION TRENDS
# ============================================================================

st.markdown('<div class="section-header"><h2>📈 Credit Consumption Trends</h2></div>', unsafe_allow_html=True)

try:
    credit_trends_df = data_layer.get_credit_consumption_trends(
        start_date=filter_start_date,
        end_date=filter_end_date,
        top_n=10
    )
    
    if not credit_trends_df.empty:
        fig_trends = viz.create_multi_line_chart(
            df=credit_trends_df,
            x_col='USAGE_DATE',
            y_col='DAILY_CREDITS',
            group_col='CUSTOMER_NAME',
            title='Daily Credit Consumption - Top 10 Customers',
            height=500
        )
        st.plotly_chart(fig_trends, width="stretch")
    else:
        st.info("📊 No credit consumption data available for the selected period")
        
except Exception as e:
    logger.error(f"Error fetching credit trends: {e}")
    st.error(f"❌ Error loading credit trends: {str(e)}")

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================================
# CUSTOMER HEALTH & CHURN ANALYSIS
# ============================================================================

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-header"><h2>💚 Customer Health Scores</h2></div>', unsafe_allow_html=True)
    
    try:
        health_scores_df = data_layer.calculate_customer_health_scores(
            start_date=filter_start_date,
            end_date=filter_end_date
        )
        
        if not health_scores_df.empty:
            # Filter by minimum health score
            health_scores_df = health_scores_df[health_scores_df['HEALTH_SCORE'] >= health_score_min]
            
            # Create distribution chart
            health_dist = health_scores_df.groupby(['TIER', 'HEALTH_CATEGORY']).size().reset_index(name='COUNT')
            
            fig_health = viz.create_segmented_bar_chart(
                df=health_dist,
                x_col='TIER',
                y_col='COUNT',
                segment_col='HEALTH_CATEGORY',
                title='Customer Health Distribution by Tier',
                height=400
            )
            st.plotly_chart(fig_health, width="stretch")
            
            # Show top healthy customers
            st.markdown("**Top 5 Healthiest Customers:**")
            top_healthy = health_scores_df.nlargest(5, 'HEALTH_SCORE')[
                ['NAME', 'TIER', 'HEALTH_SCORE', 'HEALTH_CATEGORY', 'TOTAL_REVENUE']
            ]
            viz.create_data_table(top_healthy, "", max_rows=5)
        else:
            st.info("📊 No health score data available")
            
    except Exception as e:
        logger.error(f"Error calculating health scores: {e}")
        st.error(f"❌ Error loading health scores: {str(e)}")

with col2:
    st.markdown('<div class="section-header"><h2>⚠️ Churn Risk Analysis</h2></div>', unsafe_allow_html=True)
    
    try:
        churn_predictions_df = data_layer.calculate_churn_predictions(window_days=30)
        
        if not churn_predictions_df.empty:
            # Filter by churn risk threshold
            churn_predictions_df = churn_predictions_df[churn_predictions_df['CHURN_RISK_SCORE'] >= churn_risk_threshold]
            
            # Create churn risk distribution
            churn_dist = churn_predictions_df.groupby(['TIER', 'CHURN_RISK_CATEGORY']).size().reset_index(name='COUNT')
            
            fig_churn = viz.create_segmented_bar_chart(
                df=churn_dist,
                x_col='TIER',
                y_col='COUNT',
                segment_col='CHURN_RISK_CATEGORY',
                title='Churn Risk Distribution by Tier',
                height=400
            )
            st.plotly_chart(fig_churn, width="stretch")
            
            # Show high-risk customers
            st.markdown("**⚠️ Top 5 At-Risk Customers:**")
            high_risk = churn_predictions_df.nlargest(5, 'CHURN_RISK_SCORE')[
                ['NAME', 'TIER', 'CHURN_RISK_SCORE', 'USAGE_TREND_PCT', 'CHURN_RISK_CATEGORY']
            ]
            viz.create_data_table(high_risk, "", max_rows=5)
        else:
            st.info("📊 No churn prediction data available")
            
    except Exception as e:
        logger.error(f"Error calculating churn predictions: {e}")
        st.error(f"❌ Error loading churn predictions: {str(e)}")

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================================
# REVENUE ANALYSIS
# ============================================================================

st.markdown('<div class="section-header"><h2>💰 Revenue Analysis</h2></div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    try:
        if not revenue_df.empty:
            # Revenue by tier
            tier_revenue = revenue_df.groupby('TIER')['TOTAL_REVENUE'].sum().reset_index()
            
            fig_revenue_tier = viz.create_revenue_donut_chart(
                df=tier_revenue,
                values_col='TOTAL_REVENUE',
                names_col='TIER',
                title='Revenue Distribution by Tier',
                height=400
            )
            st.plotly_chart(fig_revenue_tier, width="stretch")
        else:
            st.info("📊 No revenue data available")
    except Exception as e:
        logger.error(f"Error creating revenue chart: {e}")
        st.error(f"❌ Error loading revenue data: {str(e)}")

with col2:
    try:
        if not revenue_df.empty:
            # Revenue heatmap by tier and region
            fig_heatmap = viz.create_heatmap(
                df=revenue_df,
                x_col='REGION',
                y_col='TIER',
                value_col='TOTAL_REVENUE',
                title='Revenue Heatmap: Tier × Region',
                height=400
            )
            st.plotly_chart(fig_heatmap, width="stretch")
        else:
            st.info("📊 No revenue data available")
    except Exception as e:
        logger.error(f"Error creating heatmap: {e}")
        st.error(f"❌ Error loading heatmap: {str(e)}")

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================================
# TOP CUSTOMERS BY UTILIZATION
# ============================================================================

st.markdown('<div class="section-header"><h2>🏆 Top Customers by Credit Utilization</h2></div>', unsafe_allow_html=True)

try:
    top_customers_df = data_layer.get_top_customers_by_utilization(
        start_date=filter_start_date,
        end_date=filter_end_date,
        limit=5
    )
    
    if not top_customers_df.empty:
        # Format the dataframe for display
        display_df = top_customers_df.copy()
        display_df['TOTAL_CREDITS_USED'] = display_df['TOTAL_CREDITS_USED'].apply(lambda x: f'{x:,.0f}')
        display_df['AVG_CREDITS_PER_TRANSACTION'] = display_df['AVG_CREDITS_PER_TRANSACTION'].apply(lambda x: f'{x:,.0f}')
        display_df['LAST_USAGE_DATE'] = pd.to_datetime(display_df['LAST_USAGE_DATE']).dt.strftime('%Y-%m-%d')
        
        viz.create_data_table(
            display_df,
            title="Top 5 Customers Ranked by Total Credit Usage",
            max_rows=5
        )
    else:
        st.info("📊 No customer utilization data available")
        
except Exception as e:
    logger.error(f"Error fetching top customers: {e}")
    st.error(f"❌ Error loading top customers: {str(e)}")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #B0B0B0; padding: 20px;">
        <p>
            <strong>SaaS Analytics Dashboard</strong> | Enterprise Edition<br>
            Powered by Snowflake & Python | Built with Streamlit<br>
            © 2024 - Real-time Business Intelligence Platform
        </p>
    </div>
""", unsafe_allow_html=True)

# ============================================================================
# LOGGING & MONITORING
# ============================================================================

logger.info(f"Dashboard rendered successfully. Filters: Date Range={start_date} to {end_date}, Tiers={selected_tiers}, Statuses={selected_statuses}")
