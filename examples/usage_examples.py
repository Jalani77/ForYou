"""
Example usage of the Snowflake SaaS Analytics Pipeline

This script demonstrates how to:
1. Query customer data
2. Analyze customer health scores
3. Track MoM trends
4. Generate reports

Note: This requires Snowflake credentials to be set in environment variables.
"""
import os
import sys
from pathlib import Path

try:
    import pandas as pd
    import snowflake.connector
    from snowflake.snowpark import Session
except ImportError:
    print("Required packages not installed. Run: pip install -r requirements.txt")
    sys.exit(1)


def create_connection():
    """Create and return Snowflake connection."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "SAAS_ANALYTICS"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None


def query_to_dataframe(conn, query):
    """Execute query and return results as pandas DataFrame."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()


def example_customer_health_analysis(conn):
    """Example: Analyze customer health scores."""
    print("\n" + "="*70)
    print("EXAMPLE 1: Customer Health Analysis")
    print("="*70)
    
    query = """
    SELECT 
        customer_id,
        name,
        tier,
        health_score,
        health_category,
        total_credits_used,
        total_revenue
    FROM customer_health_scores
    WHERE status = 'Active'
    ORDER BY health_score DESC
    LIMIT 10
    """
    
    df = query_to_dataframe(conn, query)
    
    if not df.empty:
        print("\nTop 10 Healthiest Active Customers:")
        print(df.to_string(index=False))
        
        # Summary statistics
        print("\n" + "-"*70)
        print("Summary Statistics:")
        print(f"Average Health Score: {df['HEALTH_SCORE'].mean():.2f}")
        print(f"Average Revenue: ${df['TOTAL_REVENUE'].mean():.2f}")
        print(f"Average Credits Used: {df['TOTAL_CREDITS_USED'].mean():.2f}")
    else:
        print("No data available")


def example_at_risk_customers(conn):
    """Example: Identify at-risk customers."""
    print("\n" + "="*70)
    print("EXAMPLE 2: At-Risk Customers")
    print("="*70)
    
    query = """
    SELECT 
        customer_id,
        name,
        tier,
        health_score,
        health_category,
        days_since_last_usage,
        total_revenue
    FROM customer_health_scores
    WHERE health_category IN ('At Risk', 'Needs Attention', 'Critical')
        AND status = 'Active'
    ORDER BY health_score ASC
    LIMIT 10
    """
    
    df = query_to_dataframe(conn, query)
    
    if not df.empty:
        print("\nTop 10 At-Risk Customers (Requiring Attention):")
        print(df.to_string(index=False))
        
        # Count by category
        print("\n" + "-"*70)
        print("At-Risk Customer Breakdown:")
        category_counts = df['HEALTH_CATEGORY'].value_counts()
        for category, count in category_counts.items():
            print(f"  {category}: {count}")
    else:
        print("No at-risk customers found")


def example_mom_trends(conn):
    """Example: Analyze month-over-month trends."""
    print("\n" + "="*70)
    print("EXAMPLE 3: Month-over-Month Credit Trends")
    print("="*70)
    
    query = """
    SELECT 
        customer_id,
        name,
        usage_month,
        monthly_credits,
        mom_change_pct,
        trend
    FROM mom_credit_trends
    WHERE usage_month >= DATEADD(month, -6, CURRENT_DATE())
        AND mom_change_pct IS NOT NULL
    ORDER BY ABS(mom_change_pct) DESC
    LIMIT 15
    """
    
    df = query_to_dataframe(conn, query)
    
    if not df.empty:
        print("\nTop 15 Customers with Largest MoM Changes (Last 6 Months):")
        print(df.to_string(index=False))
        
        # Trend analysis
        print("\n" + "-"*70)
        print("Trend Distribution:")
        trend_counts = df['TREND'].value_counts()
        for trend, count in trend_counts.items():
            print(f"  {trend}: {count}")
    else:
        print("No trend data available")


def example_revenue_by_tier(conn):
    """Example: Analyze revenue by customer tier."""
    print("\n" + "="*70)
    print("EXAMPLE 4: Revenue Analysis by Tier")
    print("="*70)
    
    query = """
    SELECT 
        tier,
        SUM(customer_count) as total_customers,
        SUM(active_customers) as active_customers,
        SUM(total_revenue) as total_revenue,
        AVG(revenue_per_customer) as avg_revenue_per_customer
    FROM revenue_by_tier_region
    GROUP BY tier
    ORDER BY total_revenue DESC
    """
    
    df = query_to_dataframe(conn, query)
    
    if not df.empty:
        print("\nRevenue by Customer Tier:")
        print(df.to_string(index=False))
        
        # Calculate percentages
        total_revenue = df['TOTAL_REVENUE'].sum()
        print("\n" + "-"*70)
        print("Revenue Distribution:")
        for _, row in df.iterrows():
            pct = (row['TOTAL_REVENUE'] / total_revenue * 100)
            print(f"  {row['TIER']}: ${row['TOTAL_REVENUE']:.2f} ({pct:.1f}%)")
    else:
        print("No revenue data available")


def example_churn_prediction(conn):
    """Example: Identify customers at risk of churning."""
    print("\n" + "="*70)
    print("EXAMPLE 5: Churn Prediction")
    print("="*70)
    
    query = """
    SELECT 
        customer_id,
        name,
        tier,
        days_since_last_activity,
        total_active_days,
        total_credits_used,
        churn_risk_score,
        CASE 
            WHEN inactive_30_days = 1 THEN '✗ Inactive 30+ days'
            ELSE '✓ Active recently'
        END as activity_status,
        CASE 
            WHEN low_engagement = 1 THEN '✗ Low engagement'
            ELSE '✓ Good engagement'
        END as engagement_status,
        CASE 
            WHEN low_usage = 1 THEN '✗ Low usage'
            ELSE '✓ Good usage'
        END as usage_status
    FROM churn_indicators
    ORDER BY churn_risk_score DESC, days_since_last_activity DESC
    LIMIT 10
    """
    
    df = query_to_dataframe(conn, query)
    
    if not df.empty:
        print("\nTop 10 Customers at Highest Risk of Churn:")
        print(df.to_string(index=False))
        
        # Risk distribution
        print("\n" + "-"*70)
        print("Churn Risk Distribution:")
        risk_counts = df['CHURN_RISK_SCORE'].value_counts().sort_index(ascending=False)
        for score, count in risk_counts.items():
            print(f"  Risk Score {score}: {count} customers")
    else:
        print("No churn risk indicators found")


def example_feature_usage(conn):
    """Example: Analyze feature usage patterns."""
    print("\n" + "="*70)
    print("EXAMPLE 6: Feature Usage Analysis")
    print("="*70)
    
    query = """
    SELECT 
        feature,
        SUM(usage_count) as total_usage,
        SUM(unique_customers) as total_customers,
        SUM(total_credits) as total_credits,
        AVG(avg_credits_per_use) as avg_credits_per_use
    FROM feature_usage_analysis
    GROUP BY feature
    ORDER BY total_credits DESC
    """
    
    df = query_to_dataframe(conn, query)
    
    if not df.empty:
        print("\nFeature Usage Summary:")
        print(df.to_string(index=False))
        
        # Most popular features
        print("\n" + "-"*70)
        print("Most Popular Features by Usage:")
        for _, row in df.head(3).iterrows():
            print(f"  {row['FEATURE']}: {row['TOTAL_USAGE']:,.0f} uses by {row['TOTAL_CUSTOMERS']:,.0f} customers")
    else:
        print("No feature usage data available")


def main():
    """Run all examples."""
    print("\n" + "="*70)
    print(" " * 15 + "SNOWFLAKE SAAS ANALYTICS EXAMPLES")
    print("="*70)
    
    # Check credentials
    required_env_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print("\n⚠ Error: Missing Snowflake credentials!")
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        print("\nPlease set the required environment variables and try again.")
        print("See README.md for setup instructions.")
        sys.exit(1)
    
    # Create connection
    print("\nConnecting to Snowflake...")
    conn = create_connection()
    
    if not conn:
        print("Failed to connect to Snowflake. Exiting.")
        sys.exit(1)
    
    print("✓ Connected successfully")
    
    try:
        # Run examples
        example_customer_health_analysis(conn)
        example_at_risk_customers(conn)
        example_mom_trends(conn)
        example_revenue_by_tier(conn)
        example_churn_prediction(conn)
        example_feature_usage(conn)
        
        print("\n" + "="*70)
        print(" " * 20 + "EXAMPLES COMPLETE")
        print("="*70)
        print("\nThese examples demonstrate common analytics queries.")
        print("See sql/analytics_queries.sql for more query examples.")
        print("Launch the dashboard with: streamlit run dashboard/app.py")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\n✓ Connection closed")


if __name__ == "__main__":
    main()
