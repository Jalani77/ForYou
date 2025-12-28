"""
Data Transformation Layer
Handles all data retrieval, transformation, and business logic calculations.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class DataTransformationLayer:
    """
    Enterprise data transformation layer with advanced analytics and business logic.
    Handles data retrieval, filtering, and calculation of derived metrics.
    """
    
    def __init__(self, connection_manager):
        """
        Initialize data transformation layer.
        
        Args:
            connection_manager: SnowflakeConnectionManager instance
        """
        self.connection_manager = connection_manager
        self.demo_mode = connection_manager.is_demo_mode()
    
    def execute_query(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute SQL query with error handling and return DataFrame.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            pd.DataFrame: Query results
        """
        if self.demo_mode:
            return self._get_demo_data(sql)
        
        try:
            conn = self.connection_manager.get_connection()
            if not conn:
                logger.warning("No active connection, switching to demo data")
                return self._get_demo_data(sql)
            
            cursor = conn.cursor()
            cursor.execute(sql)
            
            # Get column names and data
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            logger.error(f"SQL: {sql}")
            return pd.DataFrame()
    
    def get_filtered_customers(self, 
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None,
                               tiers: Optional[list] = None,
                               statuses: Optional[list] = None) -> pd.DataFrame:
        """
        Get customers with dynamic filters applied.
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            tiers: List of customer tiers to include
            statuses: List of customer statuses to include
            
        Returns:
            pd.DataFrame: Filtered customer data
        """
        # Build WHERE clause dynamically
        where_clauses = []
        
        if start_date:
            where_clauses.append(f"created_date >= '{start_date.strftime('%Y-%m-%d')}'")
        
        if end_date:
            where_clauses.append(f"created_date <= '{end_date.strftime('%Y-%m-%d')}'")
        
        if tiers:
            tier_list = "', '".join(tiers)
            where_clauses.append(f"tier IN ('{tier_list}')")
        
        if statuses:
            status_list = "', '".join(statuses)
            where_clauses.append(f"status IN ('{status_list}')")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sql = f"""
        SELECT 
            customer_id,
            name,
            tier,
            status,
            region,
            created_date
        FROM customers
        WHERE {where_sql}
        ORDER BY created_date DESC
        """
        
        return self.execute_query(sql)
    
    def get_revenue_metrics(self,
                           start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None,
                           tiers: Optional[list] = None) -> pd.DataFrame:
        """
        Get revenue metrics with date and tier filtering.
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            tiers: List of customer tiers
            
        Returns:
            pd.DataFrame: Revenue metrics
        """
        where_clauses = []
        
        if start_date:
            where_clauses.append(f"b.billing_date >= '{start_date.strftime('%Y-%m-%d')}'")
        
        if end_date:
            where_clauses.append(f"b.billing_date <= '{end_date.strftime('%Y-%m-%d')}'")
        
        if tiers:
            tier_list = "', '".join(tiers)
            where_clauses.append(f"c.tier IN ('{tier_list}')")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sql = f"""
        SELECT 
            c.tier,
            c.region,
            SUM(b.amount) as total_revenue,
            COUNT(DISTINCT b.customer_id) as customer_count,
            AVG(b.amount) as avg_revenue_per_customer
        FROM billing b
        JOIN customers c ON b.customer_id = c.customer_id
        WHERE b.status = 'Paid' AND {where_sql}
        GROUP BY c.tier, c.region
        ORDER BY total_revenue DESC
        """
        
        return self.execute_query(sql)
    
    def get_credit_consumption_trends(self,
                                      start_date: Optional[datetime] = None,
                                      end_date: Optional[datetime] = None,
                                      top_n: int = 10) -> pd.DataFrame:
        """
        Get credit consumption trends over time for top customers.
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            top_n: Number of top customers to include
            
        Returns:
            pd.DataFrame: Credit consumption trends
        """
        where_clauses = []
        
        if start_date:
            where_clauses.append(f"cu.usage_date >= '{start_date.strftime('%Y-%m-%d')}'")
        
        if end_date:
            where_clauses.append(f"cu.usage_date <= '{end_date.strftime('%Y-%m-%d')}'")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sql = f"""
        WITH top_customers AS (
            SELECT customer_id, SUM(credits_used) as total_credits
            FROM credit_usage
            WHERE {where_sql}
            GROUP BY customer_id
            ORDER BY total_credits DESC
            LIMIT {top_n}
        )
        SELECT 
            DATE_TRUNC('day', cu.usage_date) as usage_date,
            c.name as customer_name,
            c.tier,
            SUM(cu.credits_used) as daily_credits
        FROM credit_usage cu
        JOIN customers c ON cu.customer_id = c.customer_id
        WHERE cu.customer_id IN (SELECT customer_id FROM top_customers)
          AND {where_sql}
        GROUP BY DATE_TRUNC('day', cu.usage_date), c.name, c.tier
        ORDER BY usage_date, customer_name
        """
        
        return self.execute_query(sql)
    
    def calculate_customer_health_scores(self,
                                         start_date: Optional[datetime] = None,
                                         end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Calculate weighted customer health scores based on multiple factors.
        
        Health Score Components:
        - Revenue contribution (30%)
        - Credit usage trend (30%)
        - Engagement frequency (20%)
        - Payment reliability (20%)
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            
        Returns:
            pd.DataFrame: Customer health scores with risk categories
        """
        where_clauses = []
        
        if start_date:
            where_clauses.append(f"cu.usage_date >= '{start_date.strftime('%Y-%m-%d')}'")
            where_clauses.append(f"b.billing_date >= '{start_date.strftime('%Y-%m-%d')}'")
        
        if end_date:
            where_clauses.append(f"cu.usage_date <= '{end_date.strftime('%Y-%m-%d')}'")
            where_clauses.append(f"b.billing_date <= '{end_date.strftime('%Y-%m-%d')}'")
        
        credit_where = " AND ".join([w for w in where_clauses if 'cu.' in w]) if where_clauses else "1=1"
        billing_where = " AND ".join([w for w in where_clauses if 'b.' in w]) if where_clauses else "1=1"
        
        sql = f"""
        WITH customer_metrics AS (
            SELECT 
                c.customer_id,
                c.name,
                c.tier,
                c.status,
                -- Revenue score (0-100)
                CASE 
                    WHEN SUM(b.amount) >= 10000 THEN 100
                    WHEN SUM(b.amount) >= 5000 THEN 80
                    WHEN SUM(b.amount) >= 1000 THEN 60
                    ELSE 40
                END as revenue_score,
                -- Usage score (0-100) 
                CASE 
                    WHEN SUM(cu.credits_used) >= 50000 THEN 100
                    WHEN SUM(cu.credits_used) >= 25000 THEN 80
                    WHEN SUM(cu.credits_used) >= 10000 THEN 60
                    ELSE 40
                END as usage_score,
                -- Engagement score (0-100)
                CASE 
                    WHEN COUNT(DISTINCT DATE_TRUNC('day', cu.usage_date)) >= 25 THEN 100
                    WHEN COUNT(DISTINCT DATE_TRUNC('day', cu.usage_date)) >= 15 THEN 80
                    WHEN COUNT(DISTINCT DATE_TRUNC('day', cu.usage_date)) >= 5 THEN 60
                    ELSE 40
                END as engagement_score,
                -- Payment reliability score (0-100)
                CASE 
                    WHEN SUM(CASE WHEN b.status = 'Paid' THEN 1 ELSE 0 END)::FLOAT / 
                         NULLIF(COUNT(b.billing_id), 0) >= 0.95 THEN 100
                    WHEN SUM(CASE WHEN b.status = 'Paid' THEN 1 ELSE 0 END)::FLOAT / 
                         NULLIF(COUNT(b.billing_id), 0) >= 0.85 THEN 80
                    WHEN SUM(CASE WHEN b.status = 'Paid' THEN 1 ELSE 0 END)::FLOAT / 
                         NULLIF(COUNT(b.billing_id), 0) >= 0.70 THEN 60
                    ELSE 40
                END as payment_score,
                SUM(b.amount) as total_revenue,
                SUM(cu.credits_used) as total_credits_used
            FROM customers c
            LEFT JOIN billing b ON c.customer_id = b.customer_id AND {billing_where}
            LEFT JOIN credit_usage cu ON c.customer_id = cu.customer_id AND {credit_where}
            GROUP BY c.customer_id, c.name, c.tier, c.status
        )
        SELECT 
            customer_id,
            name,
            tier,
            status,
            total_revenue,
            total_credits_used,
            revenue_score,
            usage_score,
            engagement_score,
            payment_score,
            -- Weighted health score
            ROUND(
                (revenue_score * 0.30) + 
                (usage_score * 0.30) + 
                (engagement_score * 0.20) + 
                (payment_score * 0.20)
            ) as health_score,
            -- Risk category
            CASE 
                WHEN ROUND(
                    (revenue_score * 0.30) + 
                    (usage_score * 0.30) + 
                    (engagement_score * 0.20) + 
                    (payment_score * 0.20)
                ) >= 80 THEN 'Healthy'
                WHEN ROUND(
                    (revenue_score * 0.30) + 
                    (usage_score * 0.30) + 
                    (engagement_score * 0.20) + 
                    (payment_score * 0.20)
                ) >= 60 THEN 'At Risk'
                ELSE 'Critical'
            END as health_category
        FROM customer_metrics
        ORDER BY health_score DESC
        """
        
        return self.execute_query(sql)
    
    def calculate_churn_predictions(self,
                                   window_days: int = 30) -> pd.DataFrame:
        """
        Calculate predictive churn indicators based on declining credit usage.
        
        Churn Risk Factors:
        - 30-day rolling credit usage decline > 30% = High Risk
        - 30-day rolling credit usage decline 15-30% = Medium Risk
        - Stable or growing usage = Low Risk
        
        Args:
            window_days: Rolling window for trend analysis (default 30 days)
            
        Returns:
            pd.DataFrame: Churn predictions with risk scores
        """
        sql = f"""
        WITH daily_usage AS (
            SELECT 
                customer_id,
                DATE_TRUNC('day', usage_date) as usage_date,
                SUM(credits_used) as daily_credits
            FROM credit_usage
            WHERE usage_date >= DATEADD('day', -{window_days * 2}, CURRENT_DATE())
            GROUP BY customer_id, DATE_TRUNC('day', usage_date)
        ),
        windowed_usage AS (
            SELECT 
                customer_id,
                usage_date,
                daily_credits,
                AVG(daily_credits) OVER (
                    PARTITION BY customer_id 
                    ORDER BY usage_date 
                    ROWS BETWEEN {window_days} PRECEDING AND CURRENT ROW
                ) as rolling_avg_current,
                AVG(daily_credits) OVER (
                    PARTITION BY customer_id 
                    ORDER BY usage_date 
                    ROWS BETWEEN {window_days * 2} PRECEDING AND {window_days} PRECEDING
                ) as rolling_avg_previous
            FROM daily_usage
        ),
        trend_analysis AS (
            SELECT 
                customer_id,
                MAX(usage_date) as last_usage_date,
                MAX(rolling_avg_current) as current_avg_usage,
                MAX(rolling_avg_previous) as previous_avg_usage,
                CASE 
                    WHEN MAX(rolling_avg_previous) > 0 THEN
                        ((MAX(rolling_avg_current) - MAX(rolling_avg_previous)) / 
                         MAX(rolling_avg_previous)) * 100
                    ELSE 0
                END as usage_change_pct
            FROM windowed_usage
            GROUP BY customer_id
        )
        SELECT 
            c.customer_id,
            c.name,
            c.tier,
            c.status,
            t.last_usage_date,
            COALESCE(t.current_avg_usage, 0) as current_avg_daily_credits,
            COALESCE(t.previous_avg_usage, 0) as previous_avg_daily_credits,
            COALESCE(t.usage_change_pct, 0) as usage_trend_pct,
            -- Churn risk score (0-100, higher = more risk)
            CASE 
                WHEN COALESCE(t.usage_change_pct, 0) <= -30 THEN 85
                WHEN COALESCE(t.usage_change_pct, 0) <= -15 THEN 60
                WHEN COALESCE(t.usage_change_pct, 0) <= 0 THEN 35
                ELSE 15
            END as churn_risk_score,
            -- Churn risk category
            CASE 
                WHEN COALESCE(t.usage_change_pct, 0) <= -30 THEN 'High Risk'
                WHEN COALESCE(t.usage_change_pct, 0) <= -15 THEN 'Medium Risk'
                WHEN COALESCE(t.usage_change_pct, 0) <= 0 THEN 'Low Risk'
                ELSE 'Healthy'
            END as churn_risk_category,
            DATEDIFF('day', t.last_usage_date, CURRENT_DATE()) as days_since_last_activity
        FROM customers c
        LEFT JOIN trend_analysis t ON c.customer_id = t.customer_id
        WHERE c.status = 'Active'
        ORDER BY churn_risk_score DESC, usage_trend_pct ASC
        """
        
        return self.execute_query(sql)
    
    def get_top_customers_by_utilization(self,
                                         start_date: Optional[datetime] = None,
                                         end_date: Optional[datetime] = None,
                                         limit: int = 5) -> pd.DataFrame:
        """
        Get top N customers by credit utilization.
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            limit: Number of top customers to return
            
        Returns:
            pd.DataFrame: Top customers ranked by utilization
        """
        where_clauses = []
        
        if start_date:
            where_clauses.append(f"cu.usage_date >= '{start_date.strftime('%Y-%m-%d')}'")
        
        if end_date:
            where_clauses.append(f"cu.usage_date <= '{end_date.strftime('%Y-%m-%d')}'")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sql = f"""
        SELECT 
            c.customer_id,
            c.name,
            c.tier,
            c.region,
            SUM(cu.credits_used) as total_credits_used,
            COUNT(DISTINCT DATE_TRUNC('day', cu.usage_date)) as active_days,
            AVG(cu.credits_used) as avg_credits_per_transaction,
            MAX(cu.usage_date) as last_usage_date
        FROM customers c
        JOIN credit_usage cu ON c.customer_id = cu.customer_id
        WHERE {where_sql}
        GROUP BY c.customer_id, c.name, c.tier, c.region
        ORDER BY total_credits_used DESC
        LIMIT {limit}
        """
        
        return self.execute_query(sql)
    
    def _get_demo_data(self, sql: str) -> pd.DataFrame:
        """
        Generate realistic demo data for testing without Snowflake connection.
        
        Args:
            sql: SQL query (used to determine data type)
            
        Returns:
            pd.DataFrame: Simulated data
        """
        import random
        from datetime import timedelta
        
        # Determine data type from SQL query
        sql_lower = sql.lower()
        
        if 'customer_health' in sql_lower or 'health_score' in sql_lower:
            # Customer health scores
            np.random.seed(42)
            # Uppercase column names for consistency
            data = {
                'CUSTOMER_ID': [f'CUST_{i:04d}' for i in range(1, 51)],
                'NAME': [f'Customer {i}' for i in range(1, 51)],
                'TIER': np.random.choice(['Free', 'Basic', 'Pro', 'Enterprise'], 50, p=[0.3, 0.3, 0.25, 0.15]),
                'STATUS': np.random.choice(['Active', 'Inactive'], 50, p=[0.85, 0.15]),
                'TOTAL_REVENUE': np.random.uniform(500, 50000, 50),
                'TOTAL_CREDITS_USED': np.random.uniform(1000, 100000, 50),
                'REVENUE_SCORE': np.random.randint(40, 100, 50),
                'USAGE_SCORE': np.random.randint(40, 100, 50),
                'ENGAGEMENT_SCORE': np.random.randint(40, 100, 50),
                'PAYMENT_SCORE': np.random.randint(60, 100, 50),
            }
            df = pd.DataFrame(data)
            df['HEALTH_SCORE'] = (
                df['REVENUE_SCORE'] * 0.3 + 
                df['USAGE_SCORE'] * 0.3 + 
                df['ENGAGEMENT_SCORE'] * 0.2 + 
                df['PAYMENT_SCORE'] * 0.2
            ).round()
            df['HEALTH_CATEGORY'] = df['HEALTH_SCORE'].apply(
                lambda x: 'Healthy' if x >= 80 else ('At Risk' if x >= 60 else 'Critical')
            )
            return df
            
        elif 'churn' in sql_lower:
            # Churn predictions
            np.random.seed(43)
            data = {
                'CUSTOMER_ID': [f'CUST_{i:04d}' for i in range(1, 51)],
                'NAME': [f'Customer {i}' for i in range(1, 51)],
                'TIER': np.random.choice(['Free', 'Basic', 'Pro', 'Enterprise'], 50),
                'STATUS': ['Active'] * 50,
                'LAST_USAGE_DATE': [datetime.now() - timedelta(days=random.randint(1, 30)) for _ in range(50)],
                'CURRENT_AVG_DAILY_CREDITS': np.random.uniform(100, 5000, 50),
                'PREVIOUS_AVG_DAILY_CREDITS': np.random.uniform(150, 6000, 50),
                'CHURN_RISK_SCORE': np.random.randint(15, 90, 50),
                'DAYS_SINCE_LAST_ACTIVITY': np.random.randint(1, 30, 50),
            }
            df = pd.DataFrame(data)
            df['USAGE_TREND_PCT'] = (
                (df['CURRENT_AVG_DAILY_CREDITS'] - df['PREVIOUS_AVG_DAILY_CREDITS']) / 
                df['PREVIOUS_AVG_DAILY_CREDITS'] * 100
            )
            df['CHURN_RISK_CATEGORY'] = df['CHURN_RISK_SCORE'].apply(
                lambda x: 'High Risk' if x >= 70 else ('Medium Risk' if x >= 50 else ('Low Risk' if x >= 30 else 'Healthy'))
            )
            return df
            
        elif 'credit' in sql_lower and 'trend' in sql_lower:
            # Credit consumption trends
            np.random.seed(44)
            dates = pd.date_range(start=datetime.now() - timedelta(days=60), end=datetime.now(), freq='D')
            customers = [f'Customer {i}' for i in range(1, 11)]
            tiers = ['Free', 'Basic', 'Pro', 'Enterprise']
            
            data = []
            for date in dates:
                for customer in customers:
                    data.append({
                        'USAGE_DATE': date,
                        'CUSTOMER_NAME': customer,
                        'TIER': random.choice(tiers),
                        'DAILY_CREDITS': random.uniform(100, 2000)
                    })
            
            return pd.DataFrame(data)
            
        elif 'revenue' in sql_lower:
            # Revenue metrics
            np.random.seed(45)
            tiers = ['Free', 'Basic', 'Pro', 'Enterprise']
            regions = ['North America', 'Europe', 'Asia Pacific', 'Latin America']
            
            data = []
            for tier in tiers:
                for region in regions:
                    data.append({
                        'TIER': tier,
                        'REGION': region,
                        'TOTAL_REVENUE': random.uniform(5000, 150000),
                        'CUSTOMER_COUNT': random.randint(10, 100),
                        'AVG_REVENUE_PER_CUSTOMER': random.uniform(500, 5000)
                    })
            
            return pd.DataFrame(data)
            
        elif 'top' in sql_lower and 'utilization' in sql_lower:
            # Top customers by utilization
            np.random.seed(46)
            data = {
                'CUSTOMER_ID': [f'CUST_{i:04d}' for i in range(1, 6)],
                'NAME': [f'Customer {i}' for i in range(1, 6)],
                'TIER': ['Enterprise', 'Pro', 'Enterprise', 'Pro', 'Basic'],
                'REGION': np.random.choice(['North America', 'Europe', 'Asia Pacific'], 5),
                'TOTAL_CREDITS_USED': [125000, 98000, 87000, 76000, 65000],
                'ACTIVE_DAYS': [58, 52, 49, 45, 42],
                'AVG_CREDITS_PER_TRANSACTION': [2155, 1885, 1776, 1689, 1548],
                'LAST_USAGE_DATE': [datetime.now() - timedelta(days=i) for i in range(5)]
            }
            return pd.DataFrame(data)
        
        else:
            # Default empty dataframe
            return pd.DataFrame({'MESSAGE': ['Demo data not available for this query type']})
