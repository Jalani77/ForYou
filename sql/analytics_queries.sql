-- ============================================================================
-- SNOWFLAKE SAAS ANALYTICS QUERIES
-- Customer Health Score and MoM Credit Consumption Trends
-- ============================================================================

-- Use the database and schema
USE DATABASE SAAS_ANALYTICS;
USE SCHEMA PUBLIC;

-- ============================================================================
-- 1. CUSTOMER HEALTH SCORE
-- ============================================================================
-- Comprehensive customer health score based on multiple factors:
-- - Credit usage trends
-- - Billing status
-- - Usage frequency
-- - Engagement score

CREATE OR REPLACE VIEW customer_health_scores AS
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.name,
        c.tier,
        c.status,
        c.industry,
        c.region,
        c.signup_date,
        DATEDIFF(day, c.signup_date, CURRENT_DATE()) as days_since_signup,
        
        -- Credit usage metrics
        COALESCE(SUM(cu.credits_used), 0) as total_credits_used,
        COALESCE(COUNT(DISTINCT cu.date), 0) as active_days,
        COALESCE(AVG(cu.credits_used), 0) as avg_daily_credits,
        COALESCE(MAX(cu.date), c.signup_date) as last_usage_date,
        DATEDIFF(day, COALESCE(MAX(cu.date), c.signup_date), CURRENT_DATE()) as days_since_last_usage,
        
        -- Billing metrics
        COALESCE(SUM(b.amount), 0) as total_revenue,
        COALESCE(COUNT(DISTINCT b.billing_id), 0) as billing_count,
        COALESCE(AVG(b.amount), 0) as avg_billing_amount
        
    FROM customers c
    LEFT JOIN credit_usage cu ON c.customer_id = cu.customer_id
    LEFT JOIN billing b ON c.customer_id = b.customer_id
    GROUP BY c.customer_id, c.name, c.tier, c.status, c.industry, c.region, c.signup_date
),

health_calculation AS (
    SELECT 
        *,
        
        -- Usage score (0-40 points)
        CASE 
            WHEN days_since_last_usage <= 7 THEN 40
            WHEN days_since_last_usage <= 14 THEN 30
            WHEN days_since_last_usage <= 30 THEN 20
            WHEN days_since_last_usage <= 60 THEN 10
            ELSE 0
        END as usage_recency_score,
        
        -- Frequency score (0-30 points)
        CASE 
            WHEN active_days > 100 THEN 30
            WHEN active_days > 50 THEN 25
            WHEN active_days > 20 THEN 20
            WHEN active_days > 10 THEN 15
            WHEN active_days > 5 THEN 10
            ELSE 5
        END as usage_frequency_score,
        
        -- Revenue score (0-30 points)
        CASE 
            WHEN total_revenue > 10000 THEN 30
            WHEN total_revenue > 5000 THEN 25
            WHEN total_revenue > 1000 THEN 20
            WHEN total_revenue > 500 THEN 15
            WHEN total_revenue > 100 THEN 10
            ELSE 5
        END as revenue_score
        
    FROM customer_metrics
)

SELECT 
    customer_id,
    name,
    tier,
    status,
    industry,
    region,
    total_credits_used,
    active_days,
    ROUND(avg_daily_credits, 2) as avg_daily_credits,
    days_since_last_usage,
    ROUND(total_revenue, 2) as total_revenue,
    
    -- Calculate health score (0-100)
    (usage_recency_score + usage_frequency_score + revenue_score) as health_score,
    
    -- Health category
    CASE 
        WHEN (usage_recency_score + usage_frequency_score + revenue_score) >= 80 THEN 'Healthy'
        WHEN (usage_recency_score + usage_frequency_score + revenue_score) >= 60 THEN 'At Risk'
        WHEN (usage_recency_score + usage_frequency_score + revenue_score) >= 40 THEN 'Needs Attention'
        ELSE 'Critical'
    END as health_category,
    
    -- Churn risk indicator
    CASE 
        WHEN status = 'Churned' THEN 'Already Churned'
        WHEN days_since_last_usage > 60 THEN 'High Risk'
        WHEN days_since_last_usage > 30 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as churn_risk
    
FROM health_calculation
ORDER BY health_score DESC;


-- ============================================================================
-- 2. MONTH-OVER-MONTH (MOM) CREDIT CONSUMPTION TRENDS
-- ============================================================================
-- Analyze credit usage trends month over month for each customer

CREATE OR REPLACE VIEW mom_credit_trends AS
WITH monthly_usage AS (
    SELECT 
        cu.customer_id,
        c.name,
        c.tier,
        DATE_TRUNC('MONTH', cu.date) as usage_month,
        SUM(cu.credits_used) as monthly_credits,
        COUNT(DISTINCT cu.date) as active_days_in_month,
        COUNT(cu.usage_id) as transaction_count
    FROM credit_usage cu
    JOIN customers c ON cu.customer_id = c.customer_id
    GROUP BY cu.customer_id, c.name, c.tier, DATE_TRUNC('MONTH', cu.date)
),

lagged_metrics AS (
    SELECT 
        customer_id,
        name,
        tier,
        usage_month,
        monthly_credits,
        active_days_in_month,
        transaction_count,
        LAG(monthly_credits) OVER (PARTITION BY customer_id ORDER BY usage_month) as prev_month_credits,
        LAG(usage_month) OVER (PARTITION BY customer_id ORDER BY usage_month) as prev_month
    FROM monthly_usage
)

SELECT 
    customer_id,
    name,
    tier,
    usage_month,
    ROUND(monthly_credits, 2) as monthly_credits,
    active_days_in_month,
    transaction_count,
    ROUND(COALESCE(prev_month_credits, 0), 2) as prev_month_credits,
    
    -- Month-over-month change
    ROUND(monthly_credits - COALESCE(prev_month_credits, monthly_credits), 2) as mom_change,
    
    -- Month-over-month percentage change
    CASE 
        WHEN prev_month_credits IS NULL OR prev_month_credits = 0 THEN NULL
        ELSE ROUND(((monthly_credits - prev_month_credits) / prev_month_credits) * 100, 2)
    END as mom_change_pct,
    
    -- Trend indicator
    CASE 
        WHEN prev_month_credits IS NULL THEN 'New'
        WHEN monthly_credits > prev_month_credits * 1.2 THEN 'Growing Fast'
        WHEN monthly_credits > prev_month_credits THEN 'Growing'
        WHEN monthly_credits < prev_month_credits * 0.8 THEN 'Declining Fast'
        WHEN monthly_credits < prev_month_credits THEN 'Declining'
        ELSE 'Stable'
    END as trend
    
FROM lagged_metrics
ORDER BY customer_id, usage_month DESC;


-- ============================================================================
-- 3. CUSTOMER SEGMENTATION BY CREDIT USAGE
-- ============================================================================
-- Segment customers based on their credit usage patterns

CREATE OR REPLACE VIEW customer_segments AS
WITH usage_percentiles AS (
    SELECT 
        customer_id,
        SUM(credits_used) as total_credits,
        NTILE(4) OVER (ORDER BY SUM(credits_used)) as usage_quartile
    FROM credit_usage
    GROUP BY customer_id
)

SELECT 
    c.customer_id,
    c.name,
    c.tier,
    c.status,
    ROUND(up.total_credits, 2) as total_credits,
    
    CASE up.usage_quartile
        WHEN 4 THEN 'Power User'
        WHEN 3 THEN 'High User'
        WHEN 2 THEN 'Medium User'
        ELSE 'Low User'
    END as usage_segment,
    
    up.usage_quartile
    
FROM customers c
LEFT JOIN usage_percentiles up ON c.customer_id = up.customer_id
ORDER BY up.total_credits DESC NULLS LAST;


-- ============================================================================
-- 4. REVENUE ANALYSIS BY TIER AND REGION
-- ============================================================================
-- Analyze revenue patterns across different customer tiers and regions

CREATE OR REPLACE VIEW revenue_by_tier_region AS
SELECT 
    c.tier,
    c.region,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(DISTINCT CASE WHEN c.status = 'Active' THEN c.customer_id END) as active_customers,
    ROUND(SUM(b.amount), 2) as total_revenue,
    ROUND(AVG(b.amount), 2) as avg_transaction_amount,
    COUNT(b.billing_id) as billing_transactions,
    ROUND(SUM(b.amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) as revenue_per_customer
FROM customers c
LEFT JOIN billing b ON c.customer_id = b.customer_id
GROUP BY c.tier, c.region
ORDER BY total_revenue DESC NULLS LAST;


-- ============================================================================
-- 5. FEATURE USAGE ANALYSIS
-- ============================================================================
-- Analyze which features are most used and by which customer tiers

CREATE OR REPLACE VIEW feature_usage_analysis AS
SELECT 
    cu.feature,
    c.tier,
    COUNT(cu.usage_id) as usage_count,
    COUNT(DISTINCT cu.customer_id) as unique_customers,
    ROUND(SUM(cu.credits_used), 2) as total_credits,
    ROUND(AVG(cu.credits_used), 2) as avg_credits_per_use,
    ROUND(AVG(cu.execution_time_ms), 2) as avg_execution_time_ms
FROM credit_usage cu
JOIN customers c ON cu.customer_id = c.customer_id
GROUP BY cu.feature, c.tier
ORDER BY total_credits DESC;


-- ============================================================================
-- 6. CHURN PREDICTION INDICATORS
-- ============================================================================
-- Identify customers showing signs of potential churn

CREATE OR REPLACE VIEW churn_indicators AS
SELECT 
    c.customer_id,
    c.name,
    c.tier,
    c.status,
    DATEDIFF(day, MAX(cu.date), CURRENT_DATE()) as days_since_last_activity,
    COUNT(DISTINCT cu.date) as total_active_days,
    ROUND(SUM(cu.credits_used), 2) as total_credits_used,
    
    -- Red flags
    CASE WHEN DATEDIFF(day, MAX(cu.date), CURRENT_DATE()) > 30 THEN 1 ELSE 0 END as inactive_30_days,
    CASE WHEN COUNT(DISTINCT cu.date) < 5 THEN 1 ELSE 0 END as low_engagement,
    CASE WHEN SUM(cu.credits_used) < 100 THEN 1 ELSE 0 END as low_usage,
    
    -- Overall churn risk score
    (CASE WHEN DATEDIFF(day, MAX(cu.date), CURRENT_DATE()) > 30 THEN 1 ELSE 0 END +
     CASE WHEN COUNT(DISTINCT cu.date) < 5 THEN 1 ELSE 0 END +
     CASE WHEN SUM(cu.credits_used) < 100 THEN 1 ELSE 0 END) as churn_risk_score
     
FROM customers c
LEFT JOIN credit_usage cu ON c.customer_id = cu.customer_id
WHERE c.status = 'Active'
GROUP BY c.customer_id, c.name, c.tier, c.status
HAVING churn_risk_score > 0
ORDER BY churn_risk_score DESC, days_since_last_activity DESC;


-- ============================================================================
-- 7. BILLING HEALTH METRICS
-- ============================================================================
-- Monitor billing status and payment health

CREATE OR REPLACE VIEW billing_health AS
SELECT 
    c.customer_id,
    c.name,
    c.tier,
    COUNT(b.billing_id) as total_invoices,
    COUNT(CASE WHEN b.status = 'Paid' THEN 1 END) as paid_invoices,
    COUNT(CASE WHEN b.status = 'Pending' THEN 1 END) as pending_invoices,
    COUNT(CASE WHEN b.status = 'Overdue' THEN 1 END) as overdue_invoices,
    ROUND(SUM(CASE WHEN b.status = 'Paid' THEN b.amount ELSE 0 END), 2) as total_paid,
    ROUND(SUM(CASE WHEN b.status IN ('Pending', 'Overdue') THEN b.amount ELSE 0 END), 2) as outstanding_amount,
    
    -- Payment health score
    CASE 
        WHEN COUNT(CASE WHEN b.status = 'Overdue' THEN 1 END) > 0 THEN 'Poor'
        WHEN COUNT(CASE WHEN b.status = 'Pending' THEN 1 END) > 2 THEN 'Fair'
        ELSE 'Good'
    END as payment_health
    
FROM customers c
LEFT JOIN billing b ON c.customer_id = b.customer_id
GROUP BY c.customer_id, c.name, c.tier
ORDER BY outstanding_amount DESC NULLS LAST;


-- ============================================================================
-- SAMPLE QUERIES FOR ANALYSIS
-- ============================================================================

-- View top 10 healthiest customers
-- SELECT * FROM customer_health_scores WHERE status = 'Active' LIMIT 10;

-- View customers with declining credit usage
-- SELECT * FROM mom_credit_trends WHERE trend IN ('Declining', 'Declining Fast') ORDER BY usage_month DESC LIMIT 20;

-- View high-risk churn customers
-- SELECT * FROM churn_indicators WHERE churn_risk_score >= 2 LIMIT 20;

-- View revenue by tier
-- SELECT tier, SUM(total_revenue) as revenue FROM revenue_by_tier_region GROUP BY tier ORDER BY revenue DESC;

-- View most popular features
-- SELECT feature, SUM(usage_count) as total_usage FROM feature_usage_analysis GROUP BY feature ORDER BY total_usage DESC;
