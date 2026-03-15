-- ECO_STREAM analytical models (run in Snowflake worksheet or via your CI/CD tool)
-- Assumes raw data loaded into ECO_STREAM.RAW.{CUSTOMERS,USAGE_LOGS,BILLING}

CREATE SCHEMA IF NOT EXISTS ECO_STREAM.ANALYTICS;

-- 1) Monthly usage rollup (credits + activity)
CREATE OR REPLACE VIEW ECO_STREAM.ANALYTICS.V_MONTHLY_USAGE AS
WITH daily AS (
  SELECT
    TO_DATE(USAGE_DATE)                               AS usage_date,
    ACCOUNT_ID                                        AS account_id,
    CREDITS_USED::FLOAT                               AS credits_used,
    ACTIVE_USERS::NUMBER                              AS active_users
  FROM ECO_STREAM.RAW.USAGE_LOGS
),
monthly AS (
  SELECT
    DATE_TRUNC('MONTH', usage_date)                   AS usage_month,
    account_id,
    SUM(credits_used)                                 AS credits_used_month,
    AVG(active_users)                                 AS avg_daily_active_users,
    MAX(active_users)                                 AS max_daily_active_users
  FROM daily
  GROUP BY 1, 2
)
SELECT * FROM monthly;

-- 2) Month-over-Month (MoM) credit growth per customer
CREATE OR REPLACE VIEW ECO_STREAM.ANALYTICS.V_MOM_CREDIT_GROWTH AS
WITH m AS (
  SELECT
    usage_month,
    account_id,
    credits_used_month,
    LAG(credits_used_month) OVER (PARTITION BY account_id ORDER BY usage_month) AS prior_month_credits
  FROM ECO_STREAM.ANALYTICS.V_MONTHLY_USAGE
)
SELECT
  usage_month,
  account_id,
  credits_used_month,
  prior_month_credits,
  CASE
    WHEN prior_month_credits IS NULL OR prior_month_credits = 0 THEN NULL
    ELSE (credits_used_month - prior_month_credits) / prior_month_credits
  END AS mom_credit_growth_pct
FROM m;

-- 3) Churn risk: any customer with >=20% decline in credits MoM (latest month)
CREATE OR REPLACE VIEW ECO_STREAM.ANALYTICS.V_CHURN_RISK_CUSTOMERS AS
WITH g AS (
  SELECT *
  FROM ECO_STREAM.ANALYTICS.V_MOM_CREDIT_GROWTH
),
latest AS (
  SELECT MAX(usage_month) AS latest_month
  FROM ECO_STREAM.ANALYTICS.V_MONTHLY_USAGE
)
SELECT
  g.usage_month,
  g.account_id,
  c.customer_name,
  c.plan_tier,
  c.industry,
  c.region,
  g.credits_used_month,
  g.prior_month_credits,
  g.mom_credit_growth_pct
FROM g
JOIN latest l
  ON g.usage_month = l.latest_month
LEFT JOIN ECO_STREAM.RAW.CUSTOMERS c
  ON g.account_id = c.account_id
WHERE g.mom_credit_growth_pct <= -0.20
ORDER BY g.mom_credit_growth_pct ASC;

-- Convenience query: overall MoM credits (business-level trend)
-- (Useful to validate macro growth/seasonality in the synthetic generator.)
WITH monthly_total AS (
  SELECT
    usage_month,
    SUM(credits_used_month) AS credits_used_month
  FROM ECO_STREAM.ANALYTICS.V_MONTHLY_USAGE
  GROUP BY 1
),
g AS (
  SELECT
    usage_month,
    credits_used_month,
    LAG(credits_used_month) OVER (ORDER BY usage_month) AS prior_month_credits
  FROM monthly_total
)
SELECT
  usage_month,
  credits_used_month,
  prior_month_credits,
  CASE
    WHEN prior_month_credits IS NULL OR prior_month_credits = 0 THEN NULL
    ELSE (credits_used_month - prior_month_credits) / prior_month_credits
  END AS mom_credit_growth_pct
FROM g
ORDER BY usage_month;

