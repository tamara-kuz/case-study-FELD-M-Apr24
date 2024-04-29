-- A dimension montly cohort of “customers” table with additional sales metrics:

{{ config(
    materialized='table',
) }}


-- STEP 1. Define a recursive CTE to generate a calendar of cohort months.
WITH RECURSIVE calendar_generator AS (
  -- Select the minimum order date as the initial cohort month.
  SELECT
    MIN(DATE(orderdate)) AS cohort_month
  FROM {{ ref('stg_orders') }}
  
  -- Recursively select the next month until reaching the maximum order date.
  UNION ALL
  SELECT
    DATE(cohort_month, '+1 month')
  FROM calendar_generator
  WHERE cohort_month < (
    SELECT MAX(DATE(orderdate)) FROM {{ ref('stg_orders') }}
  )
)

-- STEP 2. Define a CTE to format the cohort months.
, calendar AS (
  -- Select distinct cohort months formatted as YYYY-MM.
  SELECT DISTINCT strftime('%Y-%m', cohort_month) AS cohort_month
  FROM calendar_generator
)

-- STEP 3. Define a CTE to extract essential customer cohort information.
, customer_cohort_base AS (
  SELECT
    customer_id
    , country
    , strftime('%Y-%m', customer_since_date) AS cohort_month
    , total_order_value AS total_revenue
  FROM {{ ref('dim_customers') }}
)

-- STEP 4. Define a CTE to calculate metrics for each cohort.
, cohort_metrics AS (
  SELECT
    cc.cohort_month
    , country
    , COUNT(DISTINCT cc.customer_id) AS num_customers
    , SUM(cc.total_revenue) AS total_order_value
  FROM customer_cohort_base AS cc
  GROUP BY 1, 2
)

-- STEP 5. Define a CTE to join the calendar with cohort metrics.
, monthly_cohorts AS (
  SELECT
    cal.cohort_month
    , cm.country
    , cm.num_customers
    , cm.total_order_value
  FROM calendar cal
  LEFT JOIN cohort_metrics cm ON cal.cohort_month = cm.cohort_month
)

-- Select all columns from the monthly_cohorts CTE.
SELECT * 
FROM monthly_cohorts

