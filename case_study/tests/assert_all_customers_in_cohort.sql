/* This checks if all customers are in cohorts*/

WITH cohort AS (
  SELECT
    SUM(num_customers) AS cus_in_cohorts
  FROM {{ ref('fact_customers_cohort') }}
)

SELECT 
  COUNT(DISTINCT customerid) AS active_customers
FROM {{ ref('stg_orders') }}
CROSS JOIN cohort
HAVING active_customers != cus_in_cohorts