-- A dimension “customers” table with additional sales metrics:

{{ config(
    materialized='table',
) }}

-- STEP 1. Define a CTE named customer_base to select essential customer information from the staging customers table.
WITH customer_base AS (
  SELECT
    customerID AS customer_id
    , companyname AS company_name
    , contactname AS contact_name
    , country
  FROM {{ ref('stg_customers') }}
)

-- STEP 2. Define a CTE named sales_data to aggregate sales information by customer.
, sales_data AS (
    SELECT
      customer_id
      , n_orders
      , customer_since_date
      , MAX(total_amount) AS max_order_value
      , SUM(total_amount) AS total_revenue
    FROM {{ ref('fact_sales') }}
    GROUP BY 1, 2, 3
)

-- STEP 3. Define a CTE named rank_sales_data to rank customers based on their total revenue and determine if they are among the top 10 customers.
, rank_sales_data AS (
    SELECT
      t1.customer_id
      , t1.n_orders
      , t1.max_order_value
      , t1.total_revenue
      , t1.customer_since_date
      -- ranking calculation
      , CASE
        WHEN (
            SELECT COUNT(DISTINCT t2.total_revenue) 
            FROM sales_data AS t2 
            WHERE t2.total_revenue >= t1.total_revenue
        ) <= 10 THEN TRUE
        ELSE FALSE
    END AS top_10_customer
    FROM sales_data AS t1
    GROUP BY 1, 2, 3, 4, 5
)

-- STEP 4. Select customer information along with sales data, using left join with rank_sales_data to include ranking information.
SELECT 
  cus.*
  , COALESCE(customer_since_date, NULL) AS customer_since_date
  -- if no orders were made - insert 
  , COALESCE(n_orders, 0) AS n_orders
  , COALESCE(total_revenue, 0) AS total_order_value
  , COALESCE(max_order_value, 0) AS max_order_value
  , COALESCE(top_10_customer, FALSE) AS top_10_customer
FROM customer_base AS cus
LEFT JOIN rank_sales_data AS add_m ON add_m.customer_id = cus.customer_id
