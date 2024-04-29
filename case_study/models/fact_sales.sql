-- A transactional fact table for sales at the product level with additional dimensions and metrics
{{ config(
    materialized='table',
) }}

-- STEP 1. Define a CTE to select essential order information from the staging orders table.
WITH order_base AS (
  SELECT
    orderID AS order_id
    , orderDate AS order_date
    , customerID AS customer_id
  FROM {{ ref('stg_orders') }}
)

-- STEP 2. Define a CTE to calculate various metrics related to customer order history.
, add_customer_metrics AS (
  SELECT
    customer_id
    , MIN(order_date) AS first_order_date
    , MAX(order_date) AS last_order_date
    -- use sqlite julianday fun
    , julianday(MAX(order_date)) - julianday(MIN(order_date)) AS customer_lifetime
    , COUNT(*) AS n_orders
  FROM order_base 
  GROUP BY 1
)

-- STEP 2. Define a CTE to aggregate order item details along with customer and product information.
, order_item_base AS (
  SELECT
    ob.order_id
    , DATE(ob.order_date) AS order_date
    , ob.customer_id
    , cus.companyname AS customer_name
    -- customer is new if the date of purchase is the min purchasedate of that customer
    , CASE
      WHEN ob.order_date = first_order_date THEN 'new'
      ELSE 'recurring'
    END AS customer_type
    , CASE
      WHEN n_orders = 1 THEN NULL
      ELSE customer_lifetime
    END AS customer_lifetime
    , n_orders
    , DATE(first_order_date) AS customer_since_date
    , SUM(prd.unitprice * ord_d.quantity) AS total_amount
    , GROUP_CONCAT(
      '{"product_id": ' || ord_d.productid || ', "product_name": "' || prd.productName || '", "quantity": ' || ord_d.quantity || ', "discounted": ' || prd.unitprice || '}',
      ', '
    ) AS product_details
  FROM order_base AS ob
  LEFT JOIN {{ ref('stg_order_details') }} AS ord_d ON ord_d.orderid = ob.order_id
  LEFT JOIN {{ ref('stg_products') }} AS prd ON prd.productid = ord_d.productid
  LEFT JOIN {{ ref('stg_customers') }} AS cus ON cus.customerID = ob.customer_id
  LEFT JOIN add_customer_metrics AS add_m ON add_m.customer_id = ob.customer_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

-- Select all columns from the order_item_base CTE.
SELECT *
FROM order_item_base
