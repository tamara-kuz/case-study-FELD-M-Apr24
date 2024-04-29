/* This checks if total number in top 10 customers = 10
*/

WITH test_10 AS (
  SELECT
    SUM(top_10_customer) AS top_10
  FROM {{ ref('dim_customers') }}
)

SELECT *
FROM test_10
WHERE top_10 != 10