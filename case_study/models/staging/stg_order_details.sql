-- Source: Define the stg table containing order details data
{{ config(
    materialized='table',
) }}


SELECT 
  orderID
  , productID
  , unitPrice
  , quantity
  , discount
FROM {{ source('main', 'order_details') }}
