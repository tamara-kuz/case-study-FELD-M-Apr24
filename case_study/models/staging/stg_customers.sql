-- Source: Define the stg table containing customers data
{{ config(
    materialized='table',
) }}


SELECT
  customerID
  ,	companyName
  , contactName
  , contactTitle
  , address
  , city
  , region
  , postalCode
  , country
  , phone
  , fax
FROM {{ source('main', 'customers') }}
