# Case: Analytics Engineer

### Completed Tasks

 The project with the necessary transformations to create the following models:

   1. A new project with connected data.sqilte database, which popelated tables are transfered to the staging layer
   2. A transactional fact table for sales *fact_sales*, with the grain set at the product level, and the following additional dimensions and metrics:
      1. new or returning customer
      2. number of days between first purchase and last purchase
      
   3. A dimension table for “customers” *dim_customers*, with the grain set at the customer_id, and the following additional dimensions and metrics:
      1. number of orders
      2. value of most expensive order
      3. whether it’s one of the top 10 customers (by revenue generated)

   4. A dimension table for monthly cohorts **fact_customers_cohort*, with the grain set at country leveland the following additional dimensions and metrics:
      1. Number of customers in the monthly cohort (customers are assigned in cohorts based on date of their first purchase)
      2. Cohort's total order value

### What should be done next

1. Create full documentations for the models
2. Write tests bith for source and models
3. Set up Job in prod env

### Note
As the project is quite small the layer structure of project is hard to show, so only stg and marts are represented



