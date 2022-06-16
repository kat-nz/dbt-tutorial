{{
  config(
    materialized='table'
  )
}}

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}


),

customer_orders as (

    select
        customerid,

        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date,
        count(orderid) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customerid,
        customers.firstname,
        customers.lastname,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customerid)

)

select * from final