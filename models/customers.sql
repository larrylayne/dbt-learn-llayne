with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select order_id,
           customer_id,
           order_date
           
    from {{ ref('stg_orders')}}
    --where status != 'returned'

),

payments as (

    select order_id,
           SUM(amount_usd) as order_total
    from {{ ref('stg_payments') }}
    where payment_status = 'success'
    group by 1

),

orders_with_payment as (

    select orders.order_id,
           orders.customer_id,
           orders.order_date,
           payments.order_total

    from orders

    left join payments using (order_id)

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(order_total) as total_amount_spent

    from orders_with_payment

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.total_amount_spent

    from customers

    left join customer_orders using (customer_id)

)

select * from final