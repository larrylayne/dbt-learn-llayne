with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select order_id,
           SUM(amount_usd) as order_total
    from {{ ref('stg_payments') }}
    where payment_status = 'success'
    group by 1

),

final as (

    select orders.order_id,
           orders.customer_id,
           orders.order_date,
           payments.order_total

    from orders

    left join payments using (order_id)

)

select * from final