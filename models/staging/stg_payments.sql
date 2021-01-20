select 
    
    ORDERID as order_id,
    CAST(AMOUNT as integer)/100 as amount_usd,
    STATUS as payment_status

from raw.stripe.payment