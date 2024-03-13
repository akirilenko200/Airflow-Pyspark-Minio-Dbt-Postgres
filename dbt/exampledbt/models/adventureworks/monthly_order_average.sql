{{ config(
    materialized='table',
    ) }}

select date_part('year', modifieddate) as year, date_part('month', modifieddate) as month, sum(orderqty * unitprice) as total_revenue_per_month, sum(orderqty) as number_of_items_per_month
from {{ ref('salesorderdetail')}}
group by date_part('year', modifieddate), date_part('month', modifieddate)
order by year desc, total_revenue_per_month desc