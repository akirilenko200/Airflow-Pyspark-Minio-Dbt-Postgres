{{ config(
    materialized='table',
    ) }}

select coalesce(prsc.name, 'Unknown Category') as category, sum(sod.orderqty * sod.unitprice) as total_revenue_per_category, sum(sod.orderqty) as number_of_orders_per_category
from {{ ref('salesorderdetail')}} as sod
left outer join production.product pr on sod.productid = pr.productid
left outer join production.productsubcategory prsc on prsc.productsubcategoryid  = pr.productsubcategoryid
left outer join production.productcategory prc on prc.productcategoryid  = prsc.productcategoryid
group by coalesce(prsc.name, 'Unknown Category')
order by total_revenue_per_category desc



