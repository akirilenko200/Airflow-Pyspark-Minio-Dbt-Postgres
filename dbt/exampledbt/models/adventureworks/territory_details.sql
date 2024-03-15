{{ config(
    materialized='table',
    ) }}

select  st.name as territory_name, date_part('year', soh.modifieddate) as year, date_part('month', soh.modifieddate) as month, sum(soh.totaldue) as total_due_per_territory_per_month
from {{ ref('salesorderheader')}} as soh
left join sales.salesterritory st on soh.territoryid = st.territoryid
group by st.name, date_part('year', soh.modifieddate), date_part('month', soh.modifieddate)
order by year desc, total_due_per_territory_per_month