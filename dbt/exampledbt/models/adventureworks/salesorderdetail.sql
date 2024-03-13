{{ config(
    materialized='table',
    indexes=[
      {'columns': ['salesorderid', 'salesorderdetailid'], 'unique': True},
    ]
    ) }}

SELECT salesorderid, salesorderdetailid, carriertrackingnumber, orderqty, productid, specialofferid, unitprice, unitpricediscount, rowguid, modifieddate
FROM sales.salesorderdetail

