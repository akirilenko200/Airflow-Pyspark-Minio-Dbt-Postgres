{{ config(
    materialized='table',
    indexes=[
      {'columns': ['salesorderid'], 'unique': True},
    ]
    ) 
}}

SELECT salesorderid, revisionnumber, orderdate, duedate, shipdate, status, onlineorderflag, purchaseordernumber, "accountnumber", customerid, salespersonid, territoryid, billtoaddressid, shiptoaddressid, shipmethodid, creditcardid, creditcardapprovalcode, currencyrateid, subtotal, taxamt, freight, totaldue, "comment", rowguid, modifieddate
FROM sales.salesorderheader
