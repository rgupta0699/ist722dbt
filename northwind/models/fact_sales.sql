with stg_order_details as (
    select
        orderid,
        productid,
        quantity,
        unitprice,
        discount,
        sum(Quantity) as quantityonorder,
        sum(Quantity * UnitPrice * (1 - Discount)) as totalorderamount
    from {{ source('northwind', 'Order_Details') }}
    group by orderid,productid,
        quantity,
        unitprice,
        discount
),
stg_orders as (
    select 
        OrderID,
        employeeid,
        customerid,
        orderdate,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey,
        replace(to_date(shippeddate)::varchar, '-', '')::int as shippeddatekey,
        replace(to_date(requireddate)::varchar, '-', '')::int as requireddatekey,
        shipname,
        shipaddress,
        shipcity,
        shipregion,
        shippostalcode,
        shipcountry,
        freight,
        shipvia 
    from {{ source('northwind', 'Orders') }}
)
SELECT 
    o.employeeid,
    o.customerid,
    o.orderdate,
    od.productid,
    o.orderid,
    od.quantity,
    {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
    {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
    replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey,
    od.quantity * od.unitprice AS extendedpriceamount,
    od.quantity * od.unitprice * od.discount AS discountamount,
    od.quantity * od.unitprice * (1 - od.discount) AS soldamount
FROM 
    stg_orders o
JOIN 
    stg_order_details od ON o.orderid = od.orderid
