--                                               -------------Target Insights: Retail Customer Purchase Analysis with SQL, BigQuery-----------------------------------------------

-- 1. Import the dataset and do usual exploratory analysis steps like checking the structure & characteristics of the dataset:

-- 1.1 Data type of all columns in the "customers" table.
select
table_name,
column_name,
data_type
from `Target.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'customers';

-- 1.2 Get the time range between which the orders were placed.
SELECT 
max(EXTRACT(date FROM order_purchase_timestamp )) as order_by_date,
min(EXTRACT(date FROM order_purchase_timestamp )) as order_started_date
FROM `Target.orders`;

-- 1.3 Write an SQL query to retrieve the top 5 orders by order count from the dataset Target.orders. Include the date of purchase, 
--the total count of orders for each date, and the start and end times of purchase for each date. Ensure that the orders are sorted in descending order based on the order count.
select
Extract(date from order_purchase_timestamp) as order_by_date,
count(order_id) as order_count,
min(extract(time FROM order_purchase_timestamp )) as start_time,
max(extract(time FROM order_purchase_timestamp )) as end_time
from `Target.orders`
group by 1
order by 2 DESC limit 5

-- 1.4 Count the Cities & States of customers who ordered during the given period.
select 
count(distinct c1.customer_state) as no_of_states,
count(distinct c1.customer_city) as no_of_city
from `Target.orders` as o1 
join `Target.customers` as c1 
  on o1.customer_id = c1.customer_id


-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. In-depth Exploration:
  
-- 2.1 . Is there a growing trend in the no. of orders placed over the past years? 
 
-- Year on year tread- > 
with cte as (
select
order_on_year,
count(order_id) as count_order
from
(
SELECT *,
EXTRACT(year FROM order_purchase_timestamp ) as order_on_year
FROM `Target.orders`)
group by order_on_year
),
cte2 as (
select 
order_on_year,
count_order,
lag(count_order,1) over(order by order_on_year) as next_value,
from cte)

select
order_on_year,
count_order,
round((next_value - count_order)/ count_order* 100,2) as increment_order
from cte2
order by 1; 

-- year on month on month tread


with cte as 
(
select 
extract(year from  order_purchase_timestamp) as year,
extract(month from order_purchase_timestamp) as month,
count(*) as Total_orders
from `Target.orders`
where order_status not in ('canceled', 'unavailable') 
group by 1, 2
order by 1, 2
)
select 
*,
concat(round((Total_orders- prev_year_orders)/prev_year_orders*100 ,2)," % ") as trend
from
(
select 
*,
lag(Total_orders) over(order by year , month ) as prev_year_orders
from cte
order by year, month
)
--2.2 . Can we see some kind of monthly seasonality in terms of the no. of orders being placed?
select
order_on_month,
count(order_id) as count_order
from
(
SELECT *,
EXTRACT(month FROM order_purchase_timestamp ) as order_on_month
FROM `Target.orders`)
where order_status = 'delivered'
group by order_on_month
order by 1 asc , 2 DESC;
  '''
  2.3 During what time of the day, do the Brazilian customers mostly place their orders? (Dawn, Morning, Afternoon or Night)
0-6 hrs : Dawn
7-12 hrs : Mornings
13-18 hrs : Afternoon
19-23 hrs : Night'''

with cte as 
(
SELECT
*,
CASE 
WHEN EXTRACT(HOUR FROM order_purchase_timestamp AT TIME ZONE "UTC") BETWEEN 0 AND 6 THEN "Dawn"
WHEN EXTRACT(HOUR FROM order_purchase_timestamp AT TIME ZONE "UTC") BETWEEN 7 AND 12 THEN "Mornings"
WHEN EXTRACT(HOUR FROM order_purchase_timestamp AT TIME ZONE "UTC") BETWEEN 13 AND 18 THEN "Afternoon"
WHEN EXTRACT(HOUR FROM order_purchase_timestamp AT TIME ZONE "UTC") BETWEEN 19 AND 23 THEN "Night"
end as Days
FROM `Target.orders`)

select cte.Days,
  count(*) as no_of_days
from cte 
group by cte.Days
order by 2 DESC 

-- ***************************************************************************************3.Evolution of E-commerce orders in the Brazil region***************************************************************************************
-- 3.1.Get the month on month no. of orders placed in each state.
-- year over the months per state
with cte as
(
select *,
extract(year from order_purchase_timestamp) as order_on_year,
extract(month from order_purchase_timestamp) as order_on_month
FROM `Target.orders`)

select customer_state,
      order_on_year,
      order_on_month,
    sum(order_on_month) as no_of_orders
from `Target.customers` as c
join cte 
  on c.customer_id = cte.customer_id
group by customer_state, order_on_year,order_on_month
order by 1 ,2, 3 ,4 DESC

-- month on month per state
--   3.2   How are the customers distributed across all the states?
select
  customer_state,
  count(customer_id) as no_of_customers
from `Target.customers` 
group by customer_state
order by 2 DESC;


-- **************************************4.Impact on Economy: Analyze the money movement by e-commerce by looking at order prices, freight and others************************************


--  4.1  Get the % increase in the cost of orders from year 2017 to 2018 (include months between Jan to Aug only).
with cte as
(
select 
extract(year from o.order_purchase_timestamp) as year,
sum(payment_value) as cost
from `Target.orders` as o 
join `Target.payments` as p 
  on o.order_id = p.order_id
where extract(year from o.order_purchase_timestamp) between 2017 and 2018
and extract(month from o.order_purchase_timestamp) between 1 and 8
group by 1
)

select 
*,
(cost - lag(cost,1) over(order by year)) *100/lag(cost,1) over(order by year) as percent_increase
from cte
order by year


-- 4.2 Calculate the Total & Average value of order price for each state.
with cte as 
(select 
  o.order_id,
customer_state
from `Target.customers` as c
join  `Target.orders` as o
  on o.customer_id = c.customer_id)
select 
  cte.customer_state,
  Round(sum(oi.price),2) as Total_price,
  round(avg(oi.price),2) as avg_price
from `Target.order_items` as oi
join cte 
  on oi.order_id = cte.order_id
group by cte.customer_state
order by 1;



-- 4.3 Calculate the Total & Average value of order freight for each state.

with cte as 
(select 
  o.order_id,
customer_state
from `Target.customers` as c
join  `Target.orders` as o
  on o.customer_id = c.customer_id)

select 
  cte.customer_state,
  Round(sum(oi.freight_value),2) as Total_freight_value,
  round(avg(oi.freight_value),2) as avg_freight_value
from `Target.order_items` as oi
join cte 
  on oi.order_id = cte.order_id
group by cte.customer_state
order by 1;


-- ------------------------------------------------------5. Analysis based on sales, freight and delivery time.

'''5.1 Find the no. of days taken to deliver each order from the order’s purchase date as delivery time.
Also, calculate the difference (in days) between the estimated & actual delivery date of an order.
Do this in a single query.

You can calculate the delivery time and the difference between the estimated & actual delivery date using the given formula:

-----------        time_to_deliver = order_delivered_customer_date - order_purchase_timestamp
------------       diff_estimated_delivery = order_delivered_customer_date - order_estimated_delivery_date'''

select 
*,
date_diff(order_delivered_customer_date, order_purchase_timestamp, day) as Actual_delivery_date,
date_diff(order_estimated_delivery_date, order_delivered_customer_date,day) as estimated_delivery
from  `Target.orders`


-- 5.2 Find out the top 5 states with the highest & lowest average freight value.

select * from 
(
select 
  c.customer_state,
  round(avg(oi.freight_value),2) as avg_freight_value,
  "Top 5 high value " as sorted
from `Target.customers` as c
join  `Target.orders` as o
  on o.customer_id = c.customer_id
join `Target.order_items` as oi
  on oi.order_id = o.order_id
group by c.customer_state
order by  2 DESC limit 5) as t1
union all
select * from 
(
select 
  c.customer_state,
  round(avg(oi.freight_value),2) as avg_freight_value,
  "Top 5 low value" as sorted
from `Target.customers` as c
join  `Target.orders` as o
  on o.customer_id = c.customer_id
join `Target.order_items` as oi
  on oi.order_id = o.order_id
group by c.customer_state
order by  2 ASC limit 5) as t2


-- 5.3 Find out the top 5 states with the highest & lowest average delivery time.

with cte as
(
select state,'FAST'as val,avg(delivery_time) as avg_delivery_time,
dense_rank() over (order by avg(delivery_time) desc) as rnk
from
(
select customer_state as state,
datetime_diff(order_delivered_customer_date,order_purchase_timestamp,day) as delivery_time,
from `Target.customers` as c
join `Target.orders` as o on c.customer_id = o.customer_id
group by state,order_delivered_customer_date,order_purchase_timestamp,delivery_time
) nt1
group by state

union all

select state,'SLOW'as val,avg(delivery_time) as avg_delivery_time,
dense_rank() over (order by avg(delivery_time) asc) as rnk
from
(
select customer_state as state,
datetime_diff(order_delivered_customer_date,order_purchase_timestamp,day) as delivery_time,
from `Target.customers` as c
join `Target.orders` as o on c.customer_id = o.customer_id
group by state,order_delivered_customer_date,order_purchase_timestamp,delivery_time
) nt2
group by state
)

select concat(val," - ",rnk) as speed_of_delivery,state,round(avg_delivery_time,2)as Avg_delivery_time
from cte 
where rnk<=5
order by 1;

-- 5.4 Find out the top 5 states where the order delivery is really fast as compared to the estimated date of delivery.
-- You can use the difference between the averages of actual & estimated delivery date to figure out how fast the delivery was for each state.

select 
t1.customer_state,
round((t1.Actual_delivery_days- t1.estimated_delivery_days),2) as fastest_delivery
  from 
(select 
c.customer_state,
avg(date_diff(order_delivered_customer_date, order_purchase_timestamp, day)) as Actual_delivery_days,
avg(date_diff(order_estimated_delivery_date, order_delivered_customer_date,day)) as estimated_delivery_days

from  `Target.orders` as o
join `Target.customers` as c 
on c.customer_id = o.customer_id
where o.order_status = "delivered" 
group by c.customer_state) as t1

where round((t1.Actual_delivery_days- t1.estimated_delivery_days),2) >=0
order by 2 ASC
limit 5 ;

-- ************************************************* 6. Analysis based on the payments:

-- 6.1 Find the month on month no. of orders placed using different payment types.

-- year on month over the years 
with cte as
(
select *,
extract(year from order_purchase_timestamp) as payment_year,
extract(month from order_purchase_timestamp) as payment_month
FROM `Target.orders`)

select p.payment_type,
    cte.payment_year,
    cte.payment_month,
    count(*) as no_of_orders
from `Target.payments` as p
join cte 
on cte.order_id = p.order_id
group by 1, 2, 3
order by 1 ,2, 3 ,4 DESC;

-- month on month 

with cte as
(
select *,
extract(month from order_purchase_timestamp) as payment_month
FROM `Target.orders`)

select p.payment_type,
    cte.payment_month,
    count(*) as no_of_orders
from `Target.payments` as p
join cte 
on cte.order_id = p.order_id
group by 1, 2 
order by 1 ,2, 3 DESC;

-- 6.2 Find the no. of orders placed on the basis of the payment installments that have been paid.
select payment_installments,
count(order_id) as no_orders
from `Target.payments`
where payment_installments >1
group by 1
order by 1

