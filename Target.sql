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




