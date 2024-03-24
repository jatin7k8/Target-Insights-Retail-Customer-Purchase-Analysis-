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

