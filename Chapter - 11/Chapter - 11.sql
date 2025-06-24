use role SYSADMIN;
use database BAKERY_DB;
create schema EXT with managed access;
create schema STG with managed access;
create schema DWH with managed access;
create schema MGMT with managed access;

use role SECURITYADMIN;
grant all on schema BAKERY_DB.EXT to role BAKERY_FULL;
grant all on schema BAKERY_DB.STG to role BAKERY_FULL;
grant all on schema BAKERY_DB.DWH to role BAKERY_FULL;
grant all on schema BAKERY_DB.MGMT to role BAKERY_FULL;

grant select on all tables in schema BAKERY_DB.MGMT to role BAKERY_READ;
grant select on all views in schema BAKERY_DB.MGMT to role BAKERY_READ;

grant select on future tables in schema BAKERY_DB.MGMT to role BAKERY_READ;
grant select on future views in schema BAKERY_DB.MGMT to role BAKERY_READ;

use role ACCOUNTADMIN;
create or replace storage integration PARK_INN_INTEGRATION
 type = external_stage
 storage_provider = 'S3'
 enabled = true
 storage_aws_role_arn = 'arn:aws:iam::625616352196:role/MySnowflakeRole'
 storage_allowed_locations = ('s3://parkinorders123/');

describe storage integration PARK_INN_INTEGRATION;

use role ACCOUNTADMIN;
grant usage on integration PARK_INN_INTEGRATION to role DATA_ENGINEER;

use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema EXT;

create or replace stage JSON_ORDERS_STAGE
 storage_integration = PARK_INN_INTEGRATION
 url = 's3://parkinorders123/'
 file_format = (type = JSON);

list @JSON_ORDERS_STAGE;

-- external layer

create or replace table JSON_ORDERS_EXT(
 customer_orders variant,
 source_file_name varchar,
 load_ts timestamp
);

copy into JSON_ORDERS_EXT
from(
 select $1, metadata$filename, current_timestamp() from @JSON_ORDERS_STAGE
)
on_error = abort_statement;

select * from JSON_ORDERS_EXT;

-- staging layer
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema STG;
use database BAKERY_DB;
use schema STG;

create or replace view JSON_ORDERS_STG as
select
 customer_orders : "Customer"::varchar as customer,
 customer_orders : "Order date"::date as order_date,
 co.value:"Delivery date"::date as delivery_date,
 do.value:"Baked good type"::varchar as baked_good_type,
 do.value:"Quantity"::number as quantity,
 source_file_name,
 load_ts
from EXT.JSON_ORDERS_EXT,
lateral flatten (input => customer_orders:"Orders") co,
lateral flatten (input => co.value:"Orders by day") do;

select * from JSON_ORDERS_STG;

-- create tables in the STG schema, simulating tables populated from the source system using a data integration tool or custom solution
create table PARTNER (
partner_id integer,
partner_name varchar,
address varchar,
rating varchar,
valid_from date
);

insert into PARTNER values
(101, 'Coffee Pocket', '501 Courtney Wells', 'A', '2023-06-01'),
(102, 'Lily''s Coffee', '2825 Joshua Forest', 'A', '2023-06-01'),
(103, 'Crave Coffee', '538 Hayden Port', 'B', '2023-06-01'),
(104, 'Best Burgers', '790 Friedman Valley', 'A', '2023-06-01'),
(105, 'Page One Fast Food', '44864 Amber Walk', 'B', '2023-06-01'),
(106, 'Jimmy''s Diner', '2613 Scott Mountains', 'A', '2023-06-01'),
(107, 'Metro Fine Foods', '520 Castillo Valley', 'A', '2023-06-01'),
(108, 'New Bistro', '494 Terry Spurs', 'A', '2023-06-01'),
(109, 'Park Inn', '3692 Nelson Turnpike', 'A', '2023-06-01'),
(110, 'Chef Supplies', '870 Anthony Hill', 'A', '2023-06-01'),
(111, 'Farm Fresh', '23633 Melanie Ranch', 'A', '2023-06-01'),
(112, 'Murphy Mill', '700 Darren Centers', 'A', '2023-06-01');

select * from PARTNER;

create table PRODUCT (
product_id integer,
product_name varchar,
category varchar,
min_quantity integer,
price number(18,2),
valid_from date
);

insert into PRODUCT values
(1, 'Baguette', 'Bread', 2, 2.5, '2023-06-01'),
(2, 'Bagel', 'Bread', 6, 1.3, '2023-06-01'), 
(3, 'English Muffin', 'Bread', 6, 1.2, '2023-06-01'), 
(4, 'Croissant', 'Pastry', 4, 2.1, '2023-06-01'), 
(5, 'White Loaf', 'Bread', 1, 1.8, '2023-06-01'), 
(6, 'Hamburger Bun', 'Bread', 10, 0.9, '2023-06-01'), 
(7, 'Rye Loaf', 'Bread', 1, 3.2, '2023-06-01'), 
(8, 'Whole Wheat Loaf', 'Bread', 1, 2.8, '2023-06-01'), 
(9, 'Muffin', 'Pastry', 12, 3.0, '2023-06-01'), 
(10, 'Cinnamon Bun', 'Pastry', 6, 3.4, '2023-06-01'), 
(11, 'Blueberry Muffin', 'Pastry', 12, 3.6, '2023-06-01'), 
(12, 'Chocolate Muffin', 'Pastry', 12, 3.6, '2023-06-01'); 

select * from PRODUCT;

use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- create views PARTNER and PRODUCT in the DWH schema that select data from the STG schema
create view PARTNER as
select partner_id, partner_name, address, rating
from STG.PARTNER;

create view PRODUCT as
select product_id, product_name, category, min_quantity, price, valid_from
from STG.PRODUCT;

-- create view ORDERS in the DWH schema that adds primary keys from the PARTNER and PRODUCT tables
create view ORDERS as
select PT.partner_id, PRD.product_id, ORD.delivery_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_STG ORD
inner join STG.PARTNER PT
  on PT.partner_name = ORD.customer
inner join STG.PRODUCT PRD
  on PRD.product_name = ORD.baked_good_type;

select * from ORDERS;

use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema MGMT;

-- create view in the MGMT schema that summarizes orders by delivery date and baked good type, adding the baked good category
create view ORDERS_SUMMARY as
select ORD.delivery_date, PRD.product_name, PRD.category, sum(ORD.quantity) as total_quantity
from dwh.ORDERS ORD
left join dwh.PRODUCT PRD
on ORD.product_id = PRD.product_id
group by all;

-- use the DATA_ANALYST role to select data from the summary view
use role DATA_ANALYST;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema MGMT;

select * from ORDERS_SUMMARY;