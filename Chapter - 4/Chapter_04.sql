-- creating storage integration to connect to cloud.
use role ACCOUNTADMIN;
create storage integration PARK_IN_INTEGRATION
 type = external_stage
 storage_provider = 'S3'
 enabled = true
 storage_aws_role_arn = 'arn:aws:iam::625616352196:role/MySnowflakeRole'
 storage_allowed_locations = ('s3://parkinorders123/');

describe storage integration PARK_IN_INTEGRATION;

grant usage on integration PARK_IN_INTEGRATION to role SYSADMIN;

use role SYSADMIN;
use database BAKERY_DB;
create schema EXTERNAL_JSON_ORDERS;
use schema EXTERNAL_JSON_ORDERS;

-- create stage to load JSON format data into external stage.

create stage PARK_INN_STAGE
 storage_integration = PARK_IN_INTEGRATION
 url = 's3://parkinorders123/'
 file_format = (type = JSON);

list @PARK_INN_STAGE;

DESC INTEGRATION PARK_IN_INTEGRATION;

select $1 from @PARK_INN_STAGE;

-- inserting JSON data into variant data type.

use database BAKERY_DB;
use schema EXTERNAL_JSON_ORDERS;
create or replace table ORDERS_PARK_INN_RAW_STG(
 customer_orders variant,
 source_file_name varchar,
 load_ts timestamp
);

copy into ORDERS_PARK_INN_RAW_STG
from(
 select $1, metadata$filename, current_timestamp() from @PARK_INN_STAGE
)
on_error = abort_statement;

select * from ORDERS_PARK_INN_RAW_STG;

-- converting variant data type data and using flattening semistructured data into relational tables.
-- we use view so that it is less complex and costs less.

select 
 customer_orders : "Customer" :: varchar as customer,
 customer_orders : "Order date" :: date as order_date,
 customer_orders : "Orders"
from ORDERS_PARK_INN_RAW_STG;

select 
 customer_orders : "Customer" :: varchar as customer,
 customer_orders : "Order date" :: date as order_date,
 value : "Delivery date" :: date as delivery_date,
 value : "Orders by day"
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders");

select 
 customer_orders : "Customer" :: varchar as customer,
 customer_orders : "Order date" :: date as order_date,
 CO.value : "Delivery date" :: date as delivery_date,
 DO.value : "Baked good type" :: varchar as baked_good_type,
 DO.value : "Quantity" :: number as quantity
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO;

create or replace view ORDERS_PARK_INN_STG as
select
 customer_orders : "Customer" :: varchar as customer,
 customer_orders : "Order Date" :: date as order_date,
 CO.value : "Delivery date" :: date as delivery_date,
 DO.value : "Baked good type" :: varchar as baked_good_type,
 DO.value : "Quantity" :: number as quantity,
 source_file_name,
 load_ts
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO,

use database BAKERY_DB;
create schema TRANSFORM;
use schema TRANSFORM;

create view ORDERS_COMBINED_STG as
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from bakery_db.orders.orders_stg
union all
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from bakery_db.public.orders_bistro_stg
union all
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from bakery_db.external_json_orders.orders_park_inn_stg;

-- merging staging table data into target table data

use database BAKERY_DB;
use schema TRANSFORM;
create table CUSTOMER_ORDERS_COMBINED (
 customer varchar,
 order_date date,
 delivery_date date,
 baked_good_type varchar,
 quantity number,
 source_file_name varchar,
 load_ts timestamp
);

merge into CUSTOMER_ORDERS_COMBINED tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer
  and src.delivery_date = tgt.delivery_date
  and src.baked_good_type = tgt.baked_good_type
when matched then
 update set tgt.quantity = src.quantity,
  tgt.source_file_name = src.source_file_name,
  tgt.load_ts = src.load_ts
when not matched then
 insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
 values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());
 
select * from CUSTOMER_ORDERS_COMBINED order by quantity, delivery_date;
select * from CUSTOMER_ORDERS_COMBINED;

drop table CUSTOMER_ORDERS_COMBINED_PRO;

use database BAKERY_DB;
use schema TRANSFORM;
create table CUSTOMER_ORDERS_COMBINED_PRO (
 customer varchar,
 order_date date,
 delivery_date date,
 baked_good_type varchar,
 quantity number,
 source_file_name varchar,
 load_ts timestamp
);


-- basic stored procedure with exception handling
-- stored procedure : storing it in a procedure way to execute commands line by line. 

use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
merge into CUSTOMER_ORDERS_COMBINED_PRO tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer
  and src.delivery_date = tgt.delivery_date
  and src.baked_good_type = tgt.baked_good_type
when matched then
 update set tgt.quantity = src.quantity,
  tgt.source_file_name = src.source_file_name,
  tgt.load_ts = src.load_ts
when not matched then
 insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
 values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());
return 'Load Complete. ' || SQLROWCOUNT || ' rows affected';
exception
when other then
return 'Load failed with error message: ' || SQLERRM;
end;
$$
;

call LOAD_CUSTOMER_ORDERS();
select * from CUSTOMER_ORDERS_COMBINED_PRO;

-- adding lodging to stored procedures

use role SYSADMIN;
use schema TRANSFORM;
create event table BAKERY_EVENTS;

use role ACCOUNTADMIN;
alter account set event_table = BAKERY_DB.TRANSFORM.BAKERY_EVENTS;

grant modify log level on account to role SYSADMIN;

use role SYSADMIN;
alter procedure LOAD_CUSTOMER_ORDERS() set log_level = DEBUG;

use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
SYSTEM$LOG_DEBUG('LOAD_CUSTOMER_ORDERS begin ');
merge into CUSTOMER_ORDERS_COMBINED_PRO tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer
  and src.delivery_date = tgt.delivery_date
  and src.baked_good_type = tgt.baked_good_type
when matched then
 update set tgt.quantity = src.quantity,
  tgt.source_file_name = src.source_file_name,
  tgt.load_ts = src.load_ts
when not matched then
 insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
 values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());
return 'Load Complete. ' || SQLROWCOUNT || ' rows affected';
exception
when other then
return 'Load failed with error message: ' || SQLERRM;
end;
$$
;

call LOAD_CUSTOMER_ORDERS();

select * from BAKERY_EVENTS order by timestamp desc;

use database BAKERY_DB;
use schema TRANSFORM;
create table SUMMARY_ORDERS (
 delivery_date date,
 baked_good_type varchar,
 total_quantity number
);

truncate table SUMMARY_ORDERS;
insert into SUMMARY_ORDERS (delivery_date, baked_good_type, total_quantity)
select delivery_date, baked_good_type, sum(quantity) as total_quantity
from CUSTOMER_ORDERS_COMBINED
group by all;

-- truncating table and inserting summarized data

use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_SUMMARY_ORDERS()
returns varchar
language sql
as
$$
begin
SYSTEM$LOG_DEBUG('LOAD_CUSTOMER_SUMMARY_ORDERS begin ');
truncate table SUMMARY_ORDERS;
insert into SUMMARY_ORDERS (delivery_date, baked_good_type, total_quantity)
select delivery_date, baked_good_type, sum(quantity) as total_quantity
from CUSTOMER_ORDERS_COMBINED
group by all;
merge into CUSTOMER_ORDERS_COMBINED_PRO tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer
  and src.delivery_date = tgt.delivery_date
  and src.baked_good_type = tgt.baked_good_type
when matched then
 update set tgt.quantity = src.quantity,
  tgt.source_file_name = src.source_file_name,
  tgt.load_ts = src.load_ts
when not matched then
 insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
 values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());
return 'Load Complete. ' || SQLROWCOUNT || ' rows affected';
exception
when other then
return 'Load failed with error message: ' || SQLERRM;
end;
$$
;

call LOAD_CUSTOMER_SUMMARY_ORDERS();

select * from SUMMARY_ORDERS order by delivery_date desc;