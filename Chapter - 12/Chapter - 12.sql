use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema EXT;

create or replace table JSON_ORDERS_EXT(
 customer_orders variant,
 source_file_name varchar,
 load_ts timestamp
);

-- create stream 
create stream JSON_ORDERS_STREAM on table JSON_ORDERS_EXT;

select * from JSON_ORDERS_STREAM; -- empty because stream just got created

list @JSON_ORDERS_STAGE;

copy into JSON_ORDERS_EXT
from (
 select $1, metadata$filename, current_timestamp() from @JSON_ORDERS_STAGE
)
on_error = abort_statement;

select * from JSON_ORDERS_STREAM;

-- Extrernal table
create table STG.JSON_ORDERS_TABLE_STG(
 customer varchar,
 order_date date,
 deliver_date date,
 baked_good_type varchar,
 quantity number,
 source_file_name varchar,
 load_ts timestamp
);

insert into STG.JSON_ORDERS_TABLE_STG 
 select 
  customer_orders:"Customer"::varchar as customer,
  customer_orders:"Order date"::date as order_date,
  co.value:"Delivery date"::date as delivery_date,
  do.value:"Baked good type"::varchar as baked_good_type,
  do.value:"Quantity"::number as quantity,
  source_file_name,
  load_ts
from EXT.JSON_ORDERS_STREAM,
lateral flatten (input => customer_orders:"Orders") co,
lateral flatten (input => co.value:"Orders by day") do;

select * from STG.JSON_ORDERS_TABLE_STG;

-- repeat it with uploading other file into the storage location and perform copy command again and followed by the insert command to insert data into the table, now only the new uploaded file shows up on stream althrough there is already a file in the storage location.

use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

create table PRODUCT_TBL as select * from PRODUCT;

select * from PRODUCT_TBL;

use schema STG;
create stream PRODUCT_STREAM on table PRODUCT;

select * from PRODUCT_STREAM;

-- updating product table

update PRODUCT
 set category = 'Pastry', valid_from = '2023-08-08' where product_id = 3;

insert into PRODUCT values
 (13, 'Sourdough Bread', 'Bread', 1, 3.6, '2023-08-08');

insert into DWH.PRODUCT_TBL
 select product_id, product_name, category, min_quantity, price, valid_from from product_stream where metadata$action = 'INSERT';

select * from PRODUCT_STREAM;

select * from DWH.PRODUCT_TBL order by product_id;

create view DWH.PRODUCT_VALID_TS as
select 
 product_id, product_name, category, min_quantity, price, valid_from, NVL(
  LEAD(valid_from) over (partition by product_id order by valid_from),
  '9999-12-31') as valid_to
from DWH.PRODUCT_TBL
order by product_id;

select * from DWH.PRODUCT_VALID_TS;

-- create a table in the data warehouse layer and populate initially with the data from the staging layer
use schema DWH;

create table PARTNER_TBL as select * from STG.PARTNER;

select * from PARTNER_TBL;

use schema STG;
create stream PARTNER_STREAM on table PARTNER;

DROP STREAM IF EXISTS PARTNER_STREAM;

update PARTNER
 set rating = 'A', valid_from = '2023-08-08'
 where partner_id = 103;

select * from PARTNER_STREAM;

insert into DWH.PARTNER_TBL
 select partner_id, partner_name, address, rating, valid_from from PARTNER_STREAM where metadata$action = 'INSERT';

select * from DWH.PARTNER_TBL;

create view DWH.PARTNER_VALID_TS as
 select partner_id, partner_name, address, rating, valid_from, NVL(
  LEAD(valid_from) over (partition by partner_id order by valid_from), '9999-12-31'
 )as valid_to
from DWH.PARTNER_TBL
order by PARTNER_ID;

select * from DWH.PARTNER_VALID_TS;

-- selects normalized data

use schema DWH;
create dynamic table ORDERS_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select PT.partner_id, PRD.product_id, ORD.deliver_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_TABLE_STG ORD
inner join STG.PARTNER PT
  on PT.partner_name = ORD.customer
inner join STG.PRODUCT PRD
  on PRD.product_name = ORD.baked_good_type;

select * from ORDERS_TBL;

select ORD.deliver_date, PRD.product_name, PRD.category, sum(ORD.quantity) as total_quantity from DWH.ORDERS_TBL ORD
left join DWH.PRODUCT_TBL PRD
on ORD.product_id = PRD.product_id
group by all;

select * from DWH.PARTNER_VALID_TS where valid_to = '9999-12-31';

select ORD.deliver_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from dwh.ORDERS_TBL ORD
left join (select * from dwh.PRODUCT_VALID_TS where valid_to = '9999-12-31') PRD
on ORD.product_id = PRD.product_id
group by all;

use schema MGMT;
create dynamic table ORDERS_SUMMARY_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select ORD.deliver_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from dwh.ORDERS_TBL ORD
left join (select * from dwh.PRODUCT_VALID_TS where valid_to = '9999-12-31') PRD
on ORD.product_id = PRD.product_id
group by all;

select * from ORDERS_SUMMARY_TBL;








  