use role SYSADMIN;
create database BAKERY_DB;
create schema ORDERS;

use database BAKERY_DB;
use schema ORDERS;
create stage ORDERS_STAGE;

list @ORDERS_STAGE;

select $1, $2, $3, $4, $5 from @ORDERS_STAGE;

use database BAKERY_DB;
use schema ORDERS;
create table ORDERS_STG(
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

use database BAKERY_DB;
use schema ORDERS;
copy into ORDERS_STG
from(
 select $1, $2, $3, $4, $5, metadata$filename, current_timestamp(), from @ORDERS_STAGE
)
file_format = (type = CSV, skip_header = 1)
on_error = abort_statement
purge = true;

select * from ORDERS_STG;

list @ORDERS_STAGE;

use database BAKERY_DB;
use schema ORDERS;
create table CUSTOMER_ORDERS(
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- target table
merge into CUSTOMER_ORDERS tgt
-- source table
using ORDERS_STG as src
-- primary keys/ uniqueness
on src.customer = tgt.customer
    and src.delivery_date = tgt.delivery_date
    and src.baked_good_type = tgt.baked_good_type
-- when matched update
when matched then
    update set tgt.quantity = src.quantity,
        tgt.source_file_name = src.source_file_name,
        tgt.load_ts = current_timestamp()
-- when not matched insert new values from src to tgt
when not matched then
    insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
    values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());

select * from customer_orders order by delivery_date, quantity desc;

use database BAKERY_DB;
use schema ORDERS;
create table SUMMARY_ORDERS(
 delivery_date date,
 baked_good_type varchar,
 total_quantity number
);

select delivery_date, baked_good_type, sum(quantity) as total_quantity from CUSTOMER_ORDERS
group by all;

truncate table SUMMARY_ORDERS;

insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
 select delivery_date, baked_good_type, sum(quantity) as total_quantity from CUSTOMER_ORDERS
 group by all;

select * from SUMMARY_ORDERS;

use database BAKERY_DB;
use schema ORDERS;
create task PROCESS_ORDERS
 warehouse = BAKERY_WH
 schedule = '10 M'
as
begin
 truncate table ORDERS_STG;
 copy into ORDERS_STG
 from (
  select $1, $2, $3, $4, $5, metadata$filename, current_timestamp()
  from @ORDERS_STAGE
 )
 file_format = (type = CSV, skip_header = 1)
 on_error = abort_statement
 purge = true;

 merge into CUSTOMER_ORDERS tgt
 using ORDERS_STG as src
 on src.customer = tgt.customer
  and src.delivery_date = tgt.delivery_date
  and src.baked_good_type = tgt.baked_good_type
 when matched then
  update set tgt.quantity = src.quantity,
      tgt.source_file_name = src.source_file_name,
      tgt.load_ts = current_timestamp()
 when not matched then
  insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
    values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name,     current_timestamp());
    
  truncate table SUMMARY_ORDERS;
  insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
    select delivery_date, baked_good_type, sum(quantity) as total_quantity
    from CUSTOMER_ORDERS
    group by all;
end;

use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

execute task PROCESS_ORDERS;

select * from table(information_schema.task_history())
 order by scheduled_time desc;

alter task PROCESS_ORDERS resume;

alter task PROCESS_ORDERS suspend;
alter task PROCESS_ORDERS
set schedule = 'USING CRON 0 23 * * * UTC';
alter task PROCESS_ORDERS resume;