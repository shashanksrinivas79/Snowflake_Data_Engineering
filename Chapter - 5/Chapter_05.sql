-- creating storage integration with AZURE
use role ACCOUNTADMIN;
create storage integration SPEEDY_INTEGRATION
 type = external_stage
 storage_provider = 'AZURE'
 enabled = true
 azure_tenant_id = '41f88ecb-ca63-404d-97dd-ab0a169fd138'
 storage_allowed_locations = ('azure://bakeryorders123.blob.core.windows.net/speedyservicefiles/');

 desc integration SPEEDY_INTEGRATION;

 -- Granting permissions to role as SYSADMIN

 grant usage on integration SPEEDY_INTEGRATION to role SYSADMIN;

 use role SYSADMIN;
 use database BAKERY_DB;
 create schema DELIVERY_ORDERS;
 use schema DELIVERY_ORDERS;

 -- Creating external stage with integration SPEEDY_INTEGRATION

 create stage SPEEDY_STAGE
  storage_integration = SPEEDY_INTEGRATION
  url = 'azure://bakeryorders123.blob.core.windows.net/speedyservicefiles/'
  file_format = (type = JSON);

list @SPEEDY_STAGE;

select $1 from @SPEEDY_STAGE;

-- Extracting values from keys in JSON

select 
 $1 : "Order id",
 $1 : "Order datetime",
 $1 : "Items",
 metadata$filename,
 current_timestamp()
from @SPEEDY_STAGE;

-- Creating staging table.

create or replace table SPEEDY_ORDERS_RAW_STG (
 order_id varchar,
 order_datetime timestamp,
 items variant,
 source_file_name varchar,
 load_ts timestamp
);

-- creating a notification integration

use role ACCOUNTADMIN;
CREATE OR REPLACE NOTIFICATION INTEGRATION SPEEDY_QUEUE_INTEGRATION
ENABLED = true
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://bakeryorders123.queue.core.windows.net/speedyordersqueue'
AZURE_TENANT_ID = '41f88ecb-ca63-404d-97dd-ab0a169fd138';


describe notification integration SPEEDY_QUEUE_INTEGRATION;

grant usage on integration SPEEDY_QUEUE_INTEGRATION to role SYSADMIN;

desc integration SPEEDY_QUEUE_INTEGRATION;

-- creating SnowPipe and copying the data into staging table from external stage.
-- System integration IAM role should be storage bob data reader and notification integration should be storage bob data contributor.

use role SYSADMIN;
use database BAKERY_DB;
use schema DELIVERY_ORDERS;
create or replace pipe SPEEDY_PIPE
auto_ingest = true
integration = 'SPEEDY_QUEUE_INTEGRATION'
as
copy into SPEEDY_ORDERS_RAW_STG
from (
  select
    $1:"Order id",
    $1:"Order datetime",
    $1:"Items",
    metadata$filename,
    current_timestamp()
  from @SPEEDY_STAGE
);


select * from SPEEDY_ORDERS_RAW_STG;

drop table SPEEDY_ORDERS_RAW_STG;

alter pipe SPEEDY_PIPE refresh;

use role ACCOUNTADMIN;

select system$pipe_status ('SPEEDY_PIPE');

-- Grant SYSADMIN role to your user
grant role SYSADMIN to user SHASHANK;

-- Grant SYSADMIN access to the external stage
grant usage on stage SPEEDY_STAGE to role SYSADMIN;

-- Grant SYSADMIN access to the storage integration
grant usage on integration SPEEDY_INTEGRATION to role SYSADMIN;

-- (Optional) Grant access to the target table, if needed
grant insert on table SPEEDY_ORDERS_RAW_STG to role SYSADMIN;

select * from table (information_schema.copy_history(
 table_name => 'SPEEDY_ORDERS_RAW_STG',
 start_time => dateadd(hours, -1, current_timestamp())
));

-- flattening the JSON structure to relational format

select
 order_id,
 order_datetime,
 value : "Item" :: varchar as baked_good_type,
 value : "Quantity" :: number as quantity,
from SPEEDY_ORDERS_RAW_STG,
lateral flatten (input => items);

select order_id from speedy_orders_raw_stg where order_datetime is null;

-- transforming data into dynamic tables.
 
create or replace dynamic table SPEEDY_ORDERS
 target_lag = '1 minute'
 warehouse = BAKERY_WH
 as
 select
 order_id,
 order_datetime,
 value : "Item" ::  varchar as baked_good_type,
 value : "Quantity" :: number as quantity,
 source_file_name,
 load_ts
from speedy_orders_raw_stg,
lateral flatten (input => items);

select * from SPEEDY_ORDERS order by order_datetime desc;

select * from table
(information_schema.dynamic_table_refresh_history())
order by refresh_start_time desc;