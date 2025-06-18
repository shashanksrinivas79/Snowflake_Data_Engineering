use database CPG_RETAILERS_AND_DISTRIBUTORS;
select * from CORE_POI;

use role SYSADMIN;
select distinct
 postal_code,
 region,
 city,
 street_address,
 longitude,
 latitude,
 TO_GEOGRAPHY( -- This converts the data in Longitude and Latitude to Geographic data type.
  'Point('||longitude||' '||latitude||')' -- Point is used to concanicate the values into a string
 ) as store_loc_geo,
 ST_DISTANCE( -- This takes two parameters and provide the distance between them in meters.
  TO_GEOGRAPHY('Point(-84.19 39.76)'), store_loc_geo)/1609 as distance_miles
from CPG_RETAILERS_AND_DISTRIBUTORS.PUBLIC.CORE_POI
limit 5; -- limit to 5 results.

use role SYSADMIN;
use database BAKERY_DB;
use warehouse BAKERY_WH;
create schema RETAIL_ANALYSIS;
use schema RETAIL_ANALYSIS;

create table RETAILER_SALES as
select *,
 TO_GEOGRAPHY(
  'Point('||longitude||' '||latitude||')'
 ) as store_loc_geo,
 ST_DISTANCE(
  TO_GEOGRAPHY('Point(-84.19 39.76)'), store_loc_geo)/1000 as distance_km
from CPG_RETAILERS_AND_DISTRIBUTORS.PUBLIC.CORE_POI;

select distinct
 postal_code,
 region,
 city,
 street_address,
 distance_km
from RETAILER_SALES
order by distance_km
limit 100;

select SYSTEM$CLUSTERING_INFORMATION('retailer_sales', '(distance_km)');

select * from retailer_sales;

alter table retailer_sales cluster by (distance_km);

use role ACCOUNTADMIN;
grant monitor usage on account to role SYSADMIN;
use role SYSADMIN;

select * from table (information_schema.automatic_clustering_history(
 date_range_start=>dateadd(D, -1, current_date),
 table_name=>'BAKERY_DB.RETAIL_ANALYSIS.RETAILER_SALES'
));

-- chose store_id 392366678147865718 to perform further analysis

-- select each product sold in the chosen store and the total quantity sold
-- Listing 8.1
select 
  product_id, 
  sum(sales_quantity) as tot_quantity
from RETAILER_SALES
where store_id = 392366678147865718
group by product_id;

-- count the rows in the entire table and the number of filtered rows
select 
  'Total rows' as filtering_type, 
  count(*) as row_cnt 
from retailer_sales
union all
select 
  'Filtered rows' as filtering_type, 
  count(*) as row_cnt 
from retailer_sales 
where store_id = 392366678147865718;

-- view clustering information
select SYSTEM$CLUSTERING_INFORMATION('retailer_sales', '(store_id)');

-- add a clustering key
alter table RETAILER_SALES cluster by (store_id);

-- monitor the clustering process
-- grant privilege first
use role ACCOUNTADMIN;
grant MONITOR USAGE ON ACCOUNT to role SYSADMIN;
use role SYSADMIN;
select *
  from table(information_schema.automatic_clustering_history(
  date_range_start=>dateadd(D, -1, current_date),
  table_name=>'BAKERY_DB.RETAIL_ANALYSIS.RETAILER_SALES'));

-- execute the query from Listing 8.1 again
select 
  product_id, 
  sum(sales_quantity) as tot_quantity
from RETAILER_SALES
where store_id = 392366678147865718
group by product_id;

-- view the clustering information again
select SYSTEM$CLUSTERING_INFORMATION('retailer_sales', '(store_id)');

-- sum of the sold quantity of a chosen product in each store
-- if you don’t see a product with an ID value of 4120371332641752996, select a different product
-- Listing 8.4
select store_id, sum(sales_quantity) as tot_quantity
from RETAILER_SALES
where product_id = 4120371332641752996
group by store_id;

-- add search optimization
alter table RETAILER_SALES add search optimization on equality(product_id);

-- view the search optimization parameters
show tables like 'RETAILER_SALES';

-- grant the GOVERNANCE_VIEWER database role to SYSADMIN
use role ACCOUNTADMIN;
grant database role SNOWFLAKE.GOVERNANCE_VIEWER to role SYSADMIN;
use role SYSADMIN;

-- select the longest running queries from query history
select 
  query_id, 
  query_text, 
  partitions_scanned, 
  partitions_total, 
  total_elapsed_time
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where TO_DATE(start_time) > DATEADD(day,-1,TO_DATE(CURRENT_TIMESTAMP()))
order by total_elapsed_time desc
limit 50;
