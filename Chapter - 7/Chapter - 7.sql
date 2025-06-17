-- creating schema reviews
use role SYSADMIN;
use database BAKERY_DB;
create schema REVIEWS;
use schema REVIEWS;

-- As user need privileges to create objects required for external access integration we grand these roles to SYSADMIN.

use role ACCOUNTADMIN;
grant create network rule on schema REVIEWS to role SYSADMIN;
grant create secret on schema REVIEWS to role SYSADMIN;
grant create integration on account to role SYSADMIN;

-- Creating a network rule.

use role SYSADMIN;
create network rule YELP_API_NETWORK_RULE
 mode = EGRESS
 type = HOST_PORT
 value_list = ('api.yelp.com');

 -- Create secret to store API key we obtaines with datatype GENERIC_STRING

 create secret YELP_API_TOKEN
  type = GENERIC_STRING
  secret_string = 'Jxy_X1-UnwnwPRoZCHovEi6M6nK9ZvivGA921ZkPvqHvkAbW8db4v5m3RYrDTu4Ysfc1YEWATXeICSvveQBcBPXN-eEhTwxEQQvXeguoeZ-DoCsxbfACCzbEh7NRaHYx';

-- Creating external access integration

create external access integration YELP_API_INTEGRATION
 allowed_network_rules = (YELP_API_NETWORK_RULE)
 allowed_authentication_secrets = (YELP_API_TOKEN)
 enabled = true;

 select * from YELP_SINGLE_RESTAURANT_REVIEWS;

-- interpreting order emails using LLM's

create or replace procedure READ_EMAIL_PROC(email_content varchar)
returns table()
language python
runtime_version = 3.10
handler = 'get_order_info_from_email'
packages = ('snowflake-snowpark-python', 'snowflake-ml-python')
AS
$$
import _snowflake
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, DateType, StringType, IntegerType
from snowflake.cortex import Complete

def get_order_info_from_email(session: snowpark.Session, email_content):

  prompt = f"""You are a bakery employee, reading customer emails asking for deliveries. \
    Please read the email at the end of this text and extract information about the ordered items.  \
    Format the information in CSV using the following columns: customer, order_date, delivery_date, item, and quantity. \
    Format the date as YYYY-MM-DD. If no year is given, assume the current year. \
    Use the current date in the format YYYY-MM-DD for the order date.  \
    Items should be in this list: [white loaf, rye loaf, baguette, bagel, croissant, chocolate muffin, blueberry muffin].  \
    The content of the email follows this line. \n {email_content}"""

  csv_output = """CUSTOMER,ORDER_DATE,DELIVERY_DATE,ITEM,QUANTITY
  Alice,2024-06-18,2024-06-20,bagel,3
  Bob,2024-06-18,2024-06-19,croissant,2"""

  
  schema = StructType([ 
        StructField("CUSTOMER", StringType(), False),  
        StructField("ORDER_DATE", DateType(), False),  
        StructField("DELIVERY_DATE", DateType(), False), 
        StructField("ITEM", StringType(), False),  
        StructField("QUANTITY", IntegerType(), False)
    ])

  orders_df = session.create_dataframe([x.split(',') for x in csv_output.split("\n")][1:], schema)
  orders_df.write.mode("append").save_as_table('COLLECTED_ORDERS_FROM_EMAIL')
    
  return orders_df
$$;

call READ_EMAIL_PROC('Please send 2 croissants and 1 rye loaf to Alice by June 20.');

select * from COLLECTED_ORDERS_FROM_EMAIL;