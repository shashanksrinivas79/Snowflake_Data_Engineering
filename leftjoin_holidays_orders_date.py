from snowflake.snowpark import Session
from snowflake.snowpark.types import DateType, StringType, IntegerType, StructType, StructField, DecimalType
import json

credentials = json.load(open('connection_parameters.json'))

connection_parameters_dict = {
    "account": credentials ["account"],
    "user": credentials["user"],
    "password": credentials["password"],
    "role": credentials["role"],
    "warehouse": credentials["warehouse"],
    "database": credentials["database"],
    "schema": credentials["schema"]
}

my_session = Session.builder.configs(connection_parameters_dict).create()

df_orders = my_session.table("ORDERS_STG")
df_dim_date = my_session.table("DIM_DATE")

df_orders_with_holiday_flag = df_orders.join(
    df_dim_date,
    df_orders.delivery_date == df_dim_date.day,
    'left'
)

df_orders_with_holiday_flag.create_or_replace_view("ORDERS_HOLIDAY_FLG")

my_session.close()