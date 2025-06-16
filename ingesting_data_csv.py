from snowflake.snowpark import Session
from snowflake.snowpark.types import DateType, StringType, IntegerType, StructType, StructField, DecimalType
import json

source_file_name = '/Users/shashankmidididdi/Downloads/Orders_2023-07-07.csv'

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

#Listing 6.12
# put the file into the stage

result = my_session.file.put(source_file_name, "@orders_stage")

print(result)

#define schema for csv

schema_for_csv = StructType(
    [StructField("Customer", StringType()),
     StructField("Order date", DateType()),
     StructField("Delivery date", DateType()),
     StructField("Baked good type", StringType()),
     StructField("Quantity", DecimalType())]
)

#copy data from CSV file and store into staging table
#using session.read method
df = my_session.read.schema(schema_for_csv) .csv("@orders_stage")
result = df.copy_into_table("ORDERS_STG",
                            format_type_options={"skip_header":1})