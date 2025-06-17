from snowflake.snowpark import Session
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

print("✅ Session created successfully")

ts = my_session.sql("select current_timestamp()").collect()
print(ts)
my_session.close()