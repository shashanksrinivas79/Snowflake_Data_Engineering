from snowflake.snowpark import Session

connection_parameters_dict = {
    "account": "HWQABRS-ZK49812",
    "user": "SHASHANK",
    "password": "Shashankmidididdi@2204",
    "role": "SYSADMIN",
    "warehouse": "BAKERY_WH",
    "database": "BAKERY_DB",
    "schema": "SNOWPARK"
}

my_session = Session.builder.configs(connection_parameters_dict).create()

print("✅ Session created successfully")
my_session.close()