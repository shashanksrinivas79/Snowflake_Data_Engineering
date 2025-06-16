#import session from smowflake.snowpark package
from snowflake.snowpark import Session
#import datatypes from smowflake.snowpark package
from snowflake.snowpark.types import StructType, StructField, DateType, BooleanType
#import json package to load json files
import json
#import date and timedelta from datetime package fot generating date and time
from datetime import date, timedelta
#import holidays package to determine the given date is holiday or not
import holidays
from holidays import country_holidays

# define function that returns true if p_date is hoilday in p_country

def is_holiday(p_date, p_country):
    #get list of all hoildays in country
    all_holidays = holidays.country_holidays(p_country)
    #return true if p_date is hoilday in p_country
    if p_date in all_holidays:
        return True
    else:
        return False
    

#generate a list of dates dtarting from start_date followed by as many dates
#as defined in no_days variable
#define start date

start_dt = date(2023, 1, 1)
#define number of days
no_days = 5
#storing consecutive dates starting from the start date in list
dates = [(start_dt + timedelta(days=i)).isoformat()
         for i in range(no_days)]


#create a list of lists that combines list of dates
#with output of is_holiday() function
hoilday_flags = [[d, is_holiday(d, 'US')] for d in dates]

print(hoilday_flags)

credentials = json.load(open('connection_parameters.json'))
# create a dictionary with the connection parameters
connection_parameters_dict = {
    "account": credentials["account"],
    "user": credentials["user"],
    "password": credentials["password"],
    "role": credentials["role"],
    "warehouse": credentials["warehouse"],
    "database": credentials["database"],
    "schema": credentials["schema"] 
}  

# create a session object for the Snowpark session
my_session = Session.builder.configs(connection_parameters_dict).create()

df = my_session.create_dataframe(
    hoilday_flags,
    schema=StructType(
        [StructField("day", DateType()),
         StructField("holiday_flg", BooleanType())]
    )
)

print(df.collect())

#save the date frame to a snowflake table DIM_DATE and overwrite the table if it already exits

df.write.mode("overwrite").save_as_table("DIM_DATE")

my_session.close()