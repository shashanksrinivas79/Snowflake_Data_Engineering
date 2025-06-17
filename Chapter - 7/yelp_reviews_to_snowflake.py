import requests
from snowflake.snowpark import Session
from snowflake.snowpark.types import DateType, StringType, IntegerType, StructType, StructField, DecimalType
import json
from snowflake.snowpark.functions import udf

YELP_API_KEY = "Jxy_X1-UnwnwPRoZCHovEi6M6nK9ZvivGA921ZkPvqHvkAbW8db4v5m3RYrDTu4Ysfc1YEWATXeICSvveQBcBPXN-eEhTwxEQQvXeguoeZ-DoCsxbfACCzbEh7NRaHYx"
headers = {"Authorization": f"Bearer {YELP_API_KEY}"}

# Replace this with your target restaurant and location

search_params = {
    "term": "Farine Bakery & Cafe",
    "location": "Redmond, WA",
    "limit": 1
}

# Get the business ID for the restaurant
biz_resp = requests.get("https://api.yelp.com/v3/businesses/search", headers=headers, params=search_params)
biz = biz_resp.json()["businesses"][0]
business_id = biz["id"]
business_name = biz["name"]


# ----------------------
# 2. Get top 3 reviews for the restaurant
# ----------------------
review_resp = requests.get(f"https://api.yelp.com/v3/businesses/{business_id}/reviews", headers=headers)
reviews = review_resp.json().get("reviews", [])

review_data = [
    (business_id, business_name, r["user"]["name"], r["text"], r["rating"])
    for r in reviews
]

# ----------------------
# 3. Connect to Snowflake
# ----------------------

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

# ----------------------
# 4. Create DataFrame and define UDF
# ----------------------
schema = StructType([
    StructField("business_id", StringType()),
    StructField("business_name", StringType()),
    StructField("user_name", StringType()),
    StructField("review_text", StringType()),
    StructField("rating", IntegerType())
])

df = my_session.create_dataframe(review_data, schema=schema)

#creating a user defines function

@udf(name="tag_sentiment", replace=True)
def tag_sentiment(text: str) -> str:
    text = text.lower()
    if "bad" in text or "terrible" in text:
        return "negative"
    elif "great" in text or "excellent" in text:
        return "positive"
    else:
        return "neutral"
    
df_with_sentiment = df.with_column("sentiment", tag_sentiment(df["review_text"]))

# ----------------------
# 5. Save to Snowflake
# ----------------------
df_with_sentiment.write.save_as_table("YELP_SINGLE_RESTAURANT_REVIEWS", mode="overwrite")

print("✅ Top 3 reviews saved in Snowflake table: YELP_SINGLE_RESTAURANT_REVIEWS")

my_session.close()