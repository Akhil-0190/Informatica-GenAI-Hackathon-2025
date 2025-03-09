import snowflake.connector

# Function to create a Snowflake connection
def get_connection():
    return snowflake.connector.connect(
        user='',
        password='',
        account='',
        database='',
        schema=''
    )

