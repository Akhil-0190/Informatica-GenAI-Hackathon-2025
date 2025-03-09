import snowflake.connector

# Function to create a Snowflake connection
def get_connection():
    return snowflake.connector.connect(
        user='Akhil019',
        password='Snowflake@1234',
        account='RLWGPKG-BG25603',
        database='RESOURCE_DB',
        schema='PUBLIC'
    )

