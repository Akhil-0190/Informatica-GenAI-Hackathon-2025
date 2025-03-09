import json
import os
import pandas as pd
from sqlalchemy import create_engine
from snowflake_config import get_connection
from datetime import datetime
from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI

# Set Azure OpenAI API details as environment variables
os.environ["AZURE_OPENAI_API_KEY"] = "FHYHsFTFTpFpVN8skKUc0KMIc1uR0ID5V89xABmEr8w5E4ss5fGvJQQJ99BCAC77bzfXJ3w3AAABACOGN0Uj"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://miniproject123.openai.azure.com/"

# Function to convert Snowflake connection to SQLAlchemy engine
def get_connection_engine():
    conn = get_connection()
    engine = create_engine("snowflake://", creator=lambda: conn)
    return engine

# Initialize LangChain SQLDatabase
def initialize_database():
    engine = get_connection_engine()
    db = SQLDatabase(engine)
    return db

# Function to query Snowflake for data
def fetch_data():
    conn = get_connection()
    query = """
        SELECT TIMESTAMP, RESOURCE, STOCK_LEVEL, USAGE_RATE, MARKET_TREND, WEATHER, DEPLETION_RATE 
        FROM public.resource_data;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Function to perform AI analysis for replenishment, anomalies, and forecast
def ai_analysis(db, row):
    query = f"""
    SELECT TIMESTAMP, STOCK_LEVEL, USAGE_RATE, MARKET_TREND, WEATHER
    FROM public.resource_data
    WHERE RESOURCE = '{row['RESOURCE']}'
    ORDER BY TIMESTAMP DESC
    LIMIT 3;
    """
    try:
        # Fetch historical data for context
        historical_context = db.run(query)

        # AI prompt
        messages = [
            (
                "system",
                "You are an AI assistant performing resource forecasting, replenishment analysis, and anomaly detection. Always respond in valid JSON format."
            ),
            (
                "user",
                f"""
                Analyze the resource data for forecasting, replenishment, and anomalies:
                Historical Context:
                {historical_context}

                Current Data:
                - Resource: {row['RESOURCE']}
                - Stock: {row['STOCK_LEVEL']} units
                - Usage rate: {row['USAGE_RATE']} units/day
                - Market trend: {row['MARKET_TREND']}
                - Weather: {row['WEATHER']}
                - Depletion timeline: {row['DEPLETION_RATE']} days

                Instructions:
                1. Generate a 1-2 line forecast, analyzing trends to anticipate future needs.
                2. Determine if replenishment is required. If yes, specify the amount.
                3. Identify any anomalies in the latest current data based on historical context.
                Format your response strictly as valid JSON:
                {{
                    "Forecast": "<Forecast>",
                    "Replenishment": {{
                        "Required": "<Yes/No>",
                        "Amount": <Amount or None>
                    }},
                    "Anomalies": "<Details or None>"
                }}
                """
            ),
        ]

        # Initialize AzureChatOpenAI
        llm = AzureChatOpenAI(
            azure_deployment="gpt-4",
            api_version="2023-03-15-preview",
            temperature=0,
        )
        # Generate AI response
        response = llm.invoke(messages)
        print(f"AI Response for {row['RESOURCE']}:", response.content)

        # Parse AI response
        ai_response = json.loads(response.content)
        forecast = ai_response.get("Forecast", "No forecast available")
        replenishment = ai_response.get("Replenishment", {"Required": "No", "Amount": 0})
        anomalies = ai_response.get("Anomalies", "None")
        return forecast, replenishment, anomalies
    except Exception as e:
        print(f"Error during AI analysis: {e}")
        return "Error: Forecast unavailable", {"Required": "No", "Amount": 0}, None

# Log forecast to AI_FORECASTS table
def log_forecast(resource, forecast):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS AI_FORECASTS (
                RESOURCE VARCHAR(255),
                FORECAST VARCHAR(1000),
                GENERATED_AT TIMESTAMP_NTZ
            )
        """)
        cursor.execute("""
            INSERT INTO AI_FORECASTS (RESOURCE, FORECAST, GENERATED_AT)
            VALUES (%s, %s, %s)
        """, (resource, forecast, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print(f"Forecast logged for resource: {resource}, forecast: {forecast}")
    except Exception as e:
        print(f"Error logging forecast: {e}")
    finally:
        cursor.close()
        conn.close()

# Log replenishment actions and update stock levels in resource_data
def log_replenishment(resource, replenished_stock):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Create the replenishment log table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS REPLENISHMENT_LOG (
                RESOURCE VARCHAR(255),
                REPLENISHED_STOCK INT,
                ACTION_TIMESTAMP TIMESTAMP_NTZ
            )
        """)

        # Insert the replenishment action into the log
        cursor.execute("""
            INSERT INTO REPLENISHMENT_LOG (RESOURCE, REPLENISHED_STOCK, ACTION_TIMESTAMP)
            VALUES (%s, %s, %s)
        """, (resource, replenished_stock, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        print(f"Replenishment logged for resource: {resource}, stock added: {replenished_stock}")

        # Fetch the latest stock level for the resource
        cursor.execute(f"""
            SELECT STOCK_LEVEL, USAGE_RATE, MARKET_TREND, WEATHER
            FROM public.resource_data
            WHERE RESOURCE = %s
            ORDER BY TIMESTAMP DESC
            LIMIT 1
        """, (resource,))
        latest_entry = cursor.fetchone()

        if latest_entry:
            current_stock_level, usage_rate, market_trend, weather = latest_entry
            new_stock_level = current_stock_level + replenished_stock
            
            depletion_rate = new_stock_level / usage_rate

            # Insert the updated stock level into resource_data as a new entry
            cursor.execute("""
                INSERT INTO public.resource_data (
                    TIMESTAMP, RESOURCE, STOCK_LEVEL, USAGE_RATE, MARKET_TREND, WEATHER, DEPLETION_RATE
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                resource,
                new_stock_level,
                usage_rate,  # Carry over the same usage rate
                market_trend,  # Carry over the same market trend
                weather,  # Carry over the same weather
                depletion_rate  # Depletion rate can be recalculated later
            ))

            print(f"Updated stock level for {resource}: {new_stock_level}")

    except Exception as e:
        print(f"Error logging replenishment: {e}")
    finally:
        cursor.close()
        conn.close()

# Log anomalies
def log_anomaly(resource, anomaly_details):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ANOMALY_LOG (
                RESOURCE VARCHAR(255),
                ANOMALY_DETAILS VARCHAR(1000),
                DETECTED_AT TIMESTAMP_NTZ
            )
        """)
        cursor.execute("""
            INSERT INTO ANOMALY_LOG (RESOURCE, ANOMALY_DETAILS, DETECTED_AT)
            VALUES (%s, %s, %s)
        """, (resource, anomaly_details, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print(f"Anomaly logged for resource: {resource}, details: {anomaly_details}")
    except Exception as e:
        print(f"Error logging anomaly: {e}")
    finally:
        cursor.close()
        conn.close()

# Main function
def main():
    data = fetch_data()
    latest_data = data.sort_values(by='TIMESTAMP').groupby('RESOURCE').tail(1)
    print("Latest entries for each resource:")
    print(latest_data)

    db = initialize_database()

    for _, row in latest_data.iterrows():
        forecast, replenishment, anomalies = ai_analysis(db, row)

        # Log forecast
        log_forecast(row['RESOURCE'], forecast)

        # Handle replenishment
        if replenishment["Required"] == "Yes":
            replenished_stock = int(replenishment["Amount"])  # Use the amount from AI response
            log_replenishment(row['RESOURCE'], replenished_stock)

        # Handle anomalies
        if anomalies and anomalies != "None":
            log_anomaly(row['RESOURCE'], anomalies)

if __name__ == "__main__":
    main()
