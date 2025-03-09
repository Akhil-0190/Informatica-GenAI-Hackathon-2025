import pandas as pd
import random
from datetime import datetime, timedelta

# Constants
resources = ["Water Tank", "Fuel", "Electricity", "Food Supplies", "Medical Kits"]
market_trends = ["increasing", "decreasing", "stable"]
weather_conditions = ["Sunny", "Rainy", "Cloudy", "Stormy"]
update_days = 1 # Number of days to append data for

# Function to update and append data to the existing file
def append_data_to_existing_file(existing_file):
    try:
        # Load the existing data
        existing_data = pd.read_csv(existing_file)
        existing_data["Timestamp"] = pd.to_datetime(existing_data["Timestamp"], format="%d-%m-%Y %H:%M:%S")
    except FileNotFoundError:
        print(f"File '{existing_file}' not found. Please provide a valid file.")
        return
    
    new_data = []
    for resource in resources:
        # Get the last record for the resource
        resource_data = existing_data[existing_data["Resource"] == resource]
        if resource_data.empty:
            print(f"No existing data for resource '{resource}'. Skipping.")
            continue
        last_record = resource_data.iloc[-1]

        # Initialize values from the last record
        timestamp = last_record["Timestamp"]
        stock_level = last_record["Stock_Level"]

        for _ in range(update_days):
            # Determine market trend and weather
            market_trend = random.choice(market_trends)
            weather = random.choice(weather_conditions)

            # Adjust usage rate based on weather and market trend
            if resource == "Water Tank":
                usage_rate = random.uniform(80, 150) if weather == "Sunny" else random.uniform(50, 100)
                if market_trend == "increasing":
                    usage_rate *= 0.9  # Conservative usage
            elif resource == "Fuel":
                usage_rate = random.uniform(100, 200) if weather == "Stormy" else random.uniform(50, 120)
                if market_trend == "decreasing":
                    usage_rate *= 1.1  # Increased usage
            elif resource == "Electricity":
                usage_rate = random.uniform(100, 300) if weather in ["Sunny", "Cloudy"] else random.uniform(80, 150)
            elif resource == "Food Supplies":
                usage_rate = random.uniform(20, 60)
            elif resource == "Medical Kits":
                usage_rate = random.uniform(2, 8)
                if market_trend == "increasing":
                    usage_rate *= 1.2  # Emergency scenario

            # Update stock level
            stock_level -= usage_rate

            # Append new record
            new_data.append({
                "Timestamp": (timestamp + timedelta(days=1)).strftime("%d-%m-%Y %H:%M:%S"),
                "Resource": resource,
                "Stock_Level": max(0, round(stock_level, 2)),  # Ensure non-negative stock
                "Usage_Rate": round(usage_rate, 2),
                "Market_Trend": market_trend,
                "Weather": weather,
            })

            # Increment timestamp
            timestamp += timedelta(days=1)

    # Convert new data to DataFrame and append to the file
    new_data_df = pd.DataFrame(new_data)
    new_data_df.to_csv(existing_file, mode="a", header=False, index=False)
    print(f"Appended {len(new_data_df)} new records to '{existing_file}'.")

# Run the script
existing_file = "D:\\college\\informatica2025\\resource_data.csv"  # Existing data file
append_data_to_existing_file(existing_file)
