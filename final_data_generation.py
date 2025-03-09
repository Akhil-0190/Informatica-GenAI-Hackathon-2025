import pandas as pd
import random
from datetime import datetime, timedelta

# Constants
resources = ["Water Tank", "Fuel", "Electricity", "Food Supplies", "Medical Kits"]
market_trends = ["increasing", "decreasing", "stable"]
weather_conditions = ["Sunny", "Rainy", "Cloudy", "Stormy"]
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 31)  # One month of data

# Function to generate realistic mock data
def generate_realistic_mock_data():
    data = []
    for resource in resources:
        timestamp = start_date
        stock_level = random.randint(2000, 5000)  # Initial stock level
        for _ in range((end_date - start_date).days + 1):
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
            if stock_level < 500:  # Trigger replenishment
                stock_level += random.randint(2000, 4000)

            # Append record
            data.append({
                "Timestamp": timestamp.strftime("%d-%m-%Y %H:%M:%S"),
                "Resource": resource,
                "Stock_Level": max(0, round(stock_level, 2)),  # Ensure non-negative stock
                "Usage_Rate": round(usage_rate, 2),
                "Market_Trend": market_trend,
                "Weather": weather,
            })

            # Increment timestamp
            timestamp += timedelta(days=1)

    return pd.DataFrame(data)

# Generate and save mock data
mock_data = generate_realistic_mock_data()
mock_data.to_csv("final_critical_resource_data.csv", index=False)
print("Final mock data saved as 'final_critical_resource_data.csv'.")
