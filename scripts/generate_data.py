import pandas as pd
import numpy as np

def generate_sales_data(output_path="data/raw/sales_data.csv"):
    # Load real CPI data 
    cpi_df = pd.read_csv("data/raw/cpi_data.csv")
    cpi_df["periodName"] = pd.to_datetime(cpi_df["periodName"], format="%B").dt.month  # Convert month names to numbers
    cpi_dict = cpi_df.set_index(["year", "periodName"]).to_dict()["value"]

    # Define date range
    dates = pd.date_range(start="2020-01-01", end="2024-12-01", freq="MS")

    # Store IDs
    stores = range(101, 126)  # Store IDs 101 to 126

    # Department names
    departments = [
        "Produce", "Dairy", "Meat", "Seafood", "Grocery",
        "Non-food", "Liquor", "Floral", "Frozen", "Deli"
    ]

    # Define base sales averages per department
    base_sales = {
        "Produce": 20000, "Dairy": 15000, "Meat": 25000, "Seafood": 10000, "Grocery": 50000,
        "Non-food": 30000, "Liquor": 12000, "Floral": 5000, "Frozen": 10000, "Deli": 8000
    }

    # Base average units sold per department
    base_units = {
        "Produce": 5000, "Dairy": 4000, "Meat": 6000, "Seafood": 3000, "Grocery": 8000,
        "Non-food": 7000, "Liquor": 1200, "Floral": 1000, "Frozen": 3500, "Deli": 1500
    }

    # Growth rates per department (percent increase per month)
    growth_rates = {
        "Produce": 0.002, "Dairy": 0.0015, "Meat": 0.0025, "Seafood": 0.002, "Grocery": 0.003,
        "Non-food": 0.001, "Liquor": 0.0018, "Floral": 0.0022, "Frozen": 0.0015, "Deli": 0.002
    }

    # Seasonal multipliers per month (some departments peak in certain months)
    seasonality = {
        "Produce": [1.0, 1.0, 1.1, 1.1, 1.05, 1.0, 1.0, 1.0, 1.1, 1.2, 1.2, 1.3],  # Higher in Fall/Winter
        "Dairy": [1.0] * 12,  # Relatively stable
        "Meat": [1.1, 1.1, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0, 1.1, 1.2, 1.3],  # Summer & Winter peaks
        "Seafood": [1.0, 1.0, 1.1, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.2, 1.3, 1.4],  # Holiday spikes
        "Grocery": [1.05, 1.0, 1.0, 1.0, 1.0, 1.05, 1.1, 1.1, 1.1, 1.2, 1.3, 1.4],  # Q4 spike
        "Non-food": [1.0] * 12,  # Stable
        "Liquor": [1.0, 1.0, 1.0, 1.0, 1.2, 1.3, 1.2, 1.2, 1.1, 1.2, 1.3, 1.5],  # Big spike in December
        "Floral": [1.0, 1.5, 1.0, 1.0, 1.0, 1.2, 1.0, 1.0, 1.0, 1.1, 1.2, 1.3],  # February peak
        "Frozen": [1.0] * 12,  # Stable
        "Deli": [1.0] * 12  # Stable
    }

    # List to store generated data
    data_list = []

    # Loop through each month and store
    for date in dates:
        year, month = date.year, date.month
        cpi_multiplier = cpi_dict.get((year, month), 1.0) / cpi_dict.get((2020, 1), 1.0)  # Normalize CPI to 2020-01
        for store in stores:
            for dept in departments:
                # Retrieve base values
                avg_sales = base_sales[dept]
                avg_units = base_units[dept]
                
                # Apply growth rate for that department (compounding effect)
                months_since_start = (year - 2020) * 12 + (month - 1)
                growth_multiplier = (1 + growth_rates[dept]) ** months_since_start
                
                # Apply seasonality adjustment
                season_multiplier = seasonality[dept][month - 1]

                # Apply a random fluctuation (±20%)
                sales_multiplier = np.random.uniform(0.8, 1.2)
                units_multiplier = np.random.uniform(0.8, 1.2)

                # Calculate final sales and units
                total_sales = avg_sales * growth_multiplier * season_multiplier * sales_multiplier * cpi_multiplier
                units_sold = avg_units * growth_multiplier * season_multiplier * units_multiplier

                # Store data row
                row = {
                    "year": year,
                    "month": month,
                    "store_id": store,
                    "department": dept,
                    "total_sales": round(total_sales, 2),
                    "units_sold": int(units_sold)
                }
                data_list.append(row)

    # Convert to DataFrame and save
    df = pd.DataFrame(data_list)
    df = df.sort_values(by=["year", "month", "store_id", "department"])
    df.to_csv(output_path, index=False)
    print(f"Sales data generated and saved successfully to: {output_path}")

    return df  # Return DataFrame for testing

# Uncomment to run manually
generate_sales_data()
