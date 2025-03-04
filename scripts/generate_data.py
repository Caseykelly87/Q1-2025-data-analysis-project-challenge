import pandas as pd
import numpy as np
import os

# ---------------------------- #
#        DATA LOADING          #
# ---------------------------- #
def load_cpi_data(cpi_path="data/raw/cpi_data.csv", start_year=2020, end_year=2024):
    # Loads and processes CPI data, returning a dictionary {(year, month): cpi_value}

    cpi_df = pd.read_csv(cpi_path)

    # Convert 'periodName' to month numbers
    cpi_df["month"] = pd.to_datetime(cpi_df["periodName"], format="%B").dt.month

    # Create a dictionary of (year, month): value
    cpi_dict = {(row["year"], row["month"]): row["value"] for _, row in cpi_df.iterrows()}

    # Check for missing CPI values
    for year in cpi_df["year"].unique():
        for month in range(1, 13):
            if (year, month) not in cpi_dict:
                raise KeyError(f"Missing CPI data for ({year}, {month})")

    return cpi_dict

# ---------------------------- #
#      DATA GENERATION         #
# ---------------------------- #
def generate_sales_data(cpi_dict, start_year=2020, end_year=2024, stores=range(101, 126)):
    # Generates synthetic sales data based on store, department, seasonality, and inflation adjustments.
    
    # Define date range
    dates = pd.date_range(start=f"{start_year}-01-01", end=f"{end_year}-12-01", freq="MS")

    # Define department data
    departments, base_sales, base_units, growth_rates, seasonality = get_department_data()

    # Store generated data
    data_list = []

    # Loop through each month and store
    for date in dates:
        year, month = date.year, date.month
        cpi_multiplier = cpi_dict.get((year, month), 1.0)

        for store in stores:
            for dept in departments:
                total_sales, units_sold = calculate_sales_units(year, month, dept, base_sales, base_units, growth_rates, seasonality, cpi_multiplier)

                # Store data row
                data_list.append({
                    "year": year,
                    "month": month,
                    "store_id": store,
                    "department": dept,
                    "total_sales": round(total_sales, 2),
                    "units_sold": int(units_sold)
                })

    return pd.DataFrame(data_list)

# ---------------------------- #
#     HELPER FUNCTIONS         #
# ---------------------------- #
def get_department_data():
    # Returns department names, base sales, base units, growth rates, and seasonality multipliers.
    departments = ["Produce", "Dairy", "Meat", "Seafood", "Grocery", "Non-food", "Liquor", "Floral", "Frozen", "Deli"]

    base_sales = {
        "Produce": 20000, "Dairy": 15000, "Meat": 25000, "Seafood": 10000, "Grocery": 50000,
        "Non-food": 30000, "Liquor": 12000, "Floral": 5000, "Frozen": 10000, "Deli": 8000
    }

    base_units = {
        "Produce": 5000, "Dairy": 4000, "Meat": 6000, "Seafood": 3000, "Grocery": 8000,
        "Non-food": 7000, "Liquor": 1200, "Floral": 1000, "Frozen": 3500, "Deli": 1500
    }

    growth_rates = {
        "Produce": 0.002, "Dairy": 0.0015, "Meat": 0.0025, "Seafood": 0.002, "Grocery": 0.003,
        "Non-food": 0.001, "Liquor": 0.0018, "Floral": 0.0022, "Frozen": 0.0015, "Deli": 0.002
    }

    seasonality = {
        "Produce": [1.0, 1.0, 1.1, 1.1, 1.05, 1.0, 1.0, 1.0, 1.1, 1.2, 1.2, 1.3],
        "Dairy": [1.0] * 12,
        "Meat": [1.1, 1.1, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0, 1.1, 1.2, 1.3],
        "Seafood": [1.0, 1.0, 1.1, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.2, 1.3, 1.4],
        "Grocery": [1.05, 1.0, 1.0, 1.0, 1.0, 1.05, 1.1, 1.1, 1.1, 1.2, 1.3, 1.4],
        "Non-food": [1.0] * 12,
        "Liquor": [1.0, 1.0, 1.0, 1.0, 1.2, 1.3, 1.2, 1.2, 1.1, 1.2, 1.3, 1.5],
        "Floral": [1.0, 1.5, 1.0, 1.0, 1.0, 1.2, 1.0, 1.0, 1.0, 1.1, 1.2, 1.3],
        "Frozen": [1.0] * 12,
        "Deli": [1.0] * 12
    }

    return departments, base_sales, base_units, growth_rates, seasonality

def calculate_sales_units(year, month, dept, base_sales, base_units, growth_rates, seasonality, cpi_multiplier):
    #  Calculates total sales and units sold based on department trends, seasonality, and inflation.
    months_since_start = (year - 2020) * 12 + (month - 1)
    growth_multiplier = (1 + growth_rates[dept]) ** months_since_start
    season_multiplier = seasonality[dept][month - 1]
    
    # Apply random fluctuation
    sales_multiplier = np.random.uniform(0.8, 1.2)
    units_multiplier = np.random.uniform(0.8, 1.2)

    total_sales = base_sales[dept] * growth_multiplier * season_multiplier * sales_multiplier * cpi_multiplier
    # print(f"Department: {dept}, Total Sales: {total_sales}, CPI Multiplier: {cpi_multiplier}")
    units_sold = base_units[dept] * growth_multiplier * season_multiplier * units_multiplier

    return total_sales, units_sold

# ---------------------------- #
#        DATA EXPORTING        #
# ---------------------------- #
def save_sales_data(df, output_path="data/raw/sales_data.csv"):
    # Saves the generated sales data to a CSV file.
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = df.sort_values(by=["year", "month", "store_id", "department"])
    df.to_csv(output_path, index=False)
    print(f"Sales data saved to: {output_path}")

# ---------------------------- #
#      MAIN FUNCTION CALL      #
# ---------------------------- #
def main():
    # Main function to generate and save sales data.
    cpi_dict = load_cpi_data()
    df = generate_sales_data(cpi_dict)
    save_sales_data(df)

if __name__ == "__main__":
    main()












"""def generate_sales_data(output_path="data/raw/sales_data.csv"):
import pandas as pd
import numpy as np


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
generate_sales_data()"""
