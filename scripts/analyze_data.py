import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
import scipy.stats as stats

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
DATA_FILEPATH = "data/processed/merged_data.csv"

def load_data(filepath):
    """Load merged data from CSV file."""
    try:
        df = pd.read_csv(filepath)
        logging.info(f"Data loaded successfully from {filepath}")
        return df
    except FileNotFoundError:
        logging.error(f"The file {filepath} was not found.")
        return None
    except pd.errors.EmptyDataError:
        logging.error("No data found in the file.")
        return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

def display_data_info(df):
    """Display information and summary of the DataFrame."""
    if df is not None:
        logging.info("Displaying data information and summary")
        print(df.info())
        print(df.isnull().sum())
        print(df["total_sales"].describe())

def get_department_sales(df):
    """Calculate total sales by department."""
    if df is not None:
        logging.info("Calculating total sales by department")
        department_sales = df.groupby("department")["total_sales"].sum().sort_values(ascending=False)
        print(department_sales.to_frame())  # Convert to DataFrame for better readability

def get_cpi_by_year(df):
    """Calculate average CPI per year."""
    if df is not None:
        logging.info("Calculating average CPI per year")
        cpi_by_year = df.groupby("year")["CPI_U"].mean()
        print(cpi_by_year)

def display_sample_data(df):
    """Display sample data for verification."""
    if df is not None:
        logging.info("Displaying sample data")
        print(df[['year', 'month', 'department', 'total_sales']].head(12))

def plot_monthly_sales_trends(df):
    """Plot monthly sales trends."""
    if df is not None:
        logging.info("Plotting monthly sales trends")
        
        # Define correct month order
        month_order = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        
        # Apply categorical ordering to month
        df["month"] = pd.Categorical(df["month"], categories=month_order, ordered=True)
        
        # Ensure 'year' is treated as a string for categorical plotting
        df["year"] = df["year"].astype(str)
        
        # Ensure 'total_sales' is numerical
        df["total_sales"] = pd.to_numeric(df["total_sales"], errors='coerce')
        
        # Aggregate monthly sales data
        monthly_sales = df.groupby(["year", "month"], observed=False)["total_sales"].sum().reset_index()
        
        # Convert 'year' and 'month' to categorical for plotting
        monthly_sales["year"] = pd.Categorical(monthly_sales["year"], ordered=True)
        monthly_sales["month"] = pd.Categorical(monthly_sales["month"], categories=month_order, ordered=True)
        
        # Print data types and head to inspect the data
        logging.info(f"Data types in monthly_sales:\n{monthly_sales.dtypes}")
        logging.info(f"Head of monthly_sales:\n{monthly_sales.head()}")
        
        # Plot
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=monthly_sales, x="month", y="total_sales", hue="year", marker="o")
        plt.xticks(rotation=45)
        plt.title("Monthly Sales Trends")
        plt.show()


def perform_time_series_analysis(df):
    """Perform time series analysis on sales data."""
    if df is not None:
        logging.info("Performing time series analysis on sales data")
        df["date"] = pd.to_datetime(df["year"].astype(str) + " " + df["month"].astype(str), format="%Y %B")
        df = df.sort_values("date")
        sales_trend = df.groupby("date")["total_sales"].sum()
        decomposition = seasonal_decompose(sales_trend, model="additive", period=12)
        decomposition.plot()
        plt.show()

def plot_correlation_matrix(df):
    """Plot correlation matrix between CPI and sales."""
    if df is not None:
        logging.info("Plotting correlation matrix between CPI and sales")
        corr = df[["total_sales", "CPI_U"]].corr()
        sns.heatmap(corr, annot=True, cmap="coolwarm")
        plt.title("Correlation Between CPI & Sales")
        plt.show()

def calculate_pearson_correlation(df):
    """Calculate Pearson correlation between total sales and CPI."""
    if df is not None:
        logging.info("Calculating Pearson correlation between total sales and CPI")
        corr_value, p_value = stats.pearsonr(df["total_sales"], df["CPI_U"])
        logging.info(f"Pearson Correlation: {corr_value}, P-value: {p_value}")

def main():
    # Load data
    df = load_data(DATA_FILEPATH)

    # Display information
    display_data_info(df)

    # Calculate and display department sales
    get_department_sales(df)

    # Calculate and display average CPI per year
    get_cpi_by_year(df)

    # Display sample data for verification
    display_sample_data(df)

    # Plot monthly sales trends
    plot_monthly_sales_trends(df)

    # Perform time series analysis
    perform_time_series_analysis(df)

    # Plot correlation matrix
    plot_correlation_matrix(df)

    # Calculate Pearson correlation
    calculate_pearson_correlation(df)

if __name__ == "__main__":
    main()



