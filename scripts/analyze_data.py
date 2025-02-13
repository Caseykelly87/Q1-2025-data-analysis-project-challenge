import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
import scipy.stats as stats

# Load merged data
df = pd.read_csv("data/processed/merged_data.csv")

print(df.info())  # Shows column data types

print(df.isnull().sum())  # Checks for missing values

# Summary statistics of sales
print(df["total_sales"].describe())

# Total sales by department

department_sales = df.groupby("department")["total_sales"].sum().sort_values(ascending=False)
print(department_sales.to_frame())  # Convert to DataFrame for better readability


# Average CPI per year
cpi_by_year = df.groupby("year")["CPI_U"].mean()
print(cpi_by_year)

#print(df[['year', 'month', 'CPI_U']].drop_duplicates().head(12))
print(df[['year', 'month', 'department', 'total_sales']].head(12))

#   DATA VISUALIZATION

# Define correct month order
month_order = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Apply categorical ordering
df["month"] = pd.Categorical(df["month"], categories=month_order, ordered=True)

# Aggregate monthly sales data
monthly_sales = df.groupby(["year", "month"])["total_sales"].sum().reset_index()

# Plot
plt.figure(figsize=(12, 6))
sns.lineplot(data=monthly_sales, x="month", y="total_sales", hue="year", marker="o")
plt.xticks(rotation=45)
plt.title("Monthly Sales Trends")
print('Close Monthly Sales Trends to view Time Series Analysis')
plt.show()

#   Time Series Analysis 

# Ensure data is sorted
df["date"] = pd.to_datetime(df["year"].astype(str) + " " + df["month"].astype(str), format="%Y %B")
df = df.sort_values("date")

# Aggregate sales per month
sales_trend = df.groupby("date")["total_sales"].sum()

# Decompose time series
decomposition = seasonal_decompose(sales_trend, model="additive", period=12)
decomposition.plot()
print('Close Time Series Analysis to view Correlation Matrix')
plt.show()

#   Correlation & Inflation Impact on Sales

# Correlation matrix
corr = df[["total_sales", "CPI_U"]].corr()
sns.heatmap(corr, annot=True, cmap="coolwarm")
plt.title("Correlation Between CPI & Sales")
print('Close Correlation Matrix to print Pearson Correlation')
plt.show()

# Pearson Correlation
corr_value, p_value = stats.pearsonr(df["total_sales"], df["CPI_U"])
print(f"Pearson Correlation: {corr_value}, P-value: {p_value}")



