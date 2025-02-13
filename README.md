## ************  CONFIGURATION  ************ ##

This project uses a **Python virtual environment** to manage dependencies! This **venv** file is included in the .gitignore and will need to be created and activated manually BEFORE requirements.txt install.
## Python Virtual Environments Documention "https://docs.python.org/3/library/venv.html"


## Requires registration for a Bureau of Labor Statistics version 2 API KEY at "https://data.bls.gov/registrationEngine/"

This project uses **".env" & Environment Variables** to store sensitive information such as API keys to mimic real-world applications! This file is included in the .gitignore and will require creation. 
NOTE: Confiuration requires only that you create a file in project root named **.env** to store your **BLS_API_KEY=XXXXXXXX "YOUR KEY" XXXXXXXXXXXX** variable!
## Python-dotnet Documention "https://github.com/theskumar/python-dotenv"


## *************** PROJECT ***************** ##

Still in development, currently functions by running the following scripts individually in order. Future plans include creating unit tests before expanding the datasets used for this analysis. Streamlining workflow by automating the process into one executable main.py script, ensuring that the output is pre-processed and structured appropriately for generation of .twb files for direct integration with Tableau.

**collect_data.py** retrieves Consumer Price Index data for all urban consumers, years 2020-2024 using the BLS API **APIKEY Required**

**generate_data.py** utilizes the Consumer Price Index data retrieved by collect_data.py as a reference to SIMULATE data patterns that closely resemble real-world economic trends. Monthly sales totals, years 2020-2024 for 25 stores are generated broken down by department.

**process_data** merges the cpi and sales data sets while enhancing readablity and eliminating redundancey before generating a conslidataed CSV of the processed data.

**analyze_data.py** performs analysis in thee key areas: exploratory data, time series, and correlation. Prints various statistics to the console, aggregates and plots data for visualizations.




                                                          ORIGINAL ASSIGNMENT


Below is a **Data Analysis project** outline designed to introduce **beginner-level** concepts in data manipulation (using SQL, Pandas, or Excel) and **data visualization** (using Tableau or Power BI). It also includes **optional, more advanced** requirements for those who want to challenge themselves further. The scenario involves performing a **market analysis** of publicly available grocery data in the context of recent inflation trends and using verified sales data to provide strategic recommendations to an executive committee at a major grocery chain.

---

## 1. Project Overview

### Scenario

You are a **Data Analyst** working for **Galaxy Grocers**, a major grocery chain. Executive leadership has requested a **market analysis** that examines:

1. **Publicly available grocery market data**,
2. **Recent inflation trends**, and
3. **Verified sales data** (e.g., your chain’s sales data or aggregated industry sales data).

You’ll perform **data manipulation** and **visualization** to:

- Identify **key product lines** to promote,
- Pinpoint areas of the business likely to become **less profitable** over time, and
- Highlight **high margin** areas of the business to **expand**.

You’ll then compile **actionable recommendations** for an executive committee.

---

## 2. Project Goals & Learning Objectives

1. **Practice Data Manipulation**:
    
    - Use **SQL**, **Pandas**, or **Excel** to clean, transform, and merge datasets.
2. **Create Data Visualizations**:
    
    - Build charts/dashboards in **Tableau** or **Power BI**.
3. **Analyze Market & Economic Trends**:
    
    - Incorporate inflation data to assess how rising costs may impact sales and profits.
4. **Provide Executive-Level Recommendations**:
    
    - Summarize findings with clear business insights and strategic direction.
5. **(Optional Advanced)**:
    
    - Perform advanced analytics to forecast future market or sales trends.

---

## 3. Data Requirements & Sources

To keep this beginner-friendly, select or simulate data sets that are either small or medium in size. If you want to make it more advanced, you can expand the variety and volume of data.

1. **Grocery Market Data**
    
    - Could be from publicly available datasets like USDA or other government sites that track commodity prices and market information.
    - Example columns might include: _Product Category, Time Period, Average Price, Demand Index_.
2. **Inflation Trend Data**
    
    - Could be from government or third-party economic data (e.g., CPI data from the U.S. Bureau of Labor Statistics).
    - Example columns might include: _Date, CPI (Consumer Price Index), Inflation Rate_.
3. **Verified Grocery Sales Data**
    
    - Real or simulated sales data for **Galaxy Grocers** or from publicly available retail data.
    - Example columns might include: _Transaction Date, Product Name, Quantity Sold, Revenue, Profit Margin, Store Location_.
4. **(Optional Advanced)**:
    
    - **Demographic or Regional Data** (population density, household income, etc.) to deepen insights.
    - Additional historical data going back multiple years for time-series analysis.

---

## 4. Project Steps

### Step 1: Data Collection & Setup

1. **Acquire the data**: Download or gather the **grocery market data**, **inflation trend data**, and **sales data**.
2. **Store the data** in a location accessible to your chosen toolset:
    - If using **SQL**, import to a relational database (e.g., MySQL, PostgreSQL, or a local sqlite database).
    - If using **Pandas**, store as .csv files in a folder, then load them into dataframes.
    - If using **Excel**, keep them in separate worksheets or separate .xlsx files.

### Step 2: Data Cleaning & Preparation

1. **Check for missing values and outliers**:
    
    - In **SQL**, use queries to find rows with NULL values or extreme outliers.
    - In **Pandas**, use functions like `df.isnull().sum()`, `df.describe()`, etc.
    - In **Excel**, apply filters and conditional formatting to spot anomalies.
2. **Clean or remove** problematic data points where appropriate. Keep a record of the decisions you make here so you can include relevant details in your final executive report.
    
3. **Merge or join** datasets where necessary:
    
    - E.g., combine the inflation data with time-series grocery data on matching _Date/Month/Year_ fields.
    - Combine sales data with product categories or average price data using product IDs or product names.
4. **Create any needed calculated fields**:
    
    - For example, an _inflation-adjusted cost_ or _inflation-adjusted revenue_ column, or a _profit margin_ calculation if it’s not already provided.

### Step 3: Exploratory Data Analysis (EDA)

1. **Generate summary statistics**:
    
    - Means, medians, standard deviations, min/max, etc.
2. **Look at distributions**:
    
    - For continuous variables like price, inflation rates, or sales volume.
3. **Perform grouping and aggregations**:
    
    - Total sales or revenue by _Product Category_, _Store Location_, _Month_.
4. **Begin forming hypotheses** about which product categories might be most (or least) affected by inflation or demand changes.
    

### Step 4: Data Visualization

1. **Choose a visualization tool**:
    
    - **Tableau or Power BI** for drag-and-drop dashboard creation.
2. **Recommended Visuals**:
    
    - **Trend Line**:
        - Plot how grocery prices and sales volumes have changed over time versus inflation rates.
    - **Bar/Column Charts**:
        - Compare revenue or profit across product categories or store locations.
    - **Heatmaps/Maps** (if using location data):
        - Show regional differences in demand or sales.
    - **Scatter Plots**:
        - Show correlation between product prices and inflation index.
3. **Dashboard Creation**:
    
    - Include a **summary** page showing key metrics: total sales, average profit margins, inflation trend overlays, etc.
    - Provide **interactive filters** (e.g., by region, product category, or time period) if using Tableau.

### Step 5: Insights & Recommendations

1. **Identify Key Products to Promote**:
    
    - Look for categories with **stable or rising** demand and **good profit margins**.
    - Evaluate how inflation affects these categories; if costs are still manageable, promotion may be profitable.
2. **Spot Areas Becoming Less Profitable**:
    
    - Look for product lines where **costs** are rising faster than **revenues**, or margins are shrinking.
    - Identify items with **low demand** that may not be worth stocking in the long run.
3. **Highlight High-Margin Growth Areas**:
    
    - Identify categories with consistently **high margins** that also show **consumer demand** resiliency.
    - Recommend strategies to expand product lines or marketing in these areas.
4. **Draft an Executive Summary**:
    
    - Provide bullet points or an executive-level **slide deck** with data-driven conclusions.
    - Align recommendations with **financial goals** and **brand strategy**.
5. **(Optional Advanced)**:
    
    - **Scenario Planning**:
        - Model best/worst-case inflation scenarios and the impact on product-level profitability.

---

## 5. Deliverables

1. **Data Files** (Cleaned & Merged):
    
    - Final versions stored in SQL tables, Pandas dataframes, or Excel sheets.
2. **Exploratory Analysis**:
    
    - Write up or notebook (Jupyter Notebook if using Python) showcasing the key descriptive statistics, transformations, and charts.
3. **Visualization Dashboards or Charts**:
    
    - Tableau or Power BI dashboard (with interactive elements).
    - Showcase trends, comparisons by category/location, and inflation-adjusted insights.
4. **Executive Summary** (Presentation or Report):
    
    - Summarize main insights with visuals and bullet-point findings.
    - Clearly state product recommendations, potential problem areas, and strategic expansion opportunities.

---

## 6. Optional Advanced Extensions

1. **Geo-Spatial Analysis**:
    
    - If store location data is available, overlay on a map (in Tableau) to highlight regional discrepancies in sales or profitability.

2. **Advanced Statistical Methods**:
    
    - Correlation or causation tests to definitively link inflation spikes to certain product declines or changes in consumer behavior.

---

## 7. Success Criteria & Wrap-Up

By completing this project, you’ll:

- **Practice data cleaning & preparation** in SQL, Excel, or Pandas.
- **Build interactive visualizations** in Tableau or Power BI.
- **Interpret economic data** (inflation) in the context of **retail sales**.
- **Develop strategic recommendations** for a large business based on data insights.
- (Optionally) **Hone advanced analytics** skills with scenario planning or more sophisticated dashboards.

This project guides you through the key steps of **end-to-end data analysis**—collecting and cleaning data, exploring and visualizing patterns, and translating your findings into **real-world business recommendations**. By adjusting the scope (datasets used, complexity of analysis), you can keep it beginner-friendly or turn it into a robust, advanced data project.