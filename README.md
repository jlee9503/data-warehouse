# Sales Data Warehouse Project

## Project Overview
This project demonstrates a comprehensive data warehousing and analytics solution using SQL. It covers full data pipeline - from building a data warehouse to generating actionable insights - and showcases key data engineering and analytics skills, including data cleaning, ETL pipeline implementation, and structured data analysis.

The Project includes:
  1. **Data Architecture:** Designing a medallion architecture (Bronze, Silver, and Gold layers)
  2. **ETL Pipelines:** Extracting, transforming, and loading data from source systems into the warehouse
  3. **Data Modeling:** Developing fact and dimension tables optimized for analytical queries

## Data Architecture

The data architecture for this project uses **Medallion architecture** (Bronze, Silver, and Gold):

- **Bronze:**
  - Raw ingestion of data with minimal transformation.
  - Stores raw data from the source systems.
  - Convert CSV data files into SQL Server Database
- **Silver:**
  - Cleaned and enriched data used for analytics.
  - Includes data cleansing, standarization, and normalization process to prepare data for analysis.
- **Gold:**
  - Aggregated, business-ready data into a star schema for reporting and analytics.

## Project Requirements

### Building the Data Warehouse (Data Engineering)

Develop a data warehouse using SQL Server to combine sales data, enabling analytical reporting and informed decision-making. 

- **Dataset:** Import raw data files from two sources systems
  - CRM: cust_info.csv, prd_info.csv, sales_details.csv
  - ERP: cust_az12.csv, loc_a101.csv, px_cat_g1v2.csv
- **Data Cleaning:** Resolves data quality issues prior to analysis
- **Data Integration:** Combine both sources into a single, user-friendly model designed
  
### Business Analytics & Reporting (Data Analysis)

Develop SQL-based analytics to deliver detailed insights.
- Customer Behavior
- Product Performance
- Sales Trends

These insights empower stakeholders with key business metrics, enabling strategic decision-making.
