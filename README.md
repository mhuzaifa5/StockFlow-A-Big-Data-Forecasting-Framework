# 📈 Stock Price Forecasting Big Data Pipeline

An end-to-end big data engineering project that extracts stock data from Yahoo Finance using Python, ingests it into Hadoop HDFS via Apache Flume, forecasts future stock prices using Apache Spark, stores results back into HDFS, queries using Apache Hive, and visualizes using Tableau.

---

## 🔥 Project Workflow Overview

1. **Data Extraction**
   - Python script extracts stock data using the `yfinance` library.

2. **Data Ingestion**
   - Data is ingested into Hadoop HDFS using Apache Flume.

3. **Data Forecasting**
   - Apache Spark reads raw data from HDFS, forecasts stock prices, and stores the forecasted data back into HDFS.

4. **Data Querying**
   - Apache Hive reads both raw and forecasted data from HDFS for SQL-based querying.

5. **Data Visualization**
   - Tableau connects to Hive (through ODBC/JDBC) and visualizes the data.

---

## 📂 Project Structure

![Pipeline Workflow](C:\Users\PMLS\Desktop\projecct.png)

## ⚙️ Requirements

- Python 3.x
- Hadoop HDFS installed and running
- Apache Flume installed
- Apache Spark installed
- Apache Hive installed
- Tableau (Desktop or Public)