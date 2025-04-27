-- Create database
CREATE DATABASE IF NOT EXISTS stock_forecast;

USE stock_forecast;

-- Create table for raw data
CREATE EXTERNAL TABLE IF NOT EXISTS raw_stock_data (
    stock_symbol STRING,
    date STRING,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/stock_data/raw/';

-- Create table for forecasted data
CREATE EXTERNAL TABLE IF NOT EXISTS forecasted_stock_data (
    stock_symbol STRING,
    forecast_date STRING,
    predicted_close FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/stock_data/forecasted/';

-- Sample query to join raw + forecasted
SELECT 
    r.stock_symbol,
    r.date,
    r.close,
    f.forecast_date,
    f.predicted_close
FROM raw_stock_data r
LEFT JOIN forecasted_stock_data f
ON r.stock_symbol = f.stock_symbol
WHERE r.date <= f.forecast_date;
