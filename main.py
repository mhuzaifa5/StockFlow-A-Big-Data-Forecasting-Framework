import os
import subprocess
import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Step 1: Fetch stock data using YFinance
def fetch_stock_data(symbols, output_path):
    data = []
    for symbol in symbols:
        stock = yf.download(symbol, period="5d", interval="1d")
        stock['Symbol'] = symbol
        data.append(stock.reset_index())

    result = pd.concat(data)
    result.to_csv(output_path, index=False)
    print(f"✅ Stock data saved to {output_path}")

# Step 2: Trigger Flume Agent to load data to HDFS
def run_flume_agent():
    try:
        agent_name = "agent"
        conf_file_path = "scripts/flume.conf"  # Adjust according to your file path
        flume_bin_path = "/path/to/flume/bin/flume-ng"  # Replace with your real path

        command = [
            flume_bin_path,
            "agent",
            "--conf",
            "./conf",
            "--conf-file",
            conf_file_path,
            "--name",
            agent_name,
            "-Dflume.root.logger=INFO,console"
        ]

        print(f"⚡ Starting Flume agent...")
        subprocess.run(command, check=True)
        print("✅ Flume agent started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running Flume agent: {e}")

# Step 3: Forecast stock prices using Apache Spark
def forecast_stock_prices(input_path, output_path):
    spark = SparkSession.builder \
        .appName("Stock Price Forecasting") \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=["Open", "High", "Low", "Volume"],
        outputCol="features"
    )
    data = assembler.transform(df)

    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(featuresCol="features", labelCol="Close")
    lr_model = lr.fit(train_data)

    predictions = lr_model.transform(test_data)

    forecast = predictions.select(
        col("Symbol"),
        col("Date").alias("forecast_date"),
        col("prediction").alias("predicted_close")
    )

    forecast.write.csv(output_path, header=True, mode="overwrite")
    spark.stop()
    print(f"✅ Forecasted data saved to {output_path}")

# Step 4: Execute Hive Queries (Simple simulation)
def run_hive_queries():
    try:
        hive_query = """
        CREATE EXTERNAL TABLE IF NOT EXISTS stock_data (
            Date STRING,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Adj_Close DOUBLE,
            Volume BIGINT,
            Symbol STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 'hdfs://localhost:9000/stock_data/raw/';

        CREATE EXTERNAL TABLE IF NOT EXISTS forecasted_data (
            Symbol STRING,
            forecast_date STRING,
            predicted_close DOUBLE
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 'hdfs://localhost:9000/stock_data/forecasted/';
        """

        # Save to a temp .hql file
        with open("scripts/temp_hive_queries.hql", "w") as f:
            f.write(hive_query)

        print("⚡ Running Hive queries...")
        subprocess.run(["hive", "-f", "scripts/temp_hive_queries.hql"], check=True)
        print("✅ Hive tables created and ready for Tableau visualization.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error running Hive queries: {e}")

# Step 5: Main pipeline
def main():
    print("\n🚀 Starting Stock Data Pipeline...")

    # Local output file
    local_stock_data_path = "local_folder/stock_data.csv"
    os.makedirs("local_folder", exist_ok=True)

    # Step 1: Fetch Stock Data
    stock_symbols = ['AAPL', 'MSFT', 'GOOG']
    fetch_stock_data(stock_symbols, local_stock_data_path)

    # Step 2: Run Flume agent (Flume will pick file and ingest into HDFS)
    run_flume_agent()

    # Step 3: Forecast Stock Prices
    hdfs_raw_data_path = "hdfs://localhost:9000/stock_data/raw/"
    hdfs_forecasted_data_path = "hdfs://localhost:9000/stock_data/forecasted/"
    forecast_stock_prices(hdfs_raw_data_path, hdfs_forecasted_data_path)

    # Step 4: Create Hive tables
    run_hive_queries()

    print("\n🎯 Pipeline completed successfully!")

if __name__ == "__main__":
    main()
