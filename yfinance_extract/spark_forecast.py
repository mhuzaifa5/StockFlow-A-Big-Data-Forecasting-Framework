from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

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
    print(f"Forecasted data saved to {output_path}")

# Example usage:
# forecast_stock_prices('hdfs://localhost:9000/stock_data/raw/', 'hdfs://localhost:9000/stock_data/forecasted/')
