import logging

import pendulum

import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType, MapType
import pyspark.sql.functions as F
import pandas as pd


from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

@dag(
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pyspark"],
    is_paused_upon_creation=True
)
def pyspark_dag():
    """
    ### Pyspark DAG
    Loads data from text files in "raw" bucket to parquet files in "processed" bucket
    Does some transformations and computes on resulting data aggregtes
    """

    @task.pyspark(conn_id="spark")
    def load_stocks(spark: SparkSession, sc: SparkContext):
        stocks_schema = StructType([
            StructField("Date", DateType(), False),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Adj Close", DoubleType(), True),
            StructField("Volume", IntegerType(), True)
            ])

        df_all_stocks = spark.read.format("csv").option("header", "true").schema(schema=stocks_schema).load("s3a://raw/hist/*.csv")

        # Extracting stock name from the file name
        df_all_stocks = df_all_stocks.withColumn("S3Path", F.input_file_name())
        df_all_stocks = df_all_stocks.withColumn("Symbol", F.regexp_extract("S3Path", "s3a://raw/hist/(\\w+).csv$", 1))
        df_all_stocks = df_all_stocks.drop("S3Path")

        df_all_stocks = df_all_stocks.withColumn("Date", F.to_date("Date", "yyyy-MM-dd"))

        stock_columns_dict = {col: re.sub("\s+", "_", col) for col in df_all_stocks.columns}
        df_all_stocks = df_all_stocks.withColumnsRenamed(stock_columns_dict)
        df_all_stocks.write.format("parquet").mode("overwrite").save("s3a://processed/stocks.parquet")


    @task.pyspark(conn_id="spark")
    def load_metadata(spark: SparkSession, sc: SparkContext):
        df_csv_meta = spark.read.format("csv") \
            .option("header", "true") \
            .load("s3a://raw/symbols_valid_meta.csv")

        new_columns_dict = {col: re.sub("\s+", "_", col) for col in df_csv_meta.columns}
        df_csv_meta = df_csv_meta.withColumnsRenamed(new_columns_dict)

        df_csv_meta.write.format("parquet").mode("overwrite").save("s3a://processed/symbols_valid_meta.parquet")


    @task.pyspark(conn_id="spark")
    def load_definitions(spark: SparkSession, sc: SparkContext):
        df_definitions_json = spark.read.json("s3a://raw/nasdaq_definitions.json", multiLine=True) 

        definitions_schema = StructType([
            StructField('Definition', StringType(), True),
            StructField('Key', StringType(), True),
            StructField('Value', StringType(), True)
        ])
        df_definitions = spark.createDataFrame(data=[], schema=definitions_schema)

        for column in df_definitions_json.columns:
            flat_df = df_definitions_json.select(F.lit(column).alias('Definition'), f'{column}.*').melt(ids=["Definition"], values=None, variableColumnName="Key",valueColumnName="Value")
            df_definitions = df_definitions.union(flat_df)

        df_definitions.write.format("parquet").mode("overwrite").save("s3a://processed/nasdaq_definitions.parquet")


            
    @task.pyspark(conn_id="spark")
    def compute_aggregates(spark: SparkSession, sc: SparkContext):

        df_definitions = spark.read.format("parquet").load("s3a://processed/nasdaq_definitions.parquet")
        df_stocks_parquet = spark.read.format("parquet").load("s3a://processed/stocks.parquet")
        df_symbols_parquet = spark.read.format("parquet").load("s3a://processed/symbols_valid_meta.parquet")

        df_financial_status_definitions = df_definitions.where(df_definitions.Definition == 'Financial_Status')
        df_listing_exchange_definitions = df_definitions.where(df_definitions.Definition == 'Listing_Exchange')
        df_market_category_definitions = df_definitions.where(df_definitions.Definition == 'Market_Category')

        print("1. Distribution of Financial Status of Stocks and ETFs")
        df_symbols_parquet \
            .groupBy(["ETF", "Financial_Status"]) \
            .count() \
            .join(df_financial_status_definitions, df_financial_status_definitions.Key == df_symbols_parquet.Financial_Status, 'leftouter') \
            .withColumnRenamed('Value', 'Financial_Status_Definition') \
            .drop("Definition", "Key") \
            .fillna("Unspecified", subset=["Financial_Status", "Financial_Status_Definition"]) \
            .orderBy("count", ascending=False) \
            .show(truncate=False)
        

        print("2. Distribution of Stocks and ETFs by Market Category")
        df_symbols_parquet \
            .groupBy(["ETF", "Market_Category"]) \
            .count() \
            .join(df_market_category_definitions, df_market_category_definitions.Key == df_symbols_parquet.Market_Category, 'leftouter') \
            .withColumnRenamed('Value', 'Market_Category_Definition') \
            .drop("Definition", "Key") \
            .fillna("Unspecified", subset=["Market_Category", "Market_Category_Definition"]) \
            .orderBy("count", ascending=False) \
            .show(truncate=False)
        

        print("3. Distribution of Stocks and ETFs by Exchange")
        df_symbols_parquet \
            .groupBy(["ETF", "Listing_Exchange"]) \
            .count() \
            .join(df_listing_exchange_definitions, df_listing_exchange_definitions.Key == df_symbols_parquet.Listing_Exchange, 'leftouter') \
            .withColumnRenamed('Value', 'Listing_Exchange_Definition') \
            .drop("Definition", "Key") \
            .fillna("Unspecified", subset=["Listing_Exchange", "Listing_Exchange_Definition"]) \
            .orderBy("count", ascending=False) \
            .show(truncate=False)

        print("4. Monthly Averages and Volatilities for Stocks and ETFs")
        df_stocks_parquet \
            .fillna({"Volume": 0, "Adj_Close": 0.0}) \
            .withColumn("Last_Day", F.last_day("Date")) \
            .withColumn("Turnover", F.col('Volume') * F.col('Adj_Close')) \
            .groupBy(["Symbol", "Last_Day"]) \
            .agg(F.avg('Volume').alias('Volume_Average'), \
                F.avg('Adj_Close').alias('Close_Price_Average'),\
                F.avg('Turnover').alias('Turnover_Average'),\
                F.std('Volume').alias('Volume_Volatility'),\
                F.std('Adj_Close').alias('Close_Price_Volatility'),\
                F.std('Turnover').alias('Turnover_Volatility')) \
            .join(other=df_symbols_parquet, on="Symbol", how='inner') \
            .orderBy('Volume_Average', ascending=False) \
            .show(truncate=False)

        print("5. For each stock find continuous streaks when they were not traded (excluding streaks of length 0)")
        windowSpec = Window.partitionBy("Symbol").orderBy("Date")
        df_stocks_parquet\
            .withColumn("Lagged", F.lag("Volume", 1, 0).over(windowSpec))\
            .withColumn("Volume_Streak", (F.col('Volume') != 0) & (F.col('Lagged') != 0))\
            .withColumn("Streak_Indicator", F.col('Volume_Streak').cast('integer'))\
            .withColumn("cumsum", F.sum('Streak_Indicator').over(windowSpec))\
            .withColumn("Acc", F.when(~F.col('Volume_Streak'), -1*F.col('cumsum')).otherwise(F.col('Streak_Indicator'))) \
            .withColumn("Streak_Length", F.col('cumsum') + F.col('Acc')) \
            .groupBy("Symbol")\
            .agg(F.max('Streak_Length').alias('Longest_Empty_Streak'), F.max_by('Date', 'Streak_Length').alias('Longest_Empty_Streak_Date'))\
            .filter(F.col('Longest_Empty_Streak') > 0)\
            .orderBy('Longest_Empty_Streak','Symbol', ascending=True)\
            .show(truncate=False)

        print("6. Largest day-to-day price changes and corresponding dates")
        windowSpec = Window.partitionBy("Symbol").orderBy("Date")
        min_date = df_stocks_parquet.agg(F.min("Date").alias('min_date'))

        df_stocks_parquet\
            .withColumn("Lagged", F.lag("Adj_Close", 1, 0.0).over(windowSpec))\
            .where(df_stocks_parquet.Date > min_date.first().min_date )\
            .withColumn("Price_Change", F.col('Adj_Close') - F.col('Lagged'))\
            .groupBy(["Symbol"])\
            .agg(F.min('Price_Change').alias('Largest_Price_Decrease'),\
                F.min_by('Date', 'Price_Change').alias('Largest_Price_Decrease_Date'),\
                F.max('Price_Change').alias('Largest_Price_Increase'),\
                F.max_by('Date', 'Price_Change').alias('Largest_Price_Increase_Date')) \
            .show()


        print("7. Finding maximum correlations between ETF prices")
        unique_etf_symbols = [symbol.Symbol for symbol in df_symbols_parquet.filter(df_symbols_parquet.ETF == 'Y').select("Symbol").distinct().collect()]
        df_etf_close_pivoted = df_stocks_parquet\
            .filter(df_stocks_parquet.Symbol.isin(unique_etf_symbols))\
            .groupBy("Date")\
            .pivot("Symbol", unique_etf_symbols)\
            .agg(F.max("Adj_Close").alias('Close'))\
            .orderBy("Date", ascending=True)

        # Spark Connect crashes on trying to cache a Dataframe, using pandas instead
        df_pd = df_etf_close_pivoted.toPandas()
        # Replace nulls with average
        df_pd.iloc[:, 1:] = df_pd.iloc[:, 1:].fillna(df_pd.iloc[:, 1:].mean())
        # removes diagonal elements in correlation matrix (that are always 1 since correlated with itself)
        df_pd.iloc[:, 1:]\
            .corr()\
            .replace(1.0, -10)\
            .agg(['max', 'idxmax'])
        print(df_pd)

        print("8. Actual and Estimated 95th Quantiles for every month")
        df_stocks_parquet\
            .fillna(0)\
            .withColumn("Last_Day", F.last_day("Date")) \
            .groupBy("Symbol", "Last_Day")\
            .agg(F.percentile("Adj_Close", 0.95).alias("Actual_Percentile_95"),
                F.avg("Adj_Close").alias("Mean"),
                F.std("Adj_Close").alias("Std"))\
            .withColumn("Normal_Distribution_Percentile_95", F.col('Mean') + 2*F.col('Std'))\
            .show(truncate=False)
        

    [load_stocks(), load_metadata(), load_definitions()]  >> compute_aggregates()



dag = pyspark_dag()