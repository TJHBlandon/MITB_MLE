from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

def add_time_partitions(df):
    """Add year and month columns for partitioning"""
    return df.withColumn("year", year(col("snapshot_date"))) \
                .withColumn("month", month(col("snapshot_date")))

def save_partitioned_data(df, path):
    """Save dataframe with year/month partitioning"""
    df.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)
