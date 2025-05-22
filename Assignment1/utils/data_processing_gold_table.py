import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import argparse

from pyspark.sql.functions import col, when, upper, trim, initcap, regexp_replace, max, min, count, avg, coalesce, last
from pyspark.sql.functions import sum as sparksum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import utils.common_utils



def process_gold_table(gold_directory, silver_directory, spark):
    
    print("Starting Gold Layer Processing...")
    
    clickstream_silver = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{silver_directory}/clickstream_clean")
    attributes_silver = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{silver_directory}/attributes_clean")
    financials_silver = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{silver_directory}/financials_clean")
    loans_monthly = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{silver_directory}/loans_customer_monthly")
    
    customer_360 = create_customer_360(
        clickstream_silver, attributes_silver, financials_silver, loans_monthly
    )
    loan_portfolio = create_loan_portfolio_summary(loans_monthly)
    
    utils.common_utils.save_partitioned_data(customer_360, f"{gold_directory}/customer_360")
    utils.common_utils.save_partitioned_data(loan_portfolio, f"{gold_directory}/loan_portfolio")
    
    print("Gold Layer Processing Completed")
    return customer_360, loan_portfolio
    
def create_customer_360(clickstream_df, attributes_df, financials_df, loans_df):

    print("Creating Customer 360 view...")

    clickstream_agg = clickstream_df.groupBy("Customer_ID", "year", "month") \
        .agg(
            max("snapshot_date").alias("clickstream_snapshot_date"),
            avg("total_activity").alias("avg_total_activity"),
            avg("positive_features").alias("avg_positive_features"),
            avg("negative_features").alias("avg_negative_features"),
            count("*").alias("clickstream_records")
        )
    
    attributes_clean = attributes_df.select(
        col("Customer_ID"),
        col("year"),
        col("month"),
        col("snapshot_date").alias("attributes_snapshot_date"),
        col("Age"),
        col("Occupation")
    )
    
    financials_clean = financials_df.select(
        col("Customer_ID"),
        col("year"),
        col("month"),
        col("snapshot_date").alias("financials_snapshot_date"),
        col("Annual_Income"),
        col("Credit_Utilization_Ratio"),
        col("Outstanding_Debt"),
        col("debt_to_income_ratio"),
        col("monthly_savings_rate")
    )
    
    loans_clean = loans_df.select(
        col("Customer_ID"),
        col("year"),
        col("month"),
        col("snapshot_date").alias("loans_snapshot_date"),
        col("total_outstanding_balance"),
        col("overdue_rate"),
        col("total_loan_amount")
    )

    customer_360 = attributes_clean.join(
        financials_clean, ["Customer_ID", "year", "month"], "left"
    ).join(
        clickstream_agg, ["Customer_ID", "year", "month"], "left"  
    ).join(
        loans_clean, ["Customer_ID", "year", "month"], "left"
    )
    
    customer_360 = customer_360.na.fill({
        "avg_total_activity": 0,
        "clickstream_records": 0,
        "total_outstanding_balance": 0,
        "overdue_rate": 0,
    })
    
    customer_360 = customer_360.withColumn(
        "snapshot_date",
        coalesce(
            col("attributes_snapshot_date"),
            col("financials_snapshot_date"),
            col("clickstream_snapshot_date"),
            col("loans_snapshot_date"),
        )
    ).drop(
        "attributes_snapshot_date",
        "financials_snapshot_date", 
        "clickstream_snapshot_date",
        "loans_snapshot_date"
    )
    
    return customer_360


def create_loan_portfolio_summary(loans_df):
    """Create loan portfolio management view"""
    print("Creating Loan Portfolio Summary...")
    
    portfolio_summary = loans_df.groupBy("Customer_ID") \
        .agg(
            max("snapshot_date").alias("latest_snapshot_date"),
            max("year").alias("year"),
            max("month").alias("month"),
            last("total_outstanding_balance").alias("current_total_balance"),
            last("overdue_rate").alias("current_overdue_rate"),
            max("total_loan_amount").alias("max_loan_exposure"),
            avg("utilization_rate").alias("avg_utilization_rate"),
            count("*").alias("months_with_loans")
        )
    
    return portfolio_summary