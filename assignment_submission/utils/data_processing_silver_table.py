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

from pyspark.sql.functions import col, when, upper, trim, initcap, regexp_replace, max, min, count
from pyspark.sql.functions import sum as sparksum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import utils.common_utils


def process_silver_table(file_directory, bronze_directory, spark):
    
    print("Starting Silver Layer Processing...")
    
    clickstream_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_directory}/clickstream")
    attributes_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_directory}/attributes")
    financials_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_directory}/financials")
    loans_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_directory}/loans")
    
    clickstream_silver = clean_clickstream_data(clickstream_bronze)
    attributes_silver = clean_attributes_data(attributes_bronze)
    financials_silver = clean_financials_data(financials_bronze)
    loans_silver = clean_loans_data(loans_bronze)
    
    loans_customer_monthly = create_loan_customer_monthly(loans_silver)
    
    utils.common_utils.save_partitioned_data(clickstream_silver, f"{file_directory}/clickstream_clean")
    utils.common_utils.save_partitioned_data(attributes_silver, f"{file_directory}/attributes_clean")
    utils.common_utils.save_partitioned_data(financials_silver, f"{file_directory}/financials_clean")
    # utils.common_utils.save_partitioned_data(loans_silver, f"{file_directory}/loans_clean")
    utils.common_utils.save_partitioned_data(loans_customer_monthly, f"{file_directory}/loans_customer_monthly")
    
    print("Silver Layer Processing Completed")
    return clickstream_silver, attributes_silver, financials_silver, loans_customer_monthly
    
def clean_clickstream_data(df):

    print("Cleaning clickstream data...")
    
    feature_cols = [f"fe_{i}" for i in range(1, 21)]
    df_clean = df.na.fill(0, feature_cols)
    
    df_clean = df_clean.withColumn(
        "total_activity", 
        sum([col(f"fe_{i}") for i in range(1, 21)])
    ).withColumn(
        "avg_activity",
        (sum([col(f"fe_{i}") for i in range(1, 21)])) / 20
    ).withColumn(
        "positive_features",
        sum([when(col(f"fe_{i}") > 0, 1).otherwise(0) for i in range(1, 21)])
    ).withColumn(
        "negative_features", 
        sum([when(col(f"fe_{i}") < 0, 1).otherwise(0) for i in range(1, 21)])
    )
    
    df_clean = df_clean.withColumn("Customer_ID", upper(trim(col("Customer_ID"))))
    
    return df_clean

def clean_attributes_data(df):
    
    print("Cleaning attributes data...")
    df_clean = df
    df_clean = df_clean.withColumn("Customer_ID", upper(trim(col("Customer_ID")))) \
                    .withColumn("Name", initcap(trim(col("Name")))) \
                    .withColumn("Occupation", initcap(trim(col("Occupation"))))
    
    df_clean = df_clean.withColumn("Age", regexp_replace(col("Age"), "_", ""))
 
    df_clean = df_clean.withColumn(
        "Age", 
        when(col("Age").rlike("^\\d+$"), col("Age").cast("int")).otherwise(None)
    )

    df_clean = df_clean.filter(
        col('Age').isNotNull() &
        (col("Age") >= 1) & 
        (col("Age") <= 100)
    )

    df_clean = df_clean.withColumn("Occupation", when(col("Occupation").rlike("^_"), 'Not Declared').otherwise(col("Occupation")))
    df_clean = df_clean.withColumn("SSN", when(col("SSN").rlike("^[!@/#]"), "No Information").otherwise(col("SSN")))
    
    return df_clean

def clean_financials_data(df):

    print("Cleaning financial data...")
    
    df_clean = df.withColumn("Customer_ID", upper(trim(col("Customer_ID"))))
    
    numeric_cols = ["Annual_Income", "Monthly_Inhand_Salary", "Outstanding_Debt", 
                    "Amount_invested_monthly", "Monthly_Balance", "Num_of_Loan", 
                    "Num_of_Delayed_Payment"]
    
    string_cols = ["Type_of_Loan", "Credit_Mix", "Credit_History_Age", "Payment_Behaviour"]

    for col_name in numeric_cols:
        if col_name in df.columns:
            df_clean = df_clean.withColumn(col_name, 
                                           regexp_replace(col(col_name), "_", "")) \
            .withColumn(
                col_name,
                regexp_replace(col(col_name), "[^\\d.-]", "").cast("double")
            )
    
    df_clean = df_clean.na.fill(0, numeric_cols)
    
    df_clean = df_clean.withColumn(
        "debt_to_income_ratio",
        when(col("Annual_Income") > 0, col("Outstanding_Debt") / col("Annual_Income")).otherwise(0)
    ).withColumn(
        "monthly_savings_rate",
        when(col("Monthly_Inhand_Salary") > 0, 
                col("Amount_invested_monthly") / col("Monthly_Inhand_Salary")).otherwise(0)
    )
    
    for col_name in string_cols:
        if col_name in df.columns:
            df_clean = df_clean.withColumn(col_name, 
                                           when(col(col_name).rlike("^[!@/#]"), "No Infomation").otherwise(col(col_name)))

    df_clean = df_clean.na.fill("No Infomation", string_cols)

    return df_clean

def clean_loans_data(df):

    print("Cleaning loans data...")
    
    df_clean = df.withColumn("Customer_ID", upper(trim(col("Customer_ID")))) \
                    .withColumn("loan_id", upper(trim(col("loan_id"))))
    
    numeric_cols = ["due_amt", "paid_amt", "overdue_amt", "balance", "loan_amt", "tenure", "installment_num"]

    for col_name in numeric_cols:
        df_clean = df_clean.withColumn(
            col_name,
            when(col(col_name).isNull() | (col(col_name) == ""), 0)
            .otherwise(
                regexp_replace(col(col_name), "[^\\d.-]", "").cast("double")
            )
        )
    # Create loan performance features
    df_clean = df_clean.withColumn(
        "payment_ratio",
        when(col("due_amt") > 0, col("paid_amt").cast("double") / col("due_amt").cast("double")).otherwise(0)
    ).withColumn(
        "overdue_ratio", 
        when(col("due_amt") > 0, col("overdue_amt").cast("double") / col("due_amt").cast("double")).otherwise(0)
    ).withColumn(
        "is_overdue",
        when(col("overdue_amt") > 0, 1).otherwise(0)
    )

    return df_clean

def create_loan_customer_monthly(loans_df):
        
    print("Creating loan customer monthly aggregations...")
    loans_df_clean = loans_df.withColumn("loan_amt", col("loan_amt").cast("double")) \
                         .withColumn("balance", col("balance").cast("double")) \
                         .withColumn("due_amt", col("due_amt").cast("double")) \
                         .withColumn("paid_amt", col("paid_amt").cast("double")) \
                         .withColumn("overdue_amt", col("overdue_amt").cast("double")) \
                         .withColumn("overdue_ratio", col("overdue_ratio").cast("double")) \
                         .withColumn("is_overdue", col("is_overdue").cast("int")) \
    
    loans_monthly = loans_df_clean.groupBy("Customer_ID", "year", "month") \
        .agg(
            max("snapshot_date").alias("snapshot_date"),
            sparksum("loan_amt").alias("total_loan_amount"),
            sparksum("balance").alias("total_outstanding_balance"),
            sparksum("paid_amt").alias("total_paid_amount"),
            sparksum("overdue_amt").alias("total_overdue_amount"),
            sparksum("is_overdue").alias("overdue_installments_count"),
            count("*").alias("total_installments"),
            sparksum("overdue_ratio").alias("max_overdue_ratio"),
        )
    
    loans_monthly = loans_monthly.withColumn(
        "overdue_rate",
        when(col("total_installments") > 0, 
             col("overdue_installments_count") / col("total_installments")).otherwise(0)
    ).withColumn(
        "utilization_rate",
        when(col("total_loan_amount") > 0,
             col("total_outstanding_balance") / col("total_loan_amount")).otherwise(0)
    )
    
    return loans_monthly