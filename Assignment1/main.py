#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
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
import pyspark.sql.functions as F

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table

# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

clickstream_df = spark.read.csv("data/feature_clickstream.csv", header=True, inferSchema=True)
attributes_df = spark.read.csv("data/features_attributes.csv", header=True, inferSchema=True)
financials_df = spark.read.csv("data/features_financials.csv", header=True, inferSchema=True)
loans_df = spark.read.csv("data/lms_loan_daily.csv", header=True, inferSchema=True)

# <H2> BUILD Bronze DB</H2>
bronze_directory = "datamart/bronze"
if not os.path.exists(bronze_directory):
    os.makedirs(bronze_directory)

utils.data_processing_bronze_table.process_bronze_table(bronze_directory, clickstream_df, attributes_df, financials_df, loans_df)

# <h2>Build Silver DB</h2>
silver_directory = f"datamart/silver"
if not os.path.exists(silver_directory):
    os.makedirs(silver_directory)

utils.data_processing_silver_table.process_silver_table(silver_directory, bronze_directory, spark)


# <h2> Build Gold DB</h2>
gold_directory = f"datamart/gold"
if not os.path.exists(gold_directory):
    os.makedirs(gold_directory)

utils.data_processing_gold_table.process_gold_table(gold_directory, silver_directory, spark)





