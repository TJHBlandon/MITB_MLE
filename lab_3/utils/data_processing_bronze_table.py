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
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def process_bronze_table(snapshot_date_str, bronze_lms_directory, spark):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to source back end - IRL connect to back end source system
    list_of_files = ["data/feature_clickstream.csv","data/feature_attributes.csv","data/feature_financials.csv","data/lms_loan_daily.csv"]

    # load data - IRL ingest from back end source system
    for file in list_of_files:
        df = spark.read.csv(file, header=True, inferSchema=True).filter(col('snapshot_date') == snapshot_date)

        print(snapshot_date_str + 'row count:', df.count())
        file_name = file.strip("data/.csv")

        # save bronze table to datamart - IRL connect to database to write
        partition_name = "bronze_" + file_name + "_" + snapshot_date_str.replace('-','_') + '.csv'
        filepath = bronze_lms_directory + partition_name
        df.toPandas().to_csv(filepath, index=False)
        print('saved to:', filepath)

        return df
