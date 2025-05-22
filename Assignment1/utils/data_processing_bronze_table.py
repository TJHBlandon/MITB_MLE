import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import argparse

import utils.common_utils

def process_bronze_table(file_directory, clickstream_df, attributes_df, financials_df, loans_df):

    print("Processing Bronze data")
    
    clickstream_bronze = utils.common_utils.add_time_partitions(clickstream_df)
    attributes_bronze = utils.common_utils.add_time_partitions(attributes_df)
    financials_bronze = utils.common_utils.add_time_partitions(financials_df)
    loans_bronze = utils.common_utils.add_time_partitions(loans_df)
    
    utils.common_utils.save_partitioned_data(clickstream_bronze, f"{file_directory}/clickstream")
    utils.common_utils.save_partitioned_data(attributes_bronze, f"{file_directory}/attributes")
    utils.common_utils.save_partitioned_data(financials_bronze, f"{file_directory}/financials")
    utils.common_utils.save_partitioned_data(loans_bronze, f"{file_directory}/loans")

    print("Processing completed")
    
    return clickstream_bronze, attributes_bronze, financials_bronze, loans_bronze
