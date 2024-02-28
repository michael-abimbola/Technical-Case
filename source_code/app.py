from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

import logging
import datetime as dt

from logging.handlers import RotatingFileHandler

today = dt.datetime.today()
log_filename_format = f"{today.day:02d}-{today.month:02d}-{today.year}.log"

import pyspark.sql.functions as f

# Spark session creation
spark = SparkSession.builder\
        .appName("Programming-Exercise") \
        .getOrCreate()

# Logging initilisation
handler = RotatingFileHandler(
    filename=log_filename_format,
    maxBytes = 2048,
    backupCount=3
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger("ProjectLogger")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Create Client data
# Step 1: Create generic Filter for countries
def filter_country(origin_df: DataFrame, colum_name: str, country_name: str) -> DataFrame:
    try:
        filtered_data = origin_df.filter(f.col(colum_name) == country_name)
        return filtered_data
    except Exception as e:
        logger.exception(e)
        return DataFrame()


# Final: Function for creating final output
def client_data_creation(df1_path: str, df2_path: str, country_name: str) -> DataFrame:
    # Read both dataframes
    df1= spark.read.csv(df1_path, header = True, inferSchema = True)
    df2 = spark.read.csv(df2_path, header = True, inferSchema = True)
    logger.info("Both dataframes read by spark")

    # Join dataframe
    joined_df = df1.join(df2, "id", "inner")
    logger.info("Personal info and Credit card dataframes joined")

    # Filter joined dataframe by country
    filtered_joined_data = filter_country(joined_df, "country", country_name)
    logger.info(f"Joined dataframe has been filtered on the country {country_name}")
    return filtered_joined_data


Final_data = client_data_creation("Datasets/dataset_two.csv", "Datasets/dataset_one.csv", "Netherlands")
Final_data.show()


