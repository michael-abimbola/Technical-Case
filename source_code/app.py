from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


import logging
import datetime as dt

from logging.handlers import RotatingFileHandler

today = dt.datetime.today()
log_filename_format = f"./logs/{today.day:02d}-{today.month:02d}-{today.year}.log"

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
# Step 1: Create generic Filter function for countries
def filter_country(origin_df: DataFrame, colum_name: str, country_name: str) -> DataFrame:
    """
    Filters the specified Dataframe based on the specified country name.

    :param origin_df: The dataFrame to be filtered.
    :type origin_df: DataFrame
    :param colum_name: The column name to filter on.
    :type colum_name: str
    :param country_name: The country name to filter on.
    :type country_name: str
    :return: A dataFrame filtered by the specified country name.
    :rtype: DataFrame
    """
    logger.info(f"Starting to filter DataFrame on country {country_name}")
    try:
        filtered_data = origin_df.filter(f.col(colum_name) == country_name)
        logger.info(f"Dataframe has been filtered on the country {country_name}. Rows before filter: {origin_df.count()}, Rows after filter: {filtered_data.count()}")
        return filtered_data
    except AnalysisException  as e:
        logger.exception(f"Error filtering DataFrame on country {country_name}: {e}")
        return spark.createDataFrame([], schema=origin_df.schema)


# Step 2: Create generic Rename function
def rename_column(origin_df: DataFrame, column_name_map: dict) -> DataFrame:
        """
        Renames the specified DataFrame columns based on the specified column name dictionary.

        :param origin_df: The dataFrame to be filtered.
        :type origin_df: DataFrame
        :param column_name_map: A dictionary with mapped original column names to new column names.
        :type colum_name: dict
        :return: A dataFrame with renamed column names based on the specified mapping
        :rtype: DataFrame
        """
        logger.info(f"Starting rename columns function with the following mapping: {column_name_map}")
        try:    
                renamed_df = origin_df
                for original_column_name, new_column_name in column_name_map.items():
                        # Check if the original column name exists in the DataFrame
                        if original_column_name in origin_df.columns:
                                renamed_df = renamed_df.withColumnRenamed(original_column_name, new_column_name)
                                logger.info(f"The column {original_column_name} has been renamed to {new_column_name}")
                        else:
                                logger.warning(f"Column {original_column_name} does not exist in the DataFrame. Skipping renaming.")
                return renamed_df
        except AnalysisException  as e:
                logger.exception(f"Error renaming columns: {e}")
                return spark.createDataFrame([], schema=origin_df.schema)
        

# Step 3: Drop Personal info data
def drop_columns(origin_df: DataFrame, columns: list) -> DataFrame:
      """
      Drops column names from the specified dataframe.

      :param origin_df: The dataFrame to drop columns from.
      :type origin_df: DataFrame
      :param columns: List of column names to drop.
      :type colum_name: list
      :return: A DataFrame with specified column names dropped.
      :rtype: DataFrame
      """     
      logger.info(f"Starting dropping columns function with the following columns: {columns}") 
      try:
            dropped_df = origin_df.drop(*columns)
            logger.info(f"The following columns {columns} have been dropped")
            return dropped_df
      except AnalysisException  as e:
            logger.exception(f"Error dropping columns: {e}")
            return spark.createDataFrame([], schema=origin_df.schema)


# Final: Function for creating final output
def client_data_creation(df1_path: str, df2_path: str, country_name: str) -> DataFrame:
    # Read both dataframes
    df1= spark.read.csv(df1_path, header = True, inferSchema = True)
    df2 = spark.read.csv(df2_path, header = True, inferSchema = True)
    logger.info("Both dataframes read by spark")


    # Join dataframe
    joined_df = df1.join(df2, "id", "inner")
    logger.info("Personal info and Credit card dataframes joined")

    # Drop columns in joined dataframe
    columns_to_drop = ["first_name", "last_name", "cc_n"]
    dropped_joined_df = drop_columns(joined_df, columns_to_drop)


    # Rename joined dataframe columns
    column_renames = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }
    renamed_dropped_joined_df = rename_column(dropped_joined_df, column_renames)


    # Filter joined dataframe by country
    filtered_renamed_dropped_joined_df = filter_country(renamed_dropped_joined_df, "country", country_name)
    return filtered_renamed_dropped_joined_df

# Usage
Final_data = client_data_creation("Datasets/dataset_two.csv", "Datasets/dataset_one.csv", "Netherlands")

# Write to client_data directory in root directory
logger.info(f"Starting writing final data to client_data directory in root directory")
if Final_data:
        try:
                save_path = "./client_data/final_data.csv"
                Final_data.write.mode("overwrite").option("header", "true").csv(save_path)
                logger.info(f"Client data dataframe has been save to {save_path}")
        except Exception as e:
              logger.exception(f"Error whilte saving data to path: {e}")
              

else:
      logger.info(f"Client data dataframe is empty, writing unsuccessful")




