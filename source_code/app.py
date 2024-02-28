from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Programming-Exercise") \
        .getOrCreate()

credit_card_csv_path = "Datasets/dataset_two.csv"
personal_info_csv_path = "Datasets/dataset_one.csv"

credit_card_df = spark.read.csv(credit_card_csv_path, header = True, inferSchema = True)
personal_info_df = spark.read.csv(personal_info_csv_path, header = True, inferSchema = True)

credit_card_df.printSchema()
personal_info_df.printSchema()

# Step 1: Join both dataframes 
# Looking at the toal number of records each dataframe has before join
print("Personal info dataframe number of records is ", personal_info_df.count())

print("Credit card dataframe number of records is ", credit_card_df.count())

# Both dataframes have 1000 records, therefore no records will be missing from either dataframe
# Perform join based on id
Joined_df = personal_info_df.join(credit_card_df, "id", "inner")
Joined_df.show()
print("Total number of records in joined dataframe ", Joined_df.count())


# Step 2:

