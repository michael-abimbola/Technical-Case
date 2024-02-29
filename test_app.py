import pytest
import chispa

from source_code.app import filter_country
from source_code.app import rename_column
from source_code.app import drop_columns
from pyspark.sql import Row

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("Programming-Exercise-Test") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def input_dataframe(spark_session):
    data = [
        Row(id=1, btc_a="1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", cc_t="visa-electron", cc_n=4175006996999270, first_name="Feliza", last_name="Eusden", email="feusden0@ameblo.jp", country="Netherlands"), 
        Row(id=2, btc_a="1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", cc_t="jcb", cc_n=3587679584356527, first_name="Priscilla", last_name="Le Pine", email="plepine1@biglobe.ne.jp", country="UK"), 
        Row(id=3, btc_a="1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", cc_t="diners-club-enroute", cc_n=201876885481838, first_name="Jaimie", last_name="Sandes", email="jsandes2@reuters.com", country="Netherlands")
    ]
    df = spark_session.createDataFrame(data)
    yield df


def test_can_filter_country(spark_session, input_dataframe):

    filtered_df = filter_country(input_dataframe, "country", "Netherlands")

    expected_data = [
            Row(id = 1,  btc_a = "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", cc_t = "visa-electron", cc_n = 4175006996999270, first_name = "Feliza", last_name = "Eusden", email = "feusden0@ameblo.jp", country = "Netherlands"), 
            Row(id = 3,  btc_a = "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", cc_t = "diners-club-enroute", cc_n = 201876885481838, first_name = "Jaimie", last_name = "Sandes", email = "jsandes2@reuters.com", country = "Netherlands")
    ]

    expected_df  = spark_session.createDataFrame(expected_data)
    chispa.assert_df_equality(filtered_df, expected_df)


def test_can_rename_columns(spark_session, input_dataframe):

    column_renames = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }

    filtered_df = rename_column(input_dataframe, column_renames)

    expected_data = [
        Row(client_identifier=1, bitcoin_address="1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", credit_card_type="visa-electron", cc_n=4175006996999270, first_name="Feliza", last_name="Eusden", email="feusden0@ameblo.jp", country="Netherlands"), 
        Row(client_identifier=2, bitcoin_address="1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", credit_card_type="jcb", cc_n=3587679584356527, first_name="Priscilla", last_name="Le Pine", email="plepine1@biglobe.ne.jp", country="UK"), 
        Row(client_identifier=3, bitcoin_address="1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", credit_card_type="diners-club-enroute", cc_n=201876885481838, first_name="Jaimie", last_name="Sandes", email="jsandes2@reuters.com", country="Netherlands")
    ]

    expected_df  = spark_session.createDataFrame(expected_data)
    chispa.assert_df_equality(filtered_df, expected_df)


def test_does_not_rename_nonexistent_columns(spark_session, input_dataframe):

    column_renames = {
        "column_name": "new_column_name"
    }

    filtered_df = rename_column(input_dataframe, column_renames)

    expected_data = input_dataframe

    chispa.assert_df_equality(filtered_df, expected_data)


def test_drop_columns(spark_session, input_dataframe):

    columns_to_drop = ["first_name", "last_name", "cc_n"]
    filtered_df = drop_columns(input_dataframe, columns_to_drop)

    expected_data = [
        Row(id=1, btc_a="1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", cc_t="visa-electron", email="feusden0@ameblo.jp", country="Netherlands"), 
        Row(id=2, btc_a="1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", cc_t="jcb", email="plepine1@biglobe.ne.jp", country="UK"), 
        Row(id=3, btc_a="1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", cc_t="diners-club-enroute", email="jsandes2@reuters.com", country="Netherlands")
    ]

    expected_df  = spark_session.createDataFrame(expected_data)
    chispa.assert_df_equality(filtered_df, expected_df)