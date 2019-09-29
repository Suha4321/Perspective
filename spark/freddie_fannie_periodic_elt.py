import os
import re
import sys
import boto3
import pyspark
from pyspark import sql
from pyspark.sql import Row
from pyspark.sql import column
from pyspark.sql import functions , SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace
from pyspark.sql import DataFrameReader
from pyspark.sql.functions import coalesce
from pyspark.sql.window import Window


#######################################################################################################################
# Source layer - extract columns from S3 bucket and convert it into a dataframe
#######################################################################################################################
spark = SparkSession.builder \
    .appName("test_batch_median_update") \
    .getOrCreate()

sqlContext = pyspark.SQLContext(spark)
sc = spark.sparkContext

freddie_acquisition_url = 's3a://mortgageinsight/freddie/historical_data1_Q22010*.txt'
freddie_performance_url = 's3a://mortgageinsight/freddie/historical_data1_time_Q22010*.txt'
fannie_acquisition_url = 's3a://mortgageinsight/fannie/aquisition/Acquisition_2010Q2*.txt'
fannie_performance_url = 's3a://mortgageinsight/fannie/performance/Performance_2010Q2.txt'

credit_score_url = "s3a://mortgageinsight/reference_credit_score/credit_score_ref.json"


# define source column names
freddie_src_acquisition_col =  ["credit_score",
                                    "first_payment_date",
                                    "first_time_homebuyer_flag",
                                    "maturity_date",
                                    "msa",
                                    "mip",#
                                    "number_of_units",
                                    "occupancy_status",
                                    "original_cltv",
                                    "original_dti",
                                    "original_upb",
                                    "original_ltv",
                                    "original_interest_rate",
                                    "channel",
                                    "prepayment_penalty_flag",
                                    "product_type",
                                    "property_state",
                                    "property_type",
                                    "postal_code",
                                    "loan_seq_no",
                                    "loan_purpose",
                                    "original_loan_term",
                                    "number_of_borrowers",
                                    "seller_name",
                                    "servicer_name",
                                    "super_conforming_flag"]


freddie_src_performance_col = ["loan_seq_no",
                                    "report_period",
                                    "cur_actual_upb",
                                    "cur_delinquency",
                                    "loan_age",
                                    "mon_to_maturity",
                                    "repurchase",
                                    "modification",
                                    "zero_balance_code",
                                    "zero_balance_date",
                                    "cur_interest_rate",
                                    "cur_deferred_upb",
                                    "ddlpi",
                                    "mi_recoveries",
                                    "net_sale_proceeds",
                                    "non_mi_recoveries",
                                    "expenses",
                                    "legal_costs",
                                    "maintain_costs",
                                    "tax_insurance",
                                    "miscellaneous_expenses",
                                    "actual_loss",
                                    "modification_cost",
                                    "step_modification",
                                    "deferred_payment_modification",
                                    "estimated_ltv"]


fannie_src_acquisition_col = ["loan_seq_no",
                                   "channel",
                                   "seller_name",
                                   "original_interest_rate",
                                   "original_upb",
                                   "original_loan_term",
                                   "origination_date",
                                   "first_payment_date",
                                   "original_ltv",
                                   "original_cltv",
                                   "number_of_borrowers",
                                   "original_dti",
                                   "credit_score",
                                   "first_time_homebuyer_flag",
                                   "loan_purpose",
                                   "property_type",
                                   "number_of_units",
                                   "occupancy_status",
                                   "property_state",
                                   "postal_code",
                                   "mip",
                                   "product_type",
                                   "co_borrower_credit_score",
                                   "mortgage_insurance_type",
                                   "relocation_mortgage_indicator"]


fannie_src_performance_col = ["loan_seq_no",
                               "report_period",
                               "servicer_name",
                               "cur_interest_rate",
                               "cur_actual_upb",
                               "loan_age",
                               "mon_to_maturity",
                               "adjusted_mon_to_maturity",
                               "maturity_date",
                               "msa",
                               "cur_delinquency",
                                "modification",
                               "zero_balance_code",
                               "zero_balance_date",
                               "last_paid_installment_date",
                               "foreclosure_date",
                               "disposition_date",
                               "foreclosure_costs",
                               "property_preservation_repair_costs",
                               "asset_recovery_costs",
                               "miscellaneous_expenses",
                               "associated_taxes",
                               "net_sale_proceeds",
                               "credit_enhancement_proceeds",
                               "repurchase_make_whole_proceeds",
                               "other_foreclousure_proceeds",
                               "non_interest_bearing_upb",
                               "principal_forgiveness_amount",
                               "repurchase_make_whole_proceeds_flag",
                               "foreclousure_principle_write_off_amount",
                               "servicing_activity_indicator"]


# function to output the data frames with column names
def return_data_frame(url, col_name):
    data_frame = spark.read.format("csv").\
        option("header", "false").\
        option("delimiter", "|").\
        load(url)
    for c, n in zip(data_frame.columns,col_name ):
        data_frame = data_frame.withColumnRenamed(c, n)
    return data_frame


# apply functions to get the source data frame
freddie_src_acquisition_df = return_data_frame(freddie_acquisition_url, freddie_src_acquisition_col)
freddie_src_performance_df = return_data_frame(freddie_performance_url, freddie_src_performance_col)
fannie_src_acquisition_df = return_data_frame(fannie_acquisition_url, fannie_src_acquisition_col)
fannie_src_performance_df = return_data_frame(fannie_performance_url, fannie_src_performance_col)



#######################################################################################################################
# Work layer  - Do the required transformation operations :
#######################################################################################################################

# function to convert date to unix time stamp
# param1: data frame name
# param2: data frame column
# return: data frame with unix timestamp column
def date_to_unix(data_frame , column_name, date_format):
    data_frame = data_frame. \
        withColumn(column_name,
                   unix_timestamp(column_name, date_format).cast("double").cast("timestamp"))
    return data_frame


# function to extract year from unix timestamp date
# param1: data frame name
# param2: current date format
# return: data frame with extracted year column
def date_to_year(data_frame, date_format):
    data_frame = data_frame.\
        withColumn("first_payment_year", functions.year(functions.to_date(data_frame.first_payment_date, date_format)))
    return data_frame


# function to extract year from unix timestamp date
# param1: data frame name
# param2: current date format
# return: data frame with extracted month column
def date_to_month(data_frame, date_format):
    data_frame = data_frame.\
        withColumn("first_payment_month", functions.month(functions.to_date(data_frame.first_payment_date, date_format)))
    return data_frame

# function to cast a column to integer type
# param1: data frame name
# param2: column name
# return: data frame with column as an integer type
def cast_to_int(data_frame, column_name):
    data_frame = data_frame.withColumn(
        column_name, data_frame[column_name].cast(IntegerType()))
    return data_frame

# function to cast a column to float type
# param1: data frame name
# param2: column name
# return: data frame with column as an float type
def cast_str_float(data_frame, column_name):
    data_frame = data_frame.withColumn(
        column_name, data_frame[column_name].cast("float"))
    return data_frame

# function to standardise various seller names using regex
# param1: data frame name
# param2: column name
# param3: regex pattern obeserved
# param4: standardised replacement name for the seller
# return: data frame with column as an float type
def standardize_seller_name(data_frame, column_name, pattern, seller_name):
    data_frame = data_frame. \
        withColumn("standardised_seller_name", regexp_replace(column_name, pattern, seller_name))
    return data_frame

# function fill null values with the median
# param1: data frame name
# param2: column name having integer type
# return: data frame with null values replace with the median values
def fill_null_with_median(data_frame, column_name , median_value):
    data_frame = data_frame.na.fill(column_name,str(median_value))
    return data_frame

# function to calculate the median of the new acquistion dataframe
def calculate_median(data_frame):
    # load credit score from s3 bucket
    credit_score_df = spark.read.format("json").option("header", "true").load(credit_score_url)
    # get credit score from recent file
    new_credit_score_df = data_frame.groupby("credit_score").\
        agg(functions.count("credit_score").alias("count_credit_score01"))
    # casting the credit score in both the dataframe as integer type
    credit_score_df = credit_score_df.\
        withColumn("count_credit_score", credit_score_df["count_credit_score"].cast(IntegerType()))
    new_credit_score_df = new_credit_score_df.\
        withColumn("count_credit_score01", new_credit_score_df["count_credit_score01"].cast(IntegerType()))
    # outer join to get all the credit score and the frequency
    df_credit = credit_score_df.join(new_credit_score_df, ["credit_score"], "outer")
    # get the total frequency for the credit score by adding the freq from both the data frames
    df_credit = df_credit.withColumn("new_count" , coalesce(df_credit.count_credit_score, lit('0'))
                                     + coalesce(df_credit.count_credit_score01,lit('0')).cast(IntegerType()))
    # add the cummulative counts as running total column and get the median row count
    df_credit_ordered = df_credit.select("credit_score", "new_count" ,functions.sum("new_count").
                                         over(Window.partitionBy().orderBy("credit_score")).
                                         alias("running_total").cast(IntegerType()))
    row_count = df_credit_ordered.select(functions.sum("new_count")).collect()[0][0]
      # find the position of the median
    if row_count % 2 == 0:
        row_num = int(row_count/2)
    else:
        row_num = int((row_count + 1)/2)
    # get the starting location of the median
    credit_median = df_credit_ordered.select(df_credit_ordered.credit_score, df_credit_ordered.
                                             new_count,df_credit_ordered.running_total,
                                             (df_credit_ordered.running_total < row_num).alias('median_loc')).cache()
    #get the median from the starting location
    median = credit_median.filter(credit_median.median_loc == False).head(1)[0][0]
    return median


# All transformation on source acquisition data frame
def acquisition_work_data_frame(source_data_frame, date_format):
    source_data_frame = date_to_unix(source_data_frame, "first_payment_date", date_format)
    source_data_frame = date_to_year(source_data_frame, date_format)
    source_data_frame = date_to_month(source_data_frame, date_format)
    source_data_frame = cast_to_int(source_data_frame, "credit_score")
    source_data_frame = cast_str_float(source_data_frame, "original_interest_rate" )
    # standardizing the seller name
    jp_morgan_pattern = r"([J][P]|[J]\.[P]\.)\s*MORGAN.*"
    source_data_frame = standardize_seller_name\
        (source_data_frame, "seller_name", jp_morgan_pattern ,"JP Morgan")
    return source_data_frame



# All transformations on source performance data frame
def performance_work_data_frame(source_data_frame):
    source_data_frame = cast_str_float(source_data_frame, "cur_interest_rate")
    # Get the average interest rate per loan sequence id
    work_data_frame = source_data_frame.groupBy("loan_seq_no"). \
        agg(functions.mean("cur_interest_rate").alias("avg_current_interest_rate"),
            functions.count("cur_interest_rate"))
    return work_data_frame


# applying functions to get work data frames from source data frames
fannie_wrk_acquisition_df = acquisition_work_data_frame(fannie_src_acquisition_df , "MM/yyyy")
freddie_wrk_acquisition_df = acquisition_work_data_frame(freddie_src_acquisition_df , "yyyyMM")
fannie_wrk_performance_df =  performance_work_data_frame(fannie_src_performance_df)
freddie_wrk_performance_df = performance_work_data_frame(freddie_src_performance_df)


# adding the agency_id column:
fannie_wrk_acquisition_df = fannie_wrk_acquisition_df.withColumn ( "agency_id", lit("0"))
freddie_wrk_acquisition_df = freddie_wrk_acquisition_df.withColumn ( "agency_id", lit("1"))
fannie_wrk_performance_df = fannie_wrk_performance_df.withColumn ( "agency_id", lit("0"))
freddie_wrk_performance_df = fannie_wrk_performance_df.withColumn ( "agency_id", lit("1"))


# ######################################################################################################################
# #output layer - get the data frame that can be stored in db
# #######################################################################################################################
# select only required columns from the acquisition dataframe
unified_acquisition_wrk_cols = ["loan_seq_no",
                                "channel",
                                "original_interest_rate",
                                "first_payment_date",
                                "first_payment_year",
                                "first_payment_month",
                                "number_of_borrowers",
                                "credit_score",
                                # "credit_outlier",
                                "first_time_homebuyer_flag",
                                "loan_purpose",
                                "property_type",
                                "number_of_units",
                                "occupancy_status",
                                "property_state",
                                "postal_code",
                                "product_type",
                                "agency_id"
                                ]

unified_performance_wrk_cols = ["loan_seq_no" ,
                                "avg_current_interest_rate",
                                "agency_id"
                                ]


# get the acquisition column
acquisition = freddie_wrk_acquisition_df.select(unified_acquisition_wrk_cols).\
    union(fannie_wrk_acquisition_df.select(unified_acquisition_wrk_cols))

# update the null credit score values with the median
median_credit_score = calculate_median(acquisition)
source_data_frame = fill_null_with_median(acquisition, "credit_score" ,median_credit_score)



url = 'postgresql://host-name:5432/database_name'

print("setting properties")
properties = {'user': 'postgres',
              'password': 'db',
              'driver':'org.postgresql.Driver' ,
              'numPartitions': '10000'}


# insert into performance tables
fannie_wrk_performance_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Fannie_performance',
                                     properties=properties,
                                     mode = 'append')

freddie_wrk_performance_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Freddie_performance',
                                     properties=properties,
                                     mode = 'append')


#insert into acquisition
fannie_wrk_acquisition_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Acquisition',
                                     properties=properties,
                                     mode = 'append')


