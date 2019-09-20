import os
import re
import sys
import boto3
import pyspark
import psycopg2
from pyspark import sql
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions
from pyspark.sql.functions import split
from pyspark.sql.functions import lit



#######################################################################################################################
# Source layer - extract columns from S3 bucket and convert it into a dataframe
#######################################################################################################################
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)
sqlContext = pyspark.SQLContext(sc)

fannie_acquisition_url = 's3a://mortgageinsight/fannie/aquisition/Acquisition_2018Q2.txt'
fannie_performance_url = 's3a://mortgageinsight/fannie/performance/Performance_2018Q2.txt'

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


def return_data_frame(url, col_name):
    data_RDD = sc.textFile(fannie_acquisition_url).map(lambda x: x.split('|'))
    data_frame = sqlContext.createDataFrame(data_RDD)

    for c, n in zip(data_frame.columns,col_name ):
        data_frame = data_frame.withColumnRenamed(c, n)
    return data_frame


fannie_src_acquisition_df = return_data_frame(fannie_acquisition_url, fannie_src_acquisition_col)
fannie_src_performance_df = return_data_frame(fannie_performance_url, fannie_src_performance_col)

#######################################################################################################################
# Work layer  - Do the requiered ETL operations :
#   1) Add agency id columns to the dataframe
#   2) Extract only required columns
#######################################################################################################################


# define input columns
fannie_performance_wrk_cols = ["loan_seq_no",
                                "cur_interest_rate",
                                "mon_to_maturity"]

fannie_acquisition_wrk_cols = ["loan_seq_no",
                                # "channel",
                                # "seller_name",
                                "original_interest_rate",
                                # "original_upb",
                                # "original_loan_term",
                                "origination_date",
                                # "first_payment_date",
                                # "original_ltv",
                                # "original_cltv",
                                "number_of_borrowers",
                                # "original_dti",
                                "credit_score",
                                "first_time_homebuyer_flag",
                                "loan_purpose",
                                "property_type",
                                "number_of_units",
                                "occupancy_status",
                                "property_state",
                                "postal_code",
                                # "mip",
                                "product_type",
                                "co_borrower_credit_score",
                                "mortgage_insurance_type",
                                "relocation_mortgage_indicator"]

# make dataframes
fannie_wrk_performance_df = fannie_src_performance_df.select(fannie_performance_wrk_cols)
fannie_wrk_acquisition_df = fannie_src_acquisition_df.select(fannie_acquisition_wrk_cols)

# add agency names
fannie_wrk_performance_df = fannie_wrk_performance_df.withColumn ( "agency_id" , lit("0"))
fannie_wrk_acquisition_df = fannie_wrk_acquisition_df.withColumn ( "agency_id" , lit("0"))

fannie_wrk_performance_df.show()
fannie_wrk_acquisition_df.show()

#######################################################################################################################
# create reference tables
#######################################################################################################################
first_time_home_df = sc.parallelize([Row(first_time_home_id = 'Y',first_time_home_description = "Yes"),
                                        Row(first_time_home_id = 'N',first_time_home_description ="No"),
                                        Row(first_time_home_id ='9',first_time_home_description ="Not applicable")])\
                                        .toDF()


channel_df = sc.parallelize([Row(channel_id = 'R', channel_desc = "Retail"),
                                    Row(channel_id = 'B',channel_desc = "Broker"),
                                    Row(channel_id = 'C',channel_desc = "Correspondent"),
                                    Row(channel_id = 'T',channel_desc = "TPO Not specified"),
                                    Row(channel_id = '9', channel_desc= "Not available")]).toDF()

property_type_df = sc.parallelize([Row(property_type_id = 'Co', property_type_desc = "Condo"),
                                    Row (property_type_id = 'PU', property_type_desc = "PUD"),
                                    Row(property_type_id ='MH', property_type_desc= "Manufactured"),
                                    Row(property_type_id = 'SF', property_type_desc= "1-4 Fee Simple"),
                                    Row(property_type_id = '99', Property_type_desc= "Not available")]).toDF()

agency_df = sc.parallelize([Row(agency_id = "0" , agency_desc = "fannie"),
                             Row (agency_id = "1", agency_desc = "freddie")]).toDF()


#######################################################################################################################
# output layer
#######################################################################################################################
from pyspark.sql import DataFrameReader


url = 'postgresql://10.0.0.9:5432/test'

print("setting properties")
properties = {'user': 'postgres',
              'password': 'db',
              'driver':'org.postgresql.Driver'}


print("inserting into performance")
# insert into performance
fannie_wrk_performance_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Performance',
                                     properties=properties,
                                     mode = 'append')

print("inserting into acquisition")
# insert into acquisition
fannie_wrk_acquisition_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Acquisition',
                                     properties=properties,
                                     mode = 'overwrite')

print("inserting into first time home")
first_time_home_df.write.jdbc(url='jdbc:%s' % url,
                                     table='First_Time_Home',
                                     properties=properties,
                                     mode = 'overwrite')
#
print("inserting into channel home")
channel_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Channel_Home',
                                     properties=properties,
                                     mode = 'append')

print("inserting into property type")
property_type_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Property_Type',
                                     properties=properties,
                                     mode = 'append')


print("inserting into agency")
agency_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Agency_Type',
                                     properties=properties,
                                     mode = 'append')



