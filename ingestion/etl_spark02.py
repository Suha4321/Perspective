import os
import re
import sys
import boto3
import pyspark
# import psycopg2
from pyspark import sql
from pyspark.sql import Row
from pyspark.sql import column
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions
from pyspark.sql.functions import split
from pyspark.sql.functions import lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


#######################################################################################################################
# Source layer - extract columns from S3 bucket and convert it into a dataframe
#######################################################################################################################
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)
sqlContext = pyspark.SQLContext(sc)

fannie_acquisition_url = 's3a://mortgageinsight/fannie/aquisition/Acquisition_2010*.txt'
fannie_performance_url = 's3a://mortgageinsight/fannie/performance/Performance_2010*.txt'

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
    data_RDD = sc.textFile(url).map(lambda x: x.split('|'))
    data_frame = sqlContext.createDataFrame(data_RDD)

    for c, n in zip(data_frame.columns,col_name ):
        data_frame = data_frame.withColumnRenamed(c, n)
    return data_frame


fannie_src_acquisition_df = return_data_frame(fannie_acquisition_url, fannie_src_acquisition_col)
fannie_src_performance_df = return_data_frame(fannie_performance_url, fannie_src_performance_col)

# fannie_src_acquisition_df.show()
# fannie_src_performance_df.show()



#######################################################################################################################
# Work layer  - Do the requiered ETL operations :
#######################################################################################################################

# change the date values to unix timestamp in source file and get month and year
fannie_src_acquisition_df = fannie_src_acquisition_df.\
    withColumn("origination_date",unix_timestamp("origination_date", "MM/yyyy").cast("double").cast("timestamp"))

fannie_src_acquisition_df = fannie_src_acquisition_df.\
    withColumn("origination_year", functions.year(functions.to_date(fannie_src_acquisition_df.origination_date, "MM/yyyy")))

fannie_src_acquisition_df= fannie_src_acquisition_df.\
    withColumn("origination_month",functions.month(functions.to_date(fannie_src_acquisition_df.origination_date, "MM/yyyy")))


#replace the null credit score  with quantile medians
fannie_src_acquisition_df = fannie_src_acquisition_df.withColumn(
    "credit_score", fannie_src_acquisition_df["credit_score"].cast(IntegerType()))

# use approximate quantile to reduce calculation cost
credit_median = fannie_src_acquisition_df.approxQuantile("credit_score", [0.5], 0.25)
fannie_src_acquisition_df = fannie_src_acquisition_df.na.fill("credit_score",str(credit_median))


# cast string datatype into appropriate type
fannie_src_acquisition_df = fannie_src_acquisition_df.withColumn(
    "original_interest_rate",fannie_src_acquisition_df["original_interest_rate"].cast("float"))

fannie_src_acquisition_df = fannie_src_acquisition_df.withColumn(
    "credit_score",fannie_src_acquisition_df["credit_score"].cast(IntegerType()))

fannie_src_acquisition_df = fannie_src_acquisition_df.withColumn(
    "credit_score", fannie_src_acquisition_df["credit_score"].cast("float"))

fannie_src_performance_df = fannie_src_performance_df.withColumn(
    "cur_interest_rate", fannie_src_performance_df["cur_interest_rate"].cast("float"))



# get the mean current interest rate from the performance file, grouped by loan seq number
fannie_wrk_performance_df= fannie_src_performance_df.groupBy("loan_seq_no"). \
    agg(functions.mean("cur_interest_rate").alias("avg_current_interest_rate"))

fannie_acquisition_wrk_cols = ["loan_seq_no",
                                "channel",
                                # "seller_name",
                                "original_interest_rate",
                                # "original_upb",
                                # "original_loan_term",
                                "origination_date",
                                "origination_year",
                                 "origination_month",
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
                                "relocation_mortgage_indicator"
                               ]
#
# make dataframes
fannie_wrk_acquisition_df = fannie_src_acquisition_df.select(fannie_acquisition_wrk_cols)


# add agency id
fannie_wrk_performance_df = fannie_wrk_performance_df.withColumn ( "agency_id" , lit("0"))
fannie_wrk_acquisition_df = fannie_wrk_acquisition_df.withColumn ( "agency_id" , lit("0"))

#
# fannie_wrk_performance_df.show()
# fannie_wrk_acquisition_df.show()

#######################################################################################################################
# create reference tables
# #######################################################################################################################
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

# Property_type_RDD = sc.parallelize([('Co',"Condo"),
#                                       ('PU',"PUD"),
#                                      ('MH',"Manufactured")
#                                     ,('SF',"1-4 Fee Simple")
#                                     , ('99',"Not available")])


agency_df = sc.parallelize([Row(agency_id = "0" , agency_desc = "fannie"),
                             Row (agency_id = "1", agency_desc = "freddie")]).toDF()
#
# first_time_home_df.show()
# channel_df.show()
# property_type_df.show()
# agency_df.show()


#######################################################################################################################
# output layer
#######################################################################################################################
from pyspark.sql import DataFrameReader


url = 'postgresql://10.0.0.9:5432/mortgage_2010'

print("setting properties")
properties = {'user': 'user_name',
              'password': 'password',
              'driver':'org.postgresql.Driver' ,
              'numPartitions': '1000'}


print("inserting into performance")
# insert into performance
fannie_wrk_performance_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Performance',
                                     properties=properties,
                                     mode = 'overwrite')

print("inserting into acquisition")
#insert into acquisition
fannie_wrk_acquisition_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Acquisition',
                                     properties=properties,
                                     mode = 'overwrite')

print("inserting into first time home")
first_time_home_df.write.jdbc(url='jdbc:%s' % url,
                                     table='First_Time_Home',
                                     properties=properties,
                                     mode = 'overwrite')

print("inserting into channel home")
channel_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Channel_Home',
                                     properties=properties,
                                     mode = 'overwrite')

print("inserting into property type")
property_type_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Property_Type',
                                     properties=properties,
                                     mode = 'overwrite')


print("inserting into agency")
agency_df.write.jdbc(url='jdbc:%s' % url,
                                     table='Agency_Type',
                                     properties=properties,
                                     mode = 'overwrite')




#######################################################################################################################
# connection via psycopg2
#######################################################################################################################
# # # try:
# # #     connection = psycopg2.connect(host = "ec2-54-87-220-213.compute-1.amazonaws.com"
# # #                                   , database = "test"
# # #                                   , user = "postgres"
# # #                                   , password = "db"
# # #                                   , port = '5432')
# # #
# # # except:
# # #     print("Not connecting to postgres")
# # #
# # # cursor = connection.cursor()
# # #
# # #
# # # cur.executemany("""INSERT INTO bar(first_name,last_name) VALUES (%(first_name)s, %(last_name)s)""", namedict)
# # #
# # #
# # # connection.commit() # Important!
# # #
# # # cursor.execute('''SELECT * FROM playground''')
# # #
# # # print(cursor.fetchall())
# # #
# # # cursor.close()
# # # connection.close()

