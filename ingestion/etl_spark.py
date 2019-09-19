

import os
import boto3
import pyspark
import numpy
from pyspark.sql import SparkSession
from pyspark import SparkContext



#pyspark settings
# conf = pyspark.SparkConf()
sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

fannie_performance_url = 's3a://mortgageinsight/fannie/performance/Performance_2018Q2.txt'
fannie_acquisition_url = 's3a://mortgageinsight/fannie/aquisition/Acquisition_2018Q2.txt'


fannie_src_aquisition_col = ["loan_seq_no",
                            "channel",
                            "seller_name",
                            "original_interest_rate",
                            "original_upb",
                            "original_loan_term",
                            "first_payment_date",
                            "original_ltv",
                            "original_cltv",
                            "number_of_borrowers",
                            "original_dti",
                            "credit_score",
                            "first_time_homebuyer_flag",
                            "loan_purpose",
                            "property_type",
                            "num_of_units",
                            "occupancy_status",
                            "property_state",
                            "postal_code",
                            "mip",
                            "product_type"]


state_cols = ["property_state", "property_state_id"]


#
# fannie_src_performance_col = ["loan_seq_no",
#                                "report_period",
#                                "servicer_name",
#                                "cur_interest_rate",
#                                "cur_actual_upb",
#                                "loan_age",
#                                "mon_to_maturity",
#                                "adjusted_mon_to_maturity",
#                                "maturity_date",
#                                "msa",
#                                "cur_delinquency",
#                                "modification",
#                                "zero_balance_code",
#                                "zero_balance_date",
#                                "last_paid_installment_date",
#                                "foreclosure_date",
#                                "disposition_date",
#                                "foreclosure_costs",
#                                "property_preservation_repair_costs",
#                                "asset_recovery_costs",
#                                "miscellaneous_expenses",
#                                "associated_taxes",
#                                "net_sale_proceeds",
#                                "credit_enhancement_proceeds",
#                                "repurchase_make_whole_proceeds",
#                                "other_foreclousure_proceeds",
#                                "non_interest_bearing_upb",
#                                "principal_forgiveness_amount",
#                                "repurchase_make_whole_proceeds_flag",
#                                "foreclousure_principle_write_off_amount",
#                                "servicing_activity_indicator"]


# extract the data as tuple
data = sc.textFile(fannie_acquisition_url )
data_tuple = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
data_tuple_2 = data_tuple.map( lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],x[21]))





# extract the state from the data and add id to the states
state_rdd = data_tuple_2.map(lambda x : (x[17],1)).reduceByKey(lambda x,y: x+y).map(lambda x:x[0]).zipWithUniqueId()
state_rdd_tolist = state_rdd.collect()


####################################################################################################################################
#Adding the reference schema
####################################################################################################################################

# create reference tables
first_time_home_RDD = sc.parallelize([('Y',"Yes"),
                                      ('N',"No"),
                                     ('U',"Unknown")])

loan_purpose_RDD = sc.parallelize([('P',"Purchase"),
                                      ('C',"Cash-out refinance"),
                                     ('R',"No cash-out refinance")
                                    ,('U',"Not specified refinance")])





P, c, r, u

def first_time_home(line):
    id = line[0]
    first_time = line[13]
    if first_time == "Y":
        yield id, first_time,'1'
    if first_time == "N":
        yield id,first_time, '2'
    else:
        yield id,first_time, '0'

first_time_home_RDD = data_tuple.map(lambda x: (first_time_home(x)))
print(first_time_home_RDD.take(10))


def

# def property_id(line):
#     id = line[0]
#     property_type = line[15]
#     if property_type == "Y":
#         yield id,'1'
#     if property_type == "N":
#         yield id, '2'
#     else:
#         yield id, '0'
#
# loan_purpose_map = data_tuple.flatMap(lambda x: loan_purpose(x))


#
# print(loan_purpose_map.take(100))




# state_rdd_with_id = state_rdd.map(lambda x:x[0])
# state_rdd_with_id = state_rdd.map(lambda x:x[0]).zipWithUniqueId()

# state_list = state_rdd_with_id.collect()
# print(state_list)

# assign stateid to the main df
#
#
#
# # create function to assign col names
# def assign_col_names(data_frame,col):
#     for c, n in zip(data_frame.columns,fannie_src_aquisition_col ):
#         data_frame = data_frame.withColumnRenamed(c, n)
#     return data_frame
#
# # create Dataframe from RDD
# fannie_acuisition_data_frame = sqlContext.createDataFrame(data_tuple_2)
# state_dataframe = sqlContext.createDataFrame(state_rdd_with_id)
#
# # provide column names to dataframe
# fannie_src_aquisition_df = assign_col_names(fannie_acuisition_data_frame, fannie_src_aquisition_col)
# state_df = assign_col_names(state_dataframe, state_cols)


# if fannie_acuisition_data_frame.property_state in state_rdd_with_id.keys():
#     print('yes')





#
# print(state_rdd01.take(10))
# # # state_data_frame = sqlContext.createDataFrame(data_RDD_state_name)



# data_frame = sqlContext.createDataFrame(data_tuple_2)
# for c, n in zip(data_frame.columns,fannie_src_aquisition_col ):
#     data_frame = data_frame.withColumnRenamed(c, n)


#
# # Create the Departments
# credit_bin1 = Row(credit_score_id='0', credit_score_bucket='Not Available')
# credit_bin2 = Row(credit_score_id='1', credit_score_bucket='Very Poor')
# credit_bin3 = Row(credit_score_id='2', credit_score_bucket='Fair')
# credit_bin4 = Row(credit_score_id='3', credit_score_bucket='Good')
# credit_bin5 = Row(credit_score_id='4', credit_score_bucket='Very Good')
# credit_bin6 = Row(credit_score_id='5', credit_score_bucket='Exceptional')
#
#


# def return_data_frame(url, col_name):
#     data = sc.textFile(url)
#     data_tuple = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x]).map(lambda x:(x,1))
#     data_frame = sqlContext.createDataFrame(data_tuple)
#
#     for c, n in zip(data_frame.columns,col_name ):
#         data_frame = data_frame.withColumnRenamed(c, n)
#     return data_frame



# fannie_src_aquisition_df.select('loan_purpose').distinct().collect()
# # create the dataframe
# fannie_src_aquisition_df = return_data_frame(fannie_acuisition_url, fannie_src_aquisition_col)
# fannie_src_performance_df = return_data_frame(fannie_performance_url, fannie_src_performance_col)
# print(fannie_src_performance_df.show())

#
# rdd = fannie_src_aquisition_df.rdd.map(tuple)
# print(rdd.take(10))


# fannie_src_performance_df
# # prints the schema
# data_frame.printSchema()
# data_frame.select("auctionid").distinct.count

# # bins for the credit_score

# 800-850	Exceptional -1
# 740-799	Very Good - 2
# 670-739	Good - 3
# 580-669	Fair - 4
# 300-579	Very Poor -5

# def credit_score_bin(line):
#     id = line[0]
#     credit_score = line[12]
#
#     if credit_score < '':
#         yield id, 'NA'
#
#     if credit_score < '580':
#         yield id, 'Very Poor'
#
#     elif credit_score < '670':
#         yield id, 'Fair'
#
#     elif credit_score < '740':
#         yield id, 'Good'
#
#     elif credit_score < '800':
#         yield id, 'Very Good'
# #
#     else:
#         yield id, 'Exceptional'
#
# def agency_col(line):
#
#
# detect = data_1.flatMap(lambda x: credit_score_bin(x))
# print(detect.take(10))