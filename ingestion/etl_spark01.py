
import os
import boto3
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as func
from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list, size

#pyspark settings
# conf = pyspark.SparkConf()
sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

fannie_performance_url = 's3a://mortgageinsight/fannie/performance/Performance_2018Q2.txt'
fannie_acquisition_url = 's3a://mortgageinsight/fannie/aquisition/Acquisition_2018Q2.txt'

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


# extract the data as tuple
def return_data_frame(url, col_name):
    data = sc.textFile(url)
    data_tuple = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    data_frame = sqlContext.createDataFrame(data_tuple)

    for c, n in zip(data_frame.columns,col_name ):
        data_frame = data_frame.withColumnRenamed(c, n)
    return data_frame


fannie_src_acquisition_df = return_data_frame(fannie_acquisition_url, fannie_src_acquisition_col)
fannie_src_performance_df = return_data_frame(fannie_performance_url, fannie_src_performance_col)



# print(fannie_src_acquisition_df.count())
# # 382846
# print(fannie_src_performance_df.count())
# # 4101014

# multiple counts of loan sq seen
# +------------+-----+
# | loan_seq_no|count|
# +------------+-----+
# |100171598886|   11|
# |100184220649|   11|
# |100479120895|   10|
# |101731318226|   12|
# |102002153004|   11|
# |102506541423|   12|
# |103572342216|   11|
# |103621263605|   11|
# |104210018766|    6|
# |104721213787|   11|
# |106446493563|   10|
# |106790893498|   12|
# |106836342462|   12|
# |107075129903|   10|
# |108096970099|   11|
# |108212638546|   10|
# |109286231470|   10|
# |109335311479|   12|
# |109965216964|   10|
# |110012043168|   12|
# +------------+-----+


# Cannot be joined by loan-seq as they are not the same.

A = fannie_src_acquisition_df.alias('A')
B = fannie_src_performance_df.alias('B')

# Add agency name
fannie_src_acquisition_df = fannie_src_acquisition_df.withColumn("agency_id", "fannie")
fannie_src_acquisition_df = fannie_src_acquisition_df.withColumn("agency_id", "fannie")


# # Add the reference dataframes

first_time_home_RDD = sc.parallelize([('Y',"Yes"),
                                      ('N',"No"),
                                     ('9',"Not applicable")])

loan_purpose_RDD = sc.parallelize([('P',"Purchase"),
                                      ('C',"Cash-out refinance"),
                                     ('R',"No cash-out refinance")
                                    ,('U',"Not specified refinance")])
channel_RDD = sc.parallelize([('R',"Retail"),
                                      ('B',"Broker"),
                                     ('C',"Correspondent")
                                    ,('T',"TPO Not specified")
                                    , ('9',"Not available")])

Property_type_RDD = sc.parallelize([('Co',"Condo"),
                                      ('PU',"PUD"),
                                     ('MH',"Manufactured")
                                    ,('SF',"1-4 Fee Simple")
                                    , ('99',"Not available")])

loan_purpose_data_frame = sqlContext.createDataFrame(loan_purpose_RDD)
loan_purpose_data_frame = loan_purpose_data_frame.withColumnRenamed(loan_purpose_data_frame.columns, ["loan_purpose_id", "loan_purpose"])

first_time_home_data_frame = sqlContext.createDataFrame(first_time_home_RDD)
first_time_home_data_frame = first_time_home_data_frame.withColumnRenamed(first_time_home_RDD.columns, ["loan_purpose_id", "loan_purpose"])

channel_data_frame = sqlContext.createDataFrame(channel_RDD)
channel_data_frame = channel_data_frame.withColumnRenamed(channel_data_frame.columns, ["loan_purpose_id", "loan_purpose"])

Property_type_data_frame = sqlContext.createDataFrame(Property_type_RDD)
Property_type_data_frame = Property_type_data_frame.withColumnRenamed(Property_type_data_frame.columns, ["loan_purpose_id", "loan_purpose"])



#


# left_join = A.join(B, A.loan_seq_no == B.loan_seq_no, how='left') # Could also use 'left_outer'
# left_join.filter(col('B.loan_seq_no').isNull()).show()
# This is a bad idea
# joined_data_frame = A.join(B, A.loan_seq_no == B.loan_seq_no, how='inner')
# joined_data_frame.count()


