
##What is this project about?

In any organizations, one can expect to see three broad categories of dashboards.
1.	Operational dashboards – These dashboards provide frequent updates on the process and are required to be updated very in a millisecond window. The data are mostly real time
2.	Analytical dashboards – These dashboards provide the answer to drill down questions. For example, a marketing team might want to know how much money was spent in each category and via what channel. The information is typically updated once a day.
3.	Strategic dashboards – These dashboards provide information on the overall health of the company/ function to the C suite. The information is typically updated once or twice in a week.
For all these kinds of dashboard, the typical turnover cycle varies from 2 week to 6 month or more. About 60% of the time is spent on ETL and about 20% is spent on schema design. All though the source data is same, but each team does their own ETL and schema design to satisfy their dash-boarding needs primarily because of data pipeline limitations. Can we use the big data pipeline to reduce the time and effort spent in the ETL process?

##How is it being implemented?

To illustrate this potential, I have taken single family home data from Fannie Mae and Freddie Mac (about 250 GB combined) and have cleaned, validated and standardized the data to provide a visualization. The project also generates table for visualisation based on user input. It is a feature to enable easier visual creation with tableau.

I have used S3,  Spark , PostgreSQL and tableau for this pipeline as shown below. The architecture accommodates the need for batch updates at the later stages.


####S3
Freddie Mac and Fannie Mae data are loaded into S3 from the following sources.
http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.page
https://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html

The data is stored in unzipped tab delimited text format.

####Spark
The data is read into pyspark from S3. The process also accommodates the need for quarterly updates to the data. Some of the operations performed are as follows -
1)	Convert the columns into proper data type
2)	Impute the null values
3)	Standardize the service provider names
4)	Flag outlier values
5)	Create the final dataset for the PostgreSQL

####PostgreSQL
The data from pyspark is transferred to PostgreSQL on AWS instance using the JDBC driver. The data stored is now about 24 GB in size and has about 39 million rows per table. An additional output schema is included to aggregate results by state for presenting in Tableau

####Tableau
The desktop Tableau version is connected to the AWS Postgres instance.

####Flask
User inputs the required feature to generate the base tables for the visualisation.

##Folder structure
1) Spark folder - The one time and the batch extract, load and transform (ELT) scripts are stored in this folder
2) Postgres folder - The input schema and the visualization schema table creation scripts are stored in this folder
3) Frontend - The scripts to get the user input and generate tables for visualisation are stored in this folder
