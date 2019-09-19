Perspective:

Organisations use different kinds of dashboard to monitor the activities. Depending on the business requirement, the dashboarding requirement changes. This in turn means that the team starts from scratch to perform the ETL,
database design and dash-boarding process. To make this process simpler by identifying the commonalities across the process, the pipeline addresses different dash-boarding needs.


Data

Single family loan data avilable from Freedie Mac and Fannie Mae was used for the purpose.
The data links can be found here -
http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.page
https://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html

The data was downloaded in a zip file and transfered to an ec2 instance via scp. The data was unzip and then transfered to S3 bucket

Ingestions:
This involves reading the data from S3, cleaning the values , adding reference data , building schema for the database.



