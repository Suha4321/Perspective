On the python command line run "run.py" . This will generate a page for the user to input 
the column names for result aggregation. The script will generate
the following table for the required column names - 
1) parameter - All the distinct values for the user input column
2) count_by_loan_purpose - All values of loan purpose for the 
user input column and their respective counts.
3) count_by_first_time_homebuyer_flag - All values for the first time home buyer flag
 for the user input column and their respective counts.
4) count_by_number_of_borrowers - All values of the number of borrowers for the 
user input column and their respective counts.
5) count_by_property_state - All values of the property state for the 
user input column and their respective counts.
6) count_by_rate - Average interest rate and average credit score for the 
user input column by year.

The use case is tested with two columns  - 
1) property_type
2) channel