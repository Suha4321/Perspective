----------------------------------------------------------------------------------------------------------
-- function to extract distinct values for the parameter
-- input: user input column name
-- output: table with distinct values for the parameter
----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.select_parameter(_cols text)
  RETURNS TABLE(parameter_name text)
  LANGUAGE plpgsql
AS
$body$
BEGIN

RETURN QUERY EXECUTE
'SELECT DISTINCT ' || quote_ident(_cols) || '
 FROM   acquisition';

END;
$body$
  VOLATILE
  COST 100
  ROWS 1000;

------------------------------------------------------------------------------------------------------------
-- function to extract count by the first time home buyer flag for the  values for the parameter
-- input: user input column name
-- output: table with distinct values for the parameter , first time home buyer flag, and their count
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.select_count_by_first_time_homebuyer_flag(_cols text)
  RETURNS TABLE(parameter_name text, first_time_homebuyer_flag text, count_val bigint)
  LANGUAGE plpgsql
AS
$body$
BEGIN

  RETURN QUERY EXECUTE
  'SELECT  ' || quote_ident(_cols) || ', first_time_homebuyer_flag , COUNT(*)
   FROM   acquisition
   GROUP BY '|| quote_ident(_cols)  || ', first_time_homebuyer_flag
   ORDER BY 1,2';

END;
$body$
  VOLATILE
  COST 100
  ROWS 1000;
------------------------------------------------------------------------------------------------------------
-- function to extract count by the loan purpose for the  values for the parameter
-- input: user input column name
-- output: table with distinct values for the parameter , loan purpose, and their count
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.select_count_by_loan_purpose(_cols text)
  RETURNS TABLE(parameter_name text, loan_purpose text, count_val bigint)
  LANGUAGE plpgsql
AS
$body$
BEGIN

  RETURN QUERY EXECUTE
  'SELECT  ' || quote_ident(_cols) || ', loan_purpose , COUNT(*)
   FROM   acquisition
   GROUP BY '|| quote_ident(_cols)  || ', loan_purpose
   ORDER BY 1,2';

END;
$body$
  VOLATILE
  COST 100
  ROWS 1000;

------------------------------------------------------------------------------------------------------------
-- function to extract count by the number of borrowers for the  values for the parameter
-- input: user input column name
-- output: table with distinct values for the parameter , number of borrowers , and their count
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.select_count_by_number_of_borrowers(_cols text)
  RETURNS TABLE(parameter_name text, number_of_borrowers text, count_val bigint)
  LANGUAGE plpgsql
AS
$body$
BEGIN

  RETURN QUERY EXECUTE
  'SELECT  ' || quote_ident(_cols) || ', number_of_borrowers , COUNT(*)
   FROM   acquisition
   GROUP BY '|| quote_ident(_cols)  || ', number_of_borrowers
   ORDER BY 1,2';

END;
$body$
  VOLATILE
  COST 100
  ROWS 1000;

------------------------------------------------------------------------------------------------------------
-- function to extract count by the property state for the  values for the parameter
-- input: user input column name
-- output: table with distinct values for the parameter , property state , and their count
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.select_count_by_property_state(_cols text)
  RETURNS TABLE(parameter_name text, property_state text, count_val bigint)
  LANGUAGE plpgsql
AS
$body$
BEGIN

  RETURN QUERY EXECUTE
  'SELECT  ' || quote_ident(_cols) || ', property_state , COUNT(*)
   FROM   acquisition
   GROUP BY '|| quote_ident(_cols)  || ', property_state
   ORDER BY 1,2';

END;
$body$
  VOLATILE
  COST 100
  ROWS 1000;

------------------------------------------------------------------------------------------------------------
-- function to extract average interest rate and average credit score per year for the  values for the parameter
-- input: user input column name
-- output: table with distinct values for the parameter , year , average interest rate, average credit score
-------------------------------------------------------------------------------------------------------------

  CREATE OR REPLACE FUNCTION public.select_count_by_rates(_cols text)
  RETURNS TABLE(parameter_name text, first_payment_year integer, avg_current_interest_rate double precision, avg_credit_score numeric)
  LANGUAGE plpgsql
AS
$body$
BEGIN

  RETURN QUERY EXECUTE
  'SELECT  a.' || quote_ident(_cols) || ', a.first_payment_year  ,
   avg(b.avg_current_interest_rate)  as avg_current_interest_rate ,
   avg(a.credit_score) as avg_credit_score

   FROM   acquisition a
   inner join freddie_performance b
   on a.loan_seq_no = b.loan_seq_no
   GROUP BY '|| quote_ident(_cols)  || ', a.first_payment_year
   ORDER BY 1,2,3,4';


END;
$body$
  VOLATILE
  COST 100
  ROWS 1000;

