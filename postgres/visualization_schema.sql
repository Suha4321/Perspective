
--create required index
create INDEX loan_seq_idx on acquisition(loan_seq_no);
create index loan_seq_idx_1 on fannie_performance(loan_seq_no);
create index loan_seq_idx_2 on freddie_performance(loan_seq_no);

#############################################################################################################
--Create all the tables for visualization by state
#############################################################################################################

--create required index
create index state_idx on acquisition(property_state);

--create state table
select distinct property_state
into out.state
from acquisition;

-- create loan purpose table
select
  property_state
  ,loan_purpose 
  , count(loan_seq_no)
into out.loan_purpose
from acquisition 
group by property_state, loan_purpose;

--create first time home buyer table
select
   property_state
  ,first_time_homebuyer_flag 
  , count(loan_seq_no)
into out.first_time_homebuyer
from acquisition 
group by property_state, first_time_homebuyer_flag;

--create property type table
select
  property_state
  ,property_type 
  , count(loan_seq_no)
into out.property_type
from acquisition 
group by property_state, property_type;

--create table for the state, year, avg_current interest rate
select
  a.property_state ,
  a.first_payment_year ,
  avg(b.avg_current_interest_rate)  as avg_current_interest_rate,
  avg(a.credit_score) as avg_credit_score
into out.rate_compare
from acquisition a
inner join freddie_performance b
  on a.loan_seq_no = b.loan_seq_no
group by a.property_state , a.first_payment_year
order by 1 ,2 



