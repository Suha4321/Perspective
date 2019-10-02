from flask import request , render_template
from flaskexample import app
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import select
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine
from sqlalchemy import inspect
from sqlalchemy.types import Float
from sqlalchemy.types import Numeric
from sqlalchemy.sql import text


# define all the tables needed for the new schema
metadata = MetaData()

# create a table for parameter
mortgage_all = Table('parameter', metadata,
  Column('parameter_name', String)
)

# create a table for average interest rate counts by parameter
mortgage_all = Table('count_by_rate', metadata,
  Column('parameter_name', String),
  Column('first_payment_year', Integer),
  Column('avg_current_interest_rate', Float),
  Column('avg_credit_score', Numeric),
)

# create a table for first time home buyer counts by parameter
mortgage_all = Table('count_by_first_time_homebuyer_flag', metadata,
  Column('parameter_name', String),
  Column('first_time_homebuyer_flag', String),
  Column('count_val', Integer)
)

# create a table for loan purpose counts by parameter
mortgage_all = Table('count_by_loan_purpose', metadata,
  Column('parameter_name', String),
  Column('loan_purpose', String),
  Column('count_val', Integer)
)

# create a table for number of borrowers by parameter
mortgage_all = Table('count_by_number_of_borrowers', metadata,
  Column('parameter_name', String),
  Column('number_of_borrowers', String),
  Column('count_val', Integer)
)

# create a table for property state by parameter
mortgage_all = Table('count_by_property_state', metadata,
  Column('parameter_name', String),
  Column('property_state', String),
  Column('count_val', Integer)
)


# connect to postgres and create all the new tables for the schema
engine = create_engine('postgresql://username:password@hostname:5432/dbname')
metadata.create_all(engine)


# get user input to insert into required tables
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def index_post():
    text = request.form['text']
    processed_text = text.lower()
    with engine.connect() as con:

        # insert into the rates table
        postgreSQL_select_Query = "select * from public.select_count_by_rates ( %(col_name)s )"
        data = con.execute(postgreSQL_select_Query, ({"col_name" : processed_text}))

        for row in data:
            con.execute("""INSERT INTO "count_by_rate" VALUES (
                    %(parameter_name)s, 
                    %(first_payment_year)s,
                    %(avg_current_interest_rate)s, 
                    %(avg_credit_score)s);""", **row)


        # insert into the first_time_homebuyer table
        postgreSQL_select_Query = "select * from public.select_count_by_first_time_homebuyer_flag ( %(col_name)s )"
        data = con.execute(postgreSQL_select_Query, ({"col_name": processed_text}))

        for row in data:
            con.execute("""INSERT INTO "count_by_first_time_homebuyer_flag" VALUES (
                           %(parameter_name)s, 
                           %(first_time_homebuyer_flag)s,
                           %(count_val)s);""", **row)



        # insert into the loan purpose table
        postgreSQL_select_Query = "select * from public.select_count_by_loan_purpose ( %(col_name)s )"
        data = con.execute(postgreSQL_select_Query, ({"col_name": processed_text}))

        for row in data:
            con.execute("""INSERT INTO "count_by_loan_purpose" VALUES (
                           %(parameter_name)s, 
                           %(loan_purpose)s,
                           %(count_val)s);""", **row)


        # insert into the number of borrowers table
        postgreSQL_select_Query = "select * from public.select_count_by_number_of_borrowers ( %(col_name)s )"
        data = con.execute(postgreSQL_select_Query, ({"col_name": processed_text}))

        for row in data:
            con.execute("""INSERT INTO "count_by_number_of_borrowers" VALUES (
                           %(parameter_name)s, 
                           %(number_of_borrowers)s,
                           %(count_val)s);""", **row)


        # insert into the property state table
        postgreSQL_select_Query = "select * from public.select_count_by_property_state ( %(col_name)s )"
        data = con.execute(postgreSQL_select_Query, ({"col_name": processed_text}))

        for row in data:
            con.execute("""INSERT INTO "count_by_property_state" VALUES (
                            %(parameter_name)s, 
                            %(property_state)s,
                            %(count_val)s);""", **row)



        # insert into the parameter table
        postgreSQL_select_Query = "select * from public.select_parameter ( %(col_name)s )"
        data = con.execute(postgreSQL_select_Query, ({"col_name": processed_text}))

        for row in data:
            con.execute("""INSERT INTO "parameter" VALUES (
                            %(parameter_name)s);""", **row)

    return ('all tables created successfully')
