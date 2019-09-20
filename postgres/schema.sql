---Performance table
--CREATE SCHEMA IF NOT EXISTS mortgage AUTHORIZATION joe;

DROP TABLE  IF EXISTS  Performance;
DROP TABLE  IF EXISTS  Acquisition;
DROP TABLE  IF EXISTS  First_Time_Home;
DROP TABLE  IF EXISTS  Channel_Home;
DROP TABLE  IF EXISTS  Property_Type;
DROP TABLE  IF EXISTS  Agency_Type;


CREATE TABLE Performance(
            loan_seq_no                 Text,
            cur_interest_rate           FLOAT,
            mon_to_maturity             DATE,
            agency_id                   Text
--            PRIMARY KEY( loan_seq_no )
            );

---Acquisition table

CREATE TABLE  Acquisition(
            loan_seq_no Text ,
            original_interest_rate          FLOAT,
            origination_date                DATE,
            number_of_borrowers             INT,
            credit_score                    INT,
            first_time_homebuyer_flag       Text,
            loan_purpose                    Text,
            property_type                   Text,
            number_of_units                 INT,
            occupancy_status                Text,
            property_state                  Text,
            postal_code                     Text,
            product_type                    Text,
            co_borrower_credit_score        INT,
            mortgage_insurance_type         Text,
            relocation_mortgage_indicator   Text,
            agency_id                       Text
            );

---Create reference tables

CREATE TABLE First_Time_Home(
            first_time_home_id              TEXT    PRIMARY KEY,
            first_time_home_desc            TEXT
            );


CREATE TABLE Channel_Home(
            channel_id                      TEXT    PRIMARY KEY,
            channel_desc                    TEXT
            );

CREATE TABLE Property_Type(
            Property_type_id                TEXT    PRIMARY KEY,
            Property_type_desc              TEXT
            );

CREATE TABLE Agency_Type(
            agency_id                        TEXT    PRIMARY KEY,
            agency_desc                      TEXT
            );
