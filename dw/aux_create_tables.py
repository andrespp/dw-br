"""aux_create_tables.py
"""

AUX_DWUPDATE = """
CREATE TABLE aux_dwupdate
(
  DWUPDATE_SK BIGSERIAL
, UPDATE TIMESTAMP
, HOSTNAME VARCHAR(30)
, ELAPSED_TIME FLOAT8
);
"""

DIM_DATE = """
CREATE TABLE dim_date
(
  DATE_SK BIGSERIAL
, YEAR_NUMBER SMALLINT
, YEARMO_NUMBER INTEGER
, MONTH_NUMBER SMALLINT
, DAY_OF_YEAR_NUMBER SMALLINT
, DAY_OF_MONTH_NUMBER SMALLINT
, DAY_OF_WEEK_NUMBER SMALLINT
, WEEK_OF_YEAR_NUMBER SMALLINT
, DAY_NAME VARCHAR(30)
, MONTH_NAME VARCHAR(30)
, MONTH_SHORT_NAME VARCHAR(3)
, QUARTER_NUMBER SMALLINT
, WEEKEND_IND BOOLEAN
, DAYS_IN_MONTH SMALLINT
, DAY_DESC TEXT
, DAY_DATE TIMESTAMP
, WEEK_OF_MONTH_NUMBER SMALLINT
, YEAR_HALF_NUMBER SMALLINT
, LEAP_YEAR BOOLEAN
)
;CREATE INDEX idx_dim_date_lookup ON dim_date(DATE_SK)
;
"""

# Data Warehouse Structure
DW_TABLES = dict(dim_date=DIM_DATE,
                 aux_dwupdate=AUX_DWUPDATE,
                )
