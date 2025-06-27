""" Main PDF Generator Module that will be built as an image and loaded into SPCS to generate PDF reports - Demo using Batch Jobs Parallelism
Arguments: From YAML file"""

import argparse
import logging
import os
import sys
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import *
from pdf_gen.pdf_generator import pdfgen 

# Environment variables below will be automatically populated by Snowflake.
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Custom environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE") 

# PrPr Environment variable from SPCS 
SNOWFLAKE_JOB_INDEX = os.getenv("SNOWFLAKE_JOB_INDEX")  ## This variable holds the value of the replica number

pdf_title = "Sample PDF Report Generator - Powered by Snowflake's SPCS"
base_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_path, 'cust')
os.makedirs('cust', exist_ok=True)


def get_arg_parser():
    #Input argument list.
    parser = argparse.ArgumentParser()
    parser.add_argument("--month_part", required=True, help="month filter")
    return parser 


def get_logger():
    """
    Get a logger for local logging.
    """
    logger = logging.getLogger("job-tutorial")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_login_token():
    """
    Read the login token supplied automatically by Snowflake. These tokens
    are short lived and should always be read right before creating any new connection.
    """
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def get_connection_params():
    """
    Construct Snowflake connection params from environment variables.
    """
    if os.path.exists("/snowflake/session/token"):
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "authenticator": "oauth",
            "token": get_login_token(),
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    else:
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "role": SNOWFLAKE_ROLE,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }


if __name__ == "__main__":
    # Parse input arguments
    args = get_arg_parser().parse_args()
    month_part = args.month_part
    task_num = 0
   
    logger = get_logger()
    logger.info(f"Job started in SPCS instance # {SNOWFLAKE_JOB_INDEX}")
    query = f"""
                SELECT C_NAME, C_ACCTBAL, O_ORDERDATE, O_TOTALPRICE, 
                O_ORDERPRIORITY, C.C_CUSTKEY
                FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS O 
                JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER C 
                ON O.O_CUSTKEY = C.C_CUSTKEY 
                JOIN SPCS_DB.SPCS_SC.DATE_MOD_TRANSIENT NT 
                ON NT.C_CUSTKEY = C.C_CUSTKEY
                WHERE NT.BIN_GROUP = {SNOWFLAKE_JOB_INDEX}; -- This is where the logical data partitions are picked up corresponding to the replica #
            """ 

    # Start a Snowflake session, run the query and write results to specified table
    with Session.builder.configs(get_connection_params()).create() as session:
        # Print out current session context information.
        database = session.get_current_database()
        schema = session.get_current_schema()
        warehouse = session.get_current_warehouse()
        role = session.get_current_role()
        print(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}")
        logger.info(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}"
        )
        df = session.sql(query).to_pandas()
        cust_list = list(df['C_CUSTKEY'].unique())
        
        for cust_part in cust_list:
            pdf_table = pdfgen(pdf_title) # This is a PDF Generator class object 
            print(file_path)        
            output_filename = f"{file_path}/customer_report-{cust_part}.pdf"
            print(output_filename)
            logger.info(f"Report generation for customer {cust_part} on file {output_filename} in progress")
            fil_df = df[df['C_CUSTKEY']==cust_part]
            pdf_table.generate_pdf(fil_df)
            pdf_table.save_pdf(output_filename)            
            put_result = session.file.put(
                    local_file_name=output_filename,
                    stage_location=f'@snow_int_stage/SPCS_BATCH_JOBS/{SNOWFLAKE_JOB_INDEX}',
                    auto_compress=False,
                    source_compression='NONE',
                    overwrite=True
                )
    logger.info("Job finished")