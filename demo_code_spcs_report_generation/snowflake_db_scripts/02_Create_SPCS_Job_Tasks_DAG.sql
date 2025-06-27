/*********************************************************************************************************************************
 SPCS Demo Code for Report Generation Framework
 This code uses batch jobs (PrPr) to demonstrate the parallelism based on logical data partitions, not to be used in Production
 https://docs.snowflake.com/LIMITEDACCESS/snowpark-container-services/executing-batch-jobs
**********************************************************************************************************************************/

USE ROLE spcs_demo_role;
USE SCHEMA spcs_demo_db.spcs_sc;
USE WAREHOUSE spcs_wh;

/**** Step 1:  Get existing image repo details ****/
SHOW IMAGE REPOSITORIES; 

/**** Step 2: Build image and upload into the repo - Refer commands used in the Make file on the source code ****/
list @spcs_demo_db.spcs_sc.spcs_stage;
SHOW IMAGES IN IMAGE REPOSITORY SPCS_REPO;

/**** Step 3: Create Task DAG to execute Report Generation processes ****/

USE ROLE spcs_demo_role;
USE SCHEMA spcs_demo_db.spcs_sc;

-- Parent Task to populate transient table with data
CREATE OR REPLACE TASK spcs_pdf_generator_root_task
WAREHOUSE = spcs_wh
SCHEDULE = '60 MINUTES'
AS
BEGIN 
    CREATE OR REPLACE TRANSIENT TABLE DATE_MOD_TRANSIENT AS
          WITH DATE_MOD AS 
         (
             SELECT O_ORDERDATE, MAX(BIN_GROUP) AS BIN_GROUP
             FROM (
                 SELECT O_ORDERDATE, 
                    NTILE(5) OVER (ORDER BY O_ORDERDATE) AS BIN_GROUP
                    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
                   WHERE TRUNC(O_ORDERDATE, 'MM') IN ('1996-02-01') -- Hardcoded for demo
                )
             GROUP BY O_ORDERDATE
         )
        SELECT DISTINCT C_CUSTKEY, TO_VARCHAR(O.O_ORDERDATE, 'YYYYMM') AS ORD_MONTH,
            DT.BIN_GROUP 
            FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS O 
            JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER C
            ON O.O_CUSTKEY = C.C_CUSTKEY
            JOIN DATE_MOD DT
            ON DT.O_ORDERDATE = O.O_ORDERDATE
            LIMIT 100;
END;

-- Child Task to execute the SPCS Batch Job Service with replicas to achieve parallelism. Spec file defines the specification for container job
CREATE OR REPLACE TASK spcs_pdf_generator_child_task
WAREHOUSE = spcs_wh
AFTER spcs_pdf_generator_root_task
AS
EXECUTE IMMEDIATE
$$
    EXECUTE JOB SERVICE
    IN COMPUTE POOL spcs_compute_pool
    NAME=pdf_gen_demo_batchjob_parallel
    REPLICAS = 6
    FROM @spcs_demo_db.spcs_sc.spcs_stage
    SPEC='pdf_gen_demo_batchjob_parallel.yaml';
$$;

-- Finalizer task to move output report files from internal to external stage
CREATE OR REPLACE TASK spcs_pdf_generator_finalizer_task
WAREHOUSE = spcs_wh
FINALIZE = spcs_pdf_generator_root_task
AS
BEGIN
    COPY FILES INTO @snow_ext_stage/SPCS_BATCH_JOBS/ FROM @snow_int_stage/SPCS_BATCH_JOBS/ PATTERN = '.*';
    REMOVE @snow_int_stage/SPCS_BATCH_JOBS/; 
    RETURN 'Files moved to external stage and cleaned up from internal stage';
END;

SHOW TASKS;


/**** Step 4: Resume compute pool and execute task DAG ****/

DESCRIBE COMPUTE POOL spcs_compute_pool;
ALTER COMPUTE POOL spcs_compute_pool RESUME;

ALTER TASK spcs_pdf_generator_root_task RESUME;
ALTER TASK spcs_pdf_generator_child_task RESUME;
ALTER TASK spcs_pdf_generator_finalizer_task RESUME;

EXECUTE TASK spcs_pdf_generator_root_task;
DESCRIBE TASK spcs_pdf_generator_root_task;

--- Verify the transfer of files to external stage
list @snow_int_stage/;