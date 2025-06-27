/********************************************************************************************** 
 SPCS Demo Code for Report Generation Framework
 Purpose: Monitor the queries fired by Job Service and log entries                                 
***********************************************************************************************/

USE ROLE spcs_demo_role;
USE SCHEMA spcs_demo_db.spcs_sc;
USE WAREHOUSE spcs_wh;

-- Find queries executed by SPCS Job Service. It uses service user and runs under the custom role SPCS_DEMO_ROLE
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
  ORDER BY start_time;

-- Find the log entries from Service Logs
SELECT SYSTEM$GET_SERVICE_LOGS('pdf_gen_demo_batchjob_parallel', 0, 'main');

-- Find the log entries from Event Table
SHOW PARAMETERS LIKE 'event_table' IN ACCOUNT;

SELECT TIMESTAMP, RESOURCE_ATTRIBUTES, RECORD_ATTRIBUTES, VALUE
FROM snowflake.telemetry.events
WHERE timestamp > dateadd(day, -1, current_timestamp())
AND RESOURCE_ATTRIBUTES:"snow.service.name" = 'PDF_GEN_DEMO_BATCHJOB_PARALLEL'
ORDER BY TIMESTAMP DESC;