/**************************************************************************************************** 
 SPCS Demo Code to setup the environment, create database objects, compute pool, virtual WH and role                                  
*****************************************************************************************************/

/**** Step 1: Setup DB, Warehouse and Compute Pool ***/
USE ROLE ACCOUNTADMIN;

CREATE ROLE spcs_demo_role;
CREATE DATABASE IF NOT EXISTS spcs_demo_db;
GRANT OWNERSHIP ON DATABASE spcs_demo_db TO ROLE spcs_demo_role COPY CURRENT GRANTS;

-- A warehouse is needed because the services (including job services) can run SQL DML statements (such as SELECT and INSERT). 
CREATE OR REPLACE WAREHOUSE spcs_wh WITH  
  WAREHOUSE_SIZE='X-SMALL';

GRANT USAGE ON WAREHOUSE spcs_wh TO ROLE spcs_demo_role;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE spcs_demo_role;

-- Create Compute Pool to execute the Container Job Service
CREATE COMPUTE POOL spcs_compute_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_SUSPEND_SECS = 600;
  
GRANT USAGE, MONITOR ON COMPUTE POOL spcs_compute_pool TO ROLE spcs_demo_role;
GRANT ROLE spcs_demo_role TO USER spcs_demo_user;

/**** Step 2: Setup Image Repo and Stage ****/
USE ROLE spcs_demo_role;
USE DATABASE spcs_demo_db;
USE WAREHOUSE spcs_wh;

CREATE SCHEMA IF NOT EXISTS spcs_sc;
CREATE IMAGE REPOSITORY IF NOT EXISTS spcs_repo; -- to store container images
CREATE STAGE IF NOT EXISTS spcs_stage -- -- to store service specification files
  DIRECTORY = ( ENABLE = true );

GRANT SERVICE READ ON IMAGE REPOSITORY SPCS_REPO TO ROLE spcs_demo_role;
GRANT SERVICE WRITE ON IMAGE REPOSITORY SPCS_REPO TO ROLE spcs_demo_role;

-- Validate
SHOW COMPUTE POOLS;
SHOW WAREHOUSES;
SHOW IMAGE REPOSITORIES;
SHOW STAGES; -- Image Repo is also listed as a Stage item
SHOW GRANTS ON IMAGE REPOSITORY SPCS_REPO;