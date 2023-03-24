-- TASK				: 	MOSL RDBMS DELETE of Supporting Lookup tables
--
-- AUTHOR         		: 	S.BADHAN
--
-- FILENAME       		: 	01_DDL_CREATE_MIG_TABLES.sql
--
-- CREATED        		: 	26/02/2016
--
-- Subversion $Revision: 4023 $
--	
-- DESCRIPTION 		   	: Create all MIG tables required for MOSL database
--

--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          	Author         		Description
-- ---------      	----------     -------            	 ------------------------------------------------
-- V0.01       		26/02/2016    	S.BADHAN	     	Initial version 
------------------------------------------------------------------------------------------------------------
--CREATE TABLES--

CREATE TABLE "MIG_BATCHSTATUS"
(
"NO_BATCH" NUMBER(4,0) NOT NULL ENABLE, 
"TS_START" TIMESTAMP (6) NOT NULL ENABLE, 
"DT_PROCESS" DATE NOT NULL ENABLE, 
"TS_UPDATE" TIMESTAMP (6) NOT NULL ENABLE, 
"BATCH_STATUS" VARCHAR2(3 BYTE) NOT NULL ENABLE, 
CONSTRAINT "MIG_PK_BATCHSTATUS" PRIMARY KEY ("NO_BATCH", "TS_START")
);


-- Updated MIG_CPLOG and set RECON_MEASURE_TOTAL" NUMBER(10,0) to resolve issue I-93 [Patch 9 under MO patch scripts]
CREATE TABLE "MIG_CPLOG" 
(
"NO_BATCH" NUMBER(4,0) NOT NULL ENABLE, 
"NO_INSTANCE" NUMBER(4,0) NOT NULL ENABLE, 
"TS_CREATED" TIMESTAMP (6) NOT NULL ENABLE, 
"NO_RECON_CP" NUMBER(5,0) NOT NULL ENABLE, 
"RECON_MEASURE_TOTAL" NUMBER(10,0) NOT NULL ENABLE, 
CONSTRAINT "MIG_PK_CPLOG" UNIQUE ("NO_BATCH", "NO_INSTANCE", "TS_CREATED", "NO_RECON_CP")
);


  
CREATE TABLE "MIG_CPREF" 
(
"NO_RECON_CP" NUMBER(5,0) NOT NULL ENABLE, 
"RECON_CONTROL_POINT" VARCHAR2(6 BYTE) NOT NULL ENABLE, 
"RECON_MEASURE" NUMBER(4,0) NOT NULL ENABLE, 
"RECON_MEASURE_DESC" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
CONSTRAINT "MIG_PK_CPREF" UNIQUE ("NO_RECON_CP")
);


  
CREATE TABLE "MIG_ERRORLOG" 
(
"NO_BATCH" NUMBER(4,0) NOT NULL ENABLE, 
"NO_INSTANCE" NUMBER(4,0) NOT NULL ENABLE, 
"TS_CREATED" TIMESTAMP (6) NOT NULL ENABLE, 
"NO_SEQ" NUMBER(5,0) NOT NULL ENABLE, 
"IND_LOG" VARCHAR2(1 BYTE) NOT NULL ENABLE, 
"NO_ERR" NUMBER(3,0) NOT NULL ENABLE, 
"TXT_KEY" VARCHAR2(30 BYTE) NOT NULL ENABLE, 
"TXT_DATA" VARCHAR2(100 BYTE), 
CONSTRAINT "MIG_PK_ERRORLOG" UNIQUE ("NO_BATCH", "NO_INSTANCE", "TS_CREATED", "NO_SEQ")
);

  
CREATE TABLE "MIG_ERRREF" 
(
"IND_LOG" VARCHAR2(1 BYTE) NOT NULL ENABLE, 
"NO_ERR" NUMBER(3,0) NOT NULL ENABLE, 
"TXT_ERR" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
CONSTRAINT "MIG_PK_ERRREF" UNIQUE ("IND_LOG", "NO_ERR")
);

CREATE TABLE "MIG_JOBREF" 
(
"NO_JOB" NUMBER(4,0) NOT NULL ENABLE, 
"NM_PROCESS" VARCHAR2(30 BYTE) NOT NULL ENABLE, 
"ERR_TOLERANCE" NUMBER(5,0) DEFAULT 0 NOT NULL ENABLE, 
"EXP_TOLERANCE" NUMBER(5,0) DEFAULT 0 NOT NULL ENABLE, 
"WAR_TOLERANCE" NUMBER(5,0) DEFAULT 100 NOT NULL ENABLE, 
"NO_COMMIT" NUMBER(6,0) DEFAULT 999999 NOT NULL ENABLE, 
"NO_STREAM" NUMBER(3,0) DEFAULT 50 NOT NULL ENABLE, 
"NO_RANGE_MIN" NUMBER(9,0) DEFAULT 1 NOT NULL ENABLE, 
"NO_RANGE_MAX" NUMBER(9,0) DEFAULT 999999999 NOT NULL ENABLE, 
CONSTRAINT "MIG_PK_JOBREF" PRIMARY KEY ("NO_JOB")
);
 
   
CREATE TABLE "MIG_JOBSTATUS" 
(
"NO_BATCH" NUMBER(4,0) NOT NULL ENABLE, 
"NO_INSTANCE" NUMBER(4,0) NOT NULL ENABLE, 
"TS_START" TIMESTAMP (6) NOT NULL ENABLE, 
"DT_PROCESS" DATE NOT NULL ENABLE, 
"TS_UPDATE" TIMESTAMP (6) NOT NULL ENABLE, 
"IND_STATUS" VARCHAR2(3 BYTE) NOT NULL ENABLE, 
"TXT_ARG" VARCHAR2(60 BYTE) NOT NULL ENABLE, 
"ERR_TOLERANCE" NUMBER(5,0) NOT NULL ENABLE, 
"EXP_TOLERANCE" NUMBER(5,0) NOT NULL ENABLE, 
"WAR_TOLERANCE" NUMBER(5,0) NOT NULL ENABLE, 
"NO_COMMIT" NUMBER(6,0), 
"NO_STREAM" NUMBER(3,0) NOT NULL ENABLE, 
"NO_RANGE_MIN" NUMBER(9,0), 
"NO_RANGE_MAX" NUMBER(9,0), 
CONSTRAINT "MIG_PK_JOBSTATUS" PRIMARY KEY ("NO_BATCH", "NO_INSTANCE", "TS_START")
);

  
-- ADD COMMENTS -- 
COMMENT ON TABLE "MIG_BATCHSTATUS"  IS 'MIG_BATCHSTATUS';
COMMENT ON TABLE "MIG_JOBSTATUS"  IS 'MIG_JOBSTATUS';
COMMENT ON TABLE "MIG_JOBREF"  IS 'MIG_JOBREF';
COMMENT ON TABLE "MIG_ERRREF"  IS 'MIG_ERRREF';
COMMENT ON TABLE "MIG_ERRORLOG"  IS 'MIG_ERRORLOG';
COMMENT ON TABLE "MIG_CPREF"  IS 'MIG_CPREF';
COMMENT ON TABLE "MIG_CPLOG"  IS 'MIG_CPLOG';

commit;
exit;