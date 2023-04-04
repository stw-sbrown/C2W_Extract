------------------------------------------------------------------------------
-- TASK				: 	Transform Trade Effluent 
--
-- AUTHOR         		: 	L.Smith
--
-- FILENAME       		: 	BT_P00002.sql
--
--
-- Subversion $Revision: 6031 $	
--
-- CREATED        		: 	13/05/2016
--	
-- DESCRIPTION 		   	: 	This file contains added TE functionality
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      ----------      -------         ------------------------------------------------
-- V0.01	        13/05/2016	  L.Smith         New processes to incorporate TE functionality.
--                                                1. DDL to create a lookup table LU_TE_REFDESC and indexes
--                                                2. DDL to create table BT_TE_WORKING and indexes
--                                                3. DDL to create table BT_TE_SUMMARY and indexes
--
-- V0.02          16/05/2016    L.Smith         Append columns to tables BT_TE_WORKING and BT_TE_SUMMARY
--                                              Remove references to moutran
-- V0.03          24/05/2016    L. Smith        Create new lookup table LU_TE_BILLING_CYCLE.
-- V0.04          03/06/2016    L. Smith        Use default tablespace
-- V0.05          07/06/2016    L. Smith        Resize columns.
-- V0.06          23/26/2016    L. Smith        I-253. Create a constraint to reject negative domestic allowances.
--                                              Amend ddl to create named constraints
-- V0.07          30/06/2016    L. Smith        New marker required BT_TE_SUMMARY.SEWERAGEVOLUMEADJMENTHOD
-- V0.08          11/07/2016    L. Smith        I-286. Remove constraints CH01_BTW_ACCOUNT_REF, CH01_BTS_NO_ACCOUNT, CH01_BTS_NO_ACCOUNT_REF
-- V0.08          13/07/2016    L. Smith        I-286. Remove constraint CH01_BTS_NO_ACCOUNT_REF
-- V0.09          15/08/2016    S.Badhan        I-320. Move create of LU_TE_REFDESC to LU create script - 01_DDL_MOSL_LOOKUP_TABLES_ALL.
-- V0.10          31/1-/2016    L.Smith         BT_TE_WORKING.fa_vol change form to NUMBER(14,2)
------------------------------------------------------------------------------------------------------------
--CHANGES
------------------------------------------------------

--
--2
--
PROMPT 'Create table BT_TE_WORKING'
CREATE TABLE "BT_TE_WORKING"
  ( "NO_IWCS"                  NUMBER              CONSTRAINT "CH01_BTW_NO_IWCS"           NOT NULL ENABLE,
    "PERIOD"                   NUMBER              CONSTRAINT "CH01_BTW_PERIOD"            NOT NULL ENABLE,
    "STAGE"                    NUMBER,
    "MET_REF"                  NUMBER              CONSTRAINT "CH01_BTW_MET_REF"           NOT NULL ENABLE,
    "NO_ACCOUNT"               NUMBER(9,0),            --         CONSTRAINT "CH01_BTW_NO_ACCOUNT"        NOT NULL ENABLE,
    "ACCOUNT_REF"              VARCHAR2(150 BYTE),     --         CONSTRAINT "CH01_BTW_ACCOUNT_REF"       NOT NULL ENABLE,
    "TE_REVISED_NAME"          VARCHAR2(200 BYTE)  CONSTRAINT "CH01_BTW_TE_REVISED_NAME"   NOT NULL ENABLE,
    "TE_CATEGORY"              VARCHAR2(100 BYTE)  CONSTRAINT "CH01_BTW_TE_CATEGORY"       NOT NULL ENABLE,
    "SERIAL_NO"                VARCHAR2(150 BYTE),
    "REFDESC"                  VARCHAR2(250 BYTE),
    "TARGET_REF"               VARCHAR2(150 BYTE),
    "UNIT"                     VARCHAR2(150 BYTE),
    "UNITS"                    NUMBER,
    "START_DATE"               DATE,
    "START_READ"               VARCHAR2(150 BYTE),
    "CODE"                     VARCHAR2(150 BYTE),
    "END_DATE"                 DATE,
    "END_READ"                 VARCHAR2(150 BYTE),
    "CODEA"                    VARCHAR2(150 BYTE),
    "TE"                       NUMBER,
    "TE_VOL"                   NUMBER,
    "MS"                       NUMBER,
    "MS_VOL"                   NUMBER,
    "REASON"                   VARCHAR2(150 BYTE),
    "OUW_YEAR"                 NUMBER,
    "TE_YEAR"                  NUMBER,
    "FA_YN"                    VARCHAR2(1 BYTE),
    "FA_VOL"                   NUMBER(14,2),
    "DA_YN"                    VARCHAR2(1 BYTE),
    "DA_VOL"                   NUMBER(12,0),
    "PA_YN"                    VARCHAR2(1 BYTE),
    "PA_PERC"                  NUMBER(5,2),
    "MDVOL_FOR_WS_METER_YN"    VARCHAR2(1 BYTE),
    "MDVOL_FOR_WS_METER_PERC"  NUMBER(5,2),
    "MDVOL_FOR_TE_METER_YN"    VARCHAR2(1 BYTE),
    "MDVOL_FOR_TE_METER_PERC"  NUMBER(5,2),
    "CALC_DISCHARGE_YN"        VARCHAR2(1 BYTE),
    "CALC_DISCHARGE_VOL"       NUMBER(14,2),
    "WS_VOL"                   NUMBER(12,0),
    "SUB_METER"                NUMBER(12,0),
    "TE_VOL_FILTERED"          NUMBER,
    "TE_VOL_CALC"              NUMBER(12,0),
    "OUW_VOL_CALC"             NUMBER(12,0),
CONSTRAINT "CH01_DA_VOL" CHECK (NVL(DA_VOL,0) >= 0) ENABLE
  )
  SEGMENT CREATION IMMEDIATE PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING STORAGE
  (
    INITIAL 26214400 NEXT 26214400 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  );

PROMPT 'Create index BT_TE_WORKING_idx1'
CREATE UNIQUE INDEX "BT_TE_WORKING_IDX1" ON "BT_TE_WORKING"
  (
    "NO_IWCS", "PERIOD", "STAGE", "MET_REF"
  )
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS STORAGE
  (
    INITIAL 26214400 NEXT 26214400 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  );


--
--3
--
PROMPT 'Create table BT_TE_SUMMARY'
CREATE TABLE "BT_TE_SUMMARY"
  (
    "NO_ACCOUNT"               NUMBER(9,0),           --         CONSTRAINT "CH01_BTS_NO_ACCOUNT"         NOT NULL ENABLE,
    "NO_IWCS"                  NUMBER              CONSTRAINT "CH01_BTS_NO_IWCS"            NOT NULL ENABLE,
    "NO_PROPERTY"              NUMBER(9,0),           --         CONSTRAINT "CH01_BTS_NO_PROPERTY"        NOT NULL ENABLE,
    "SUPPLY_POINT_CODE"        CHAR(2),               --         CONSTRAINT "CH01_BTS_SUPPLY_POINT_CODE"  NOT NULL ENABLE,
    "NO_LEGAL_ENTITY"          NUMBER(9,0),           --         CONSTRAINT "CH01_BTS_NO_LEGAL_ENTITY"    NOT NULL ENABLE,
    "NO_WORKING_ROWS"          NUMBER(9,0)         CONSTRAINT "CH01_BTS_NO_WORKING_ROWS"    NOT NULL ENABLE, 
    "DISTRICT"                 NUMBER,
    "SEWAGE"                   NUMBER,
    "SITECODE"                 NUMBER,
    "DISNO"                    NUMBER,
    "SITE_NAME"                VARCHAR2(150 BYTE),
    "SITE_ADD_1"               VARCHAR2(150 BYTE),
    "SITE_ADD_2"               VARCHAR2(150 BYTE),
    "SITE_ADD_3"               VARCHAR2(150 BYTE),
    "SITE_ADD_4"               VARCHAR2(150 BYTE),
    "SITE_PC"                  VARCHAR2(150 BYTE),
    "BILL_NAME"                VARCHAR2(150 BYTE),
    "BILL_ADD_1"               VARCHAR2(150 BYTE),
    "BILL_ADD_2"               VARCHAR2(150 BYTE),
    "BILL_ADD_3"               VARCHAR2(150 BYTE),
    "BILL_ADD_4"               VARCHAR2(150 BYTE),
    "BILL_PC"                  VARCHAR2(150 BYTE),
    "XREF"                     NUMBER,
    "SP_CODE"                  VARCHAR2(150 BYTE),
    "NO_ACCOUNT_REF"           VARCHAR2(150 BYTE),     --       CONSTRAINT "CH01_BTS_NO_ACCOUNT_REF"   NOT NULL ENABLE,
    "CHARGE_CODE"              VARCHAR2(150 BYTE),
    "CW_ADV"                   VARCHAR2(150 BYTE),
    "OTHER_USED_WATER"         VARCHAR2(150 BYTE),
    "DIS_DESC"                 VARCHAR2(150 BYTE),
    "AMMONIA"                  VARCHAR2(150 BYTE),
    "COD"                      NUMBER,
    "SS"                       NUMBER,
    "STATUS"                   VARCHAR2(150 BYTE),
    "CEASED_DATE"              DATE,
    "BILL_CYCLE"               NUMBER,
    "START_CYPHER"             DATE,
    "DATA_PROVIDE_METHOD"      VARCHAR2(150 BYTE),
    "DIS_START_DATE"           DATE,
    "TAME_AREA"                VARCHAR2(1 BYTE),
    "COL_CALC"                 VARCHAR2(250 BYTE)  CONSTRAINT "CH01_BTS_COL_CALC"         NOT NULL ENABLE,
    "TE_VOL"                   NUMBER(12,0),
    "OUW_VOL"                  NUMBER(12,0),
    "TE_DAYS"                  NUMBER(12,0),
    "OUW_DAYS"                 NUMBER(12,0),
    "FA_VOL"                   NUMBER(12,0),
    "DA_VOL"                   NUMBER(12,0),
    "PA_PERC"                  NUMBER(5,2),
    "MDVOL_FOR_WS_METER_PERC"  NUMBER(5,2),
    "MDVOL_FOR_TE_METER_PERC"  NUMBER(5,2),
    "CALC_DISCHARGE_VOL"       NUMBER(14,2),
    "SUB_METER"                NUMBER(12,0),
    "WS_VOL"                   NUMBER(12,0),
    "MO_CALC"                  NUMBER(12,0),
    "STW_CALC"                 NUMBER(12,0),
    "MO_STW_BALANCED_YN"       VARCHAR2(1 BYTE),
    "SEWERAGEVOLUMEADJMENTHOD" VARCHAR2(8 BYTE),
    CONSTRAINT "CH02_WORKING_ROWS" CHECK (NO_WORKING_ROWS > 0) ENABLE
  )
  SEGMENT CREATION IMMEDIATE PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING STORAGE
  (
    INITIAL 26214400 NEXT 26214400 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  );

PROMPT 'Create index BT_TE_SUMMARY_IDX1';
CREATE UNIQUE INDEX "BT_TE_SUMMARY_IDX1" ON "BT_TE_SUMMARY"
  (
    "NO_IWCS"
  )
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS STORAGE
  (
    INITIAL 26214400 NEXT 26214400 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  );


exit;




