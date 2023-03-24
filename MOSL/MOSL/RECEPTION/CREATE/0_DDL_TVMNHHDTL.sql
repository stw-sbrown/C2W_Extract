--
-- 0_DDL_TVMNHHDTL.sql
--
-- Subversion $Revision: 4023 $	
--
-- Create non house hold key information table
-- Date - 12/04/2016
-- Written By - Surinder Badhan
--

---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.00      12/04/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------

DROP TABLE TVMNHHDTL PURGE;
DROP PUBLIC SYNONYM TVMNHHDTL;

 CREATE TABLE TVMNHHDTL 
 (CD_COMPANY_SYSTEM               CHAR(4), 
	NO_ACCOUNT                      NUMBER(9), 
	NO_PROPERTY                     NUMBER(9), 
	CD_SERVICE_PROV                 CHAR(5), 
	NO_COMBINE_054                  NUMBER(9), 
	NO_SERV_PROV                    NUMBER(3), 
  ST_SERV_PROV                    VARCHAR2(1),
	DT_START                        DATE, 
	DT_END                          DATE, 
	NM_LOCAL_SERVICE                VARCHAR2(25), 
	CD_PROPERTY_USE                 CHAR(1), 
	NO_COMBINE_024                  NUMBER(9), 
	TP_CUST_ACCT_ROLE               CHAR(1), 
	NO_LEGAL_ENTITY                 NUMBER(9), 
	NC024_DT_START                  DATE, 
	NC024_DT_END                    DATE, 
	IND_LEGAL_ENTITY                CHAR(1), 
	NO_COMBINE_163                  NUMBER(9), 
	IND_INST_AT_PROP                CHAR(1), 
	TVP202_NO_PROPERTY              NUMBER, 
	TVP202_NO_SERV_PROV             NUMBER, 
	FG_TOO_HARD                     CHAR(1) )  ;

CREATE INDEX TVMNHHDTL_IDX1 ON TVMNHHDTL
(NO_PROPERTY, CD_COMPANY_SYSTEM);

CREATE INDEX TVMNHHDTL_IDX2 ON TVMNHHDTL
(NO_ACCOUNT, CD_COMPANY_SYSTEM);

CREATE INDEX TVMNHHDTL_IDX3 ON TVMNHHDTL
(NO_LEGAL_ENTITY, CD_COMPANY_SYSTEM);

COMMENT ON TABLE TVMNHHDTL  IS 'Non Household key information';
CREATE PUBLIC SYNONYM TVMNHHDTL FOR TVMNHHDTL;

commit;

exit;

