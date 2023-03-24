--
-- 0_DDL_TVMNHHDTL.sql
--
-- Subversion $Revision: 5870 $	
--
-- Create non house hold key information table
-- Date - 12/04/2016
-- Written By - Surinder Badhan
--
-- check
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.03      17/10/2016  S.Badhan   Remove drop of table as done in separate drop script
-- V 1.02      13/09/2016  S.Badhan   Add MO flags.
-- V 1.01      25/08/2016  S.Badhan   Update table definition.
-- V 1.00      12/04/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------

CREATE TABLE TVMNHHDTL 
(	CD_COMPANY_SYSTEM           CHAR(4) NOT NULL, 
	NO_ACCOUNT                  NUMBER(9,0), 
	NO_PROPERTY                 NUMBER(9,0) NOT NULL, 
	CD_SERVICE_PROV             CHAR(5) NOT NULL, 
	ST_SERV_PROV                CHAR(1) NOT NULL, 
	NO_COMBINE_054              NUMBER(9,0), 
	NO_SERV_PROV                NUMBER(3,0) NOT NULL, 
	DT_START                    DATE, 
	DT_END                      DATE, 
	NM_LOCAL_SERVICE            VARCHAR2(25), 
	CD_PROPERTY_USE             CHAR(1) NOT NULL, 
	NO_COMBINE_024              NUMBER(9,0), 
	TP_CUST_ACCT_ROLE           CHAR(1), 
	NO_LEGAL_ENTITY             NUMBER(9,0), 
	NC024_DT_START              DATE, 
	NC024_DT_END                DATE, 
	IND_LEGAL_ENTITY            CHAR(1), 
	NO_COMBINE_163              NUMBER(9,0), 
	IND_INST_AT_PROP            CHAR(1), 
	TVP202_NO_PROPERTY          NUMBER(9,0), 
	TVP202_NO_SERV_PROV         NUMBER(3,0), 
	FG_TOO_HARD                 CHAR(1), 
	PHASE                       CHAR(1), 
	CORESPID                    VARCHAR2(10), 
	AGG_NET                     CHAR(1), 
	NO_PROPERTY_MASTER          NUMBER(9,0), 
	ID_OWC                      VARCHAR2(30),
  SUPPLY_POINT_CATEGORY       CHAR(1),
  FG_MO_RDY                   CHAR(1),
  FG_MO_LOADED                CHAR(1),
  TS_MO_LOADED                DATE,
  FG_SAP_RDY                  CHAR(1),
  FG_SAP_LOADED               CHAR(1),
  TS_SAP_LOADED                DATE    
   ) ;
  
CREATE INDEX XVMNHH1 ON TVMNHHDTL (NO_PROPERTY, CD_COMPANY_SYSTEM); 

CREATE INDEX XVMNHH2 ON TVMNHHDTL (NO_ACCOUNT, CD_COMPANY_SYSTEM) ;

CREATE INDEX XVMNHH3 ON TVMNHHDTL (NO_LEGAL_ENTITY, CD_COMPANY_SYSTEM) ;

CREATE INDEX XVMNHH5 ON TVMNHHDTL (TVP202_NO_PROPERTY, TVP202_NO_SERV_PROV, CD_COMPANY_SYSTEM) ;
  
COMMENT ON TABLE TVMNHHDTL  IS 'Non Household key information';

commit;

exit;

