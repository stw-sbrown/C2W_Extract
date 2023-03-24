--
-- 1_DDL_KeyGen_TVP054.sql
-- Create key gen table
-- Subversion $Revision: 4023 $
-- Date - 23/03/2016
-- Written By - Surinder Badhan
--

---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.01      11/04/2016  S.Badhan   Removed meter information and added serv prov status,
--                                    AGG_NET AND FG_CONSOLIDATED
-- V 1.00      23/03/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------

DROP TABLE BT_TVP054 PURGE;
--DROP PUBLIC SYNONYM BT_TVP054;

 CREATE TABLE BT_TVP054 
 (CD_COMPANY_SYSTEM       CHAR(4), 
	NO_ACCOUNT              NUMBER(9), 
	NO_PROPERTY             NUMBER(9), 
	CD_SERVICE_PROV         VARCHAR2(5), 
	NO_COMBINE_054          NUMBER(9), 
	NO_SERV_PROV            NUMBER(3), 
  ST_SERV_PROV            VARCHAR2(1),
	DT_START                DATE, 
	DT_END                  DATE, 
	NM_LOCAL_SERVICE        VARCHAR2(25), 
	CD_PROPERTY_USE         CHAR(1), 
	NO_COMBINE_024          NUMBER(9), 
	TP_CUST_ACCT_ROLE       CHAR(1), 
	NO_LEGAL_ENTITY         NUMBER(9), 
	NC024_DT_START          DATE, 
	NC024_DT_END            DATE, 
	IND_LEGAL_ENTITY        CHAR(1), 
	FG_TOO_HARD             CHAR(1), 
	CD_PROPERTY_USE_ORIG    VARCHAR2(1), 
	CD_PROPERTY_USE_CURR    VARCHAR2(1), 
	CD_PROPERTY_USE_FUT     VARCHAR2(1), 
	UDPRN                   NUMBER, 
	UPRN                    NUMBER, 
	VOA_REFERENCE           VARCHAR2(60), 
	SAP_FLOC                VARCHAR2(30), 
	CORESPID                VARCHAR2(10),
  AGG_NET                 CHAR(1), 
  FG_CONSOLIDATED         CHAR(1)
   )  ;

CREATE INDEX BT_TVP054_IDX1 ON BT_TVP054 (NO_PROPERTY, CD_COMPANY_SYSTEM); 

CREATE INDEX BT_TVP054_IDX2 ON BT_TVP054 (NO_ACCOUNT, CD_COMPANY_SYSTEM) ;

CREATE INDEX BT_TVP054_IDX3 ON BT_TVP054 (NO_LEGAL_ENTITY, CD_COMPANY_SYSTEM);

COMMENT ON TABLE BT_TVP054  IS 'Key Gen SPR BT_TVP054';
--CREATE PUBLIC SYNONYM BT_TVP054 FOR BT_TVP054;

commit;

exit;


