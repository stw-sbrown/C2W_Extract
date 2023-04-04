--
-- Subversion $Revision: 4751 $
--
--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      07/07/2016  S.Badhan   I-278. Intial version
-----------------------------------------------------------------------------------------
 
 CREATE TABLE BT_MISS_AG_SC
 (CD_COMPANY_SYSTEM     CHAR(4), 
	NO_ACCOUNT            NUMBER(9,0), 
	NO_PROPERTY           NUMBER(9,0), 
	CD_SERVICE_PROV       VARCHAR2(5), 
	NO_COMBINE_054        NUMBER(9,0), 
	NO_SERV_PROV          NUMBER(3,0), 
	ST_SERV_PROV          VARCHAR2(1), 
	DT_START              DATE, 
	DT_END                DATE, 
	NM_LOCAL_SERVICE      VARCHAR2(25), 
	CD_PROPERTY_USE       CHAR(1), 
	NO_COMBINE_024        NUMBER(9,0), 
	TP_CUST_ACCT_ROLE     CHAR(1), 
	NO_LEGAL_ENTITY       NUMBER(9,0), 
	NC024_DT_START        DATE, 
	NC024_DT_END          DATE, 
	IND_LEGAL_ENTITY      CHAR(1), 
	FG_TOO_HARD           CHAR(1), 
	CD_PROPERTY_USE_ORIG  VARCHAR2(1), 
	CD_PROPERTY_USE_CURR  VARCHAR2(1), 
	CD_PROPERTY_USE_FUT   VARCHAR2(1), 
	UDPRN                 NUMBER, 
	UPRN                  NUMBER, 
	VOA_REFERENCE         VARCHAR2(60), 
	SAP_FLOC              VARCHAR2(30), 
	CORESPID              VARCHAR2(10), 
	AGG_NET               CHAR(1), 
	FG_CONSOLIDATED       CHAR(1), 
	FG_TE                 CHAR(1), 
	FG_MECOMS_RDY         CHAR(1), 
	NO_PROPERTY_MASTER    NUMBER, 
	FG_NMM                CHAR(1)
  ) ;

  COMMENT ON TABLE BT_MISS_AG_SC  IS 'Aggregate property missing service provisions from sub properties';

  CREATE INDEX BT_MISS_AG_SC_IDX1 ON BT_MISS_AG_SC (NO_PROPERTY, CD_COMPANY_SYSTEM) ;
  CREATE INDEX BT_MISS_AG_SC_IDX2 ON BT_MISS_AG_SC (NO_ACCOUNT, CD_COMPANY_SYSTEM) ;
  CREATE INDEX BT_MISS_AG_SC_IDX3 ON BT_MISS_AG_SC (NO_LEGAL_ENTITY, CD_COMPANY_SYSTEM) ;
  
   commit;
   exit;


