--
-- 1_DDL_KeyGen_TVP163.sql
-- Subversion $Revision: 4023 $
-- Create METER key gen table
-- Date - 06/04/2016
-- Written By - Dominic Cheung
--

----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.01      13/04/2016  S.Badhan   Added AGG_NET AND FG_CONSOLIDATED.
-- V 1.00      06/04/2016  D.Cheung   Intial Version.
----------------------------------------------------------------------------------------

DROP TABLE BT_TVP163 PURGE;
--DROP PUBLIC SYNONYM BT_TVP163;

 CREATE TABLE BT_TVP163 
  (CD_COMPANY_SYSTEM          CHAR(4), 
  	NO_ACCOUNT                NUMBER(9), 
  	NO_PROPERTY               NUMBER(9), 
    CD_SERVICE_PROV           VARCHAR2(5), 
  	NO_COMBINE_054            NUMBER(9), 
    NO_SERV_PROV              NUMBER(3), 
  	DT_START_054              DATE, 
    DT_END_054                DATE, 
  	NM_LOCAL_SERVICE          VARCHAR2(25), 
    CD_PROPERTY_USE           CHAR(1), 
  	NO_COMBINE_034            NUMBER(9), 
    DT_START_034              DATE, 
    DT_START_LR_034           DATE, 
    DT_END_034                DATE, 
    FG_ADD_SUBTRACT           CHAR(1),
    NO_UTL_EQUIP              CHAR(12),
    NO_EQUIPMENT              NUMBER(9),
    NO_COMBINE_043            NUMBER(9),
    NO_REGISTER               NUMBER(2),
    TP_EQUIPMENT              CHAR(2),
    CD_TARIFF                 CHAR(10),
    NO_TARIFF_GROUP           NUMBER(3),
    NO_TARIFF_SET             NUMBER(3),
    PC_USAGE_SPLIT            NUMBER(5,2),
    AM_BILL_MULTIPLIER        NUMBER(10,5),
    ST_METER_REG_115          CHAR(5),
  	NO_COMBINE_163_INST       NUMBER(9), 
    NO_PROPERTY_INST          NUMBER, 
  	TVP202_NO_SERV_PROV_INST  NUMBER, 
    IND_MARKET_PROP_INST      CHAR(1),
  	FG_TOO_HARD               CHAR(1),
    CORESPID                  VARCHAR2(10),
    AGG_NET                   CHAR(1), 
    FG_CONSOLIDATED           CHAR(1)    
   );

CREATE INDEX BT_TVP163_IDX1 ON BT_TVP163 (NO_PROPERTY, CD_COMPANY_SYSTEM); 

CREATE INDEX BT_TVP163_IDX2 ON BT_TVP163 (NO_EQUIPMENT, CD_COMPANY_SYSTEM); 

CREATE INDEX BT_TVP163_IDX3 ON BT_TVP163 (NO_ACCOUNT, CD_COMPANY_SYSTEM) ;

CREATE INDEX BT_TVP163_IDX4 ON BT_TVP163 (NO_PROPERTY_INST, NO_EQUIPMENT, CD_COMPANY_SYSTEM) ;

COMMENT ON TABLE BT_TVP163  IS 'Key Gen SPR_METER BT_TVP163';
--CREATE PUBLIC SYNONYM BT_TVP163 FOR BT_TVP163;

commit;

exit;


