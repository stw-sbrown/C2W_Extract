--
-- Subversion $Revision: 4678 $
-- DDL to re-create table BT_METER_NETWORK
--
--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      01/07/2016  D.Cheung   CR_021 - Initial Draft
-----------------------------------------------------------------------------------------

  DROP TABLE BT_METER_NETWORK;

  CREATE TABLE BT_METER_NETWORK
  (	
      MAIN_STWPROPERTYNUMBER   NUMBER(9,0) CONSTRAINT CH01_BTMAINSTWPROPERTYNUMBER NOT NULL,
      MAIN_STWMETERREF         NUMBER(15,0),
      SUB_STWPROPERTYNUMBER    NUMBER(9,0) CONSTRAINT CH01_BTNOTSUBSTWPROPERTYNUMBER NULL,
      SUB_STWMETERREF          NUMBER(15,0) CONSTRAINT CH01_BTNOTSUBSTWMETERREF NULL,
      FG_ADD_SUBTRACT          CHAR(1 BYTE), 
      CORESPID	               VARCHAR2(10),
      FG_NMM	                 CHAR(1),
      MASTER_PROPERTY          NUMBER(9,0) CONSTRAINT CH01_MASTERPROPERTY NOT NULL,
      NET_LEVEL                NUMBER(4) CONSTRAINT CH01_BTNETLEVEL NOT NULL
  );  

  COMMENT ON TABLE BT_METER_NETWORK IS 'Table to build relationships for MO_METER_NETWORK';
  COMMENT ON COLUMN BT_METER_NETWORK.MAIN_STWPROPERTYNUMBER IS 'STW TARGET NO_PROPERTY Reference for MAIN meter';
  COMMENT ON COLUMN BT_METER_NETWORK.MAIN_STWMETERREF IS 'STW TARGET NO_PROPERTY Reference for MAIN meter';
  COMMENT ON COLUMN BT_METER_NETWORK.SUB_STWPROPERTYNUMBER IS 'STW TARGET NO_PROPERTY Reference for SUB meter';
  COMMENT ON COLUMN BT_METER_NETWORK.SUB_STWMETERREF IS 'STW TARGET NO_PROPERTY Reference for MAIN meter';
  COMMENT ON COLUMN BT_METER_NETWORK.FG_ADD_SUBTRACT IS 'Used to indicate if main or sub meter for billing';
  COMMENT ON COLUMN BT_METER_NETWORK.CORESPID IS 'CORESPID of MAIN property';
  COMMENT ON COLUMN BT_METER_NETWORK.FG_NMM IS 'Flag to indicate if NON-MARKET METER';
  COMMENT ON COLUMN BT_METER_NETWORK.MASTER_PROPERTY IS 'Top Master Property Number at Head of Network';
  COMMENT ON COLUMN BT_METER_NETWORK.NET_LEVEL IS 'Level indicator of SUB in network hierarchy';

commit;
/
exit;