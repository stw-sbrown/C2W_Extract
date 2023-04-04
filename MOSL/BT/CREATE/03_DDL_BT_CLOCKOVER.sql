--
-- Subversion $Revision: 5189 $
-- DDL to re-create table BT_CLOCKOVER
-- Drop table first.  This will cause an error if the table does not exist, which can be ignored.

---------------------------- Modification History ----------------------------------------------------------
--
-- Version     Date         Author      Description
-- ---------   ----------   -------     --------------------------------------------------------------------
-- V0.01       17/08/2016   S.Badhan    I-320. BT_CLOCKOVER comments added from LU_P006.			
------------------------------------------------------------------------------------------------------------


--drop table BT_CLOCKOVER;

  CREATE TABLE BT_CLOCKOVER 
 (	STWMETERREF_PK NUMBER(32,0) NOT NULL, 
	METERREADDATE DATE NOT NULL, 
	METERREAD NUMBER(12,0), 
	ROLLOVERINDICATOR NUMBER(1,0) NOT NULL, 
	 CONSTRAINT CHK01_BTCLOCKOVERINDICATOR CHECK (ROLLOVERINDICATOR IN (0,1)) 
   );  
   
-- add  field comment

COMMENT ON COLUMN BT_CLOCKOVER.STWMETERREF_PK IS 'Target NO_EQUIPMENT (Meterthat this reading was for'; 
COMMENT ON COLUMN BT_CLOCKOVER.METERREADDATE IS 'Target TS_CAPTURED (Meter read date), so that the correct reading can be flagged as clock over'; 
COMMENT ON COLUMN BT_CLOCKOVER.METERREAD IS 'Target AM_READING (METER Read value) at clockover used to check that the correct reading is flagged'; 
COMMENT ON COLUMN BT_CLOCKOVER.ROLLOVERINDICATOR IS 'Rollover Indicator~~~D3020 - Default to 1 (Y). Indicates the meter read has rolled over for this meter.';

commit;
exit;

