------------------------------------------------------------------------------
-- TASK					: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	LU_P00006.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	07/04/2016
--
-- Subversion $Revision: 4023 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    07/04/2016		M.Marron		Drop LU_CLOCKOVER
--													Create BT_CLOCKOVER one off
--													Amend BT_CLOCKOVER add Primary Key
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ==============
DROP TABLE LU_CLOCKOVER PURGE;
-- Create BT_CLOCKOVER
CREATE TABLE BT_CLOCKOVER
(
STWMETERREF_PK NUMBER(32) NOT NULL,
METERREADDATE DATE NOT NULL,
METERREAD NUMBER(12) ,
ROLLOVERINDICATOR NUMBER(1) NOT NULL
);
--
-- set Constraint 
ALTER TABLE BT_CLOCKOVER ADD CONSTRAINT CHK01_BTCLOCKOVERINDICATOR CHECK (ROLLOVERINDICATOR IN (0,1));
--
-- add  field comments
COMMENT ON COLUMN BT_CLOCKOVER.STWMETERREF_PK IS 'Target NO_EQUIPMENT (Meterthat this reading was for'; 
COMMENT ON COLUMN BT_CLOCKOVER.METERREADDATE IS 'Target TS_CAPTURED (Meter read date), so that the correct reading can be flagged as clock over'; 
COMMENT ON COLUMN BT_CLOCKOVER.METERREAD IS 'Target AM_READING (METER Read value) at clockover used to check that the correct reading is flagged'; 
COMMENT ON COLUMN BT_CLOCKOVER.ROLLOVERINDICATOR IS 'Rollover Indicator~~~D3020 - Default to 1 (Y). Indicates the meter read has rolled over for this meter.';
--
commit;
exit;

