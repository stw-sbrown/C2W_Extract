------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P0014.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	25/02/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL database
--
-- NOTES  			:	Place a summery at the top of the file with more detailed information 
--					where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    07/04/2016		M.Marron        Field MO_TARIFF.TARIFFNAME not long enough
--                                                  Drop constraint CHK02_SERVICECOMPONENTTYPE
--													Alter table and drop column TARIFFNAME.
--                                                  Add column TARIFFNAME VARCHAR(255)
--                                                  Add constraint CHK02_SERVICECOMPONENTTYPE
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--changes
--drop unique constraints on TABLES:
------------------------------------------------------
ALTER TABLE MO_METER_READING DROP CONSTRAINT FK_METER_READING_MANUFACT02;
ALTER TABLE MO_METER_READING DROP CONSTRAINT PK_METER_READING;
ALTER TABLE MO_METER_DPIDXREF DROP CONSTRAINT FK_METER_READING_MANUFACT01;
ALTER TABLE MO_METER_ADDRESS DROP CONSTRAINT FK_METERSERIALNUMBER_PK;
ALTER TABLE MO_METER DROP CONSTRAINT PK_MAN_SERIAL_REF_COMP;
alter table MO_METER DROP CONSTRAINT PK_MANUFACTURERSERIALNUM; 
-- remove the field
alter table MO_METER_READING drop column METER_READING_PK;
-- add new field
--ALTER TABLE MO_METER_READING ADD METERREF NUMBER(9) CONSTRAINT CH01_METERREF NOT NULL UNIQUE;
-- add constraint back on 
--RECREATE PRIMARY KEY ON MO_METER (COMPOSITE ON TWO COLUMNS)
ALTER TABLE MO_METER ADD CONSTRAINT PK_MAN_SERIAL_REF_COMP PRIMARY KEY(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
--ALTER TABLE MO_METER ADD CONSTRAINT PK_MANUFACTURERSERIALNUM UNIQUE (MANUFACTURERSERIALNUM_PK);
ALTER TABLE MO_METER_ADDRESS ADD CONSTRAINT FK_METERSERIALNUMBER_PK FOREIGN KEY (METERSERIALNUMBER_PK) REFERENCES MO_METER(MANUFACTURERSERIALNUM_PK);
ALTER TABLE MO_METER_DPIDXREF ADD CONSTRAINT FK_METER_READING_MANUFACT01 FOREIGN KEY (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
ALTER TABLE MO_METER_READING ADD CONSTRAINT FK_METER_READING_MANUFACT02  FOREIGN KEY (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
commit;
exit;

