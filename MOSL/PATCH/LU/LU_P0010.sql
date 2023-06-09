------------------------------------------------------------------------------
-- TASK			: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Dominic Cheung
--
-- FILENAME       		: 	LU_P0010.sql
--
--
-- Subversion $Revision: 6375 $	
--
-- CREATED        		: 	28/06/2016
--
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           	 Author         	Description
-- ---------      ----------     	 -------        	------------------------------------------------
-- V0.01		    	28/06/2016		D.Cheung		        Create new lookup table for Meter Manufacturer	 
-- V0.02          30/06/2016    D.Cheung            Add Cast Conversion													
-- V0.03          04/08/2016    D.Cheung            CR_33 - Change KENT METERS LIMITED mapping from KENT to KENTMETERS due to duplicates from other Wholesalers
-- V0.04          21/11/2016    K.Burton            SAP Defect 387 - Additional mappings added for OWC file import
------------------------------------------------------------------------------------------------------------

--Patch Script ------------


-- Create LU_METER_MANUFACTURER table
DROP TABLE LU_METER_MANUFACTURER PURGE;
CREATE TABLE LU_METER_MANUFACTURER
(
MANUFACTURER_PK VARCHAR2(32) CONSTRAINT CH01_MANUFUFACTURER_PK NOT NULL,
MANUFCODE       VARCHAR2(32) CONSTRAINT CH01_MANUFCODE NOT NULL
);
--
-- set primary key
ALTER TABLE LU_METER_MANUFACTURER ADD CONSTRAINT PK_METER_MANUFACTURER PRIMARY KEY (MANUFACTURER_PK);
--
-- add table and field comments
COMMENT ON TABLE LU_METER_MANUFACTURER IS 'This table holds the mappings between system manufacturer value and MOSL compliant mappings';
COMMENT ON COLUMN LU_METER_MANUFACTURER.MANUFACTURER_PK IS 'Original system Meter Manufacturer';
COMMENT ON COLUMN LU_METER_MANUFACTURER.MANUFCODE IS 'Mapped Manufacturer Code for MOSL output';

INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('FROST METERS LIMITED', 'FROST');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('FUSION METERS LIMITED',	'FUSION');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('GLENFIELD METERS LIMITED','GLENFIELD');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('KENT METERS LIMITED','KENTMETERS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('LEEDS METERS LIMITED','LEEDS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('NEPTUNE METERS LIMITED','NEPTUNE');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('PONT-A-MOUSSON','PONTAMOUSSON');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('RADIO ACTARIS','ACTARIS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('SCHLUMBERGER WATER METERS LTD','SCHLUMBERGER');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('SENSUS','SENSUS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('SIEMENS','SIEMENS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('SMARTMETER','SMARTMETER');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('SOC-A-MOUSSON','SOCAMOUSSON');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('STELLA METERS LIMITED','STELLA');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('UNKNOWN','UNKNOWN');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK, MANUFCODE) VALUES ('CAST CONVERSION','UNKNOWN');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('ARAD','ARAD');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('ELSTER','ELSTER');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('FUSION','FUSION');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('ITRON','ITRON');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('KENT','KENTMETERS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('MEASURE','MEASURE');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('SCHLUMBERGER','SCHLUMBERGER');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('SCHLUMBERGERAQUADINLINE(SAI)','SCHLUMBERGER');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('TAGUS','TAGUS');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('WATEAU','WATEAU');
INSERT INTO LU_METER_MANUFACTURER (MANUFACTURER_PK,MANUFCODE) VALUES ('KENTMETERS','KENTMETERS');

commit;
/
exit;
