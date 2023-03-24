------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	P00024.sql
--
--
-- Subversion $Revision: 5870 $	
--
-- CREATED        		: 	22/04/2016
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
-- ---------      ----------      -------         ------------------------------------------------
-- V0.01		      22/04/2016		  D.Cheung        Add column NONMARKETMETER BOOLEAN to MO_METER
--                                                REMOVE constraint CH04_SPID on MO_METER - make SPID_PK NULLABLE
--                                                Create NEW TABLE MO_METER_SPID_ASSOC  
--                                                Create NEW TABLE MO_METER_NETWORK
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
--CHANGES
--ADD COLUMN TO TABLE MO_METER:
------------------------------------------------------
ALTER TABLE MO_METER ADD NONMARKETMETERFLAG NUMBER(1,0) CONSTRAINT CH01_NONMARKETMETERFLAG NOT NULL;
ALTER TABLE MO_METER MODIFY NONMARKETMETERFLAG DEFAULT 0;

--Add Comment
COMMENT ON COLUMN MO_METER.NONMARKETMETERFLAG IS 'Flag to indicate if Marketable or NON-Marketable Meter';

COMMIT;
/

--MAKE COLUMN SPID NULLABLE on MO_METER
ALTER TABLE MO_METER MODIFY (SPID_PK NULL);
COMMIT;
/

--CREATE NEW TABLE MO_METER_SPID_ASSOC
--
-- 1_DDL_MO_METER_SPID_ASSOC.sql
-- Create MO_METER_SPID_ASSOC table
-- Date - 21/04/2016
-- Written By - Dominic Cheung
--

---------------------------------------------------------------------------------------
-- Version     Date        Author     CP/DEF.     Description
-- ---------   ----------  --------   ---------	  -------------------------------------
-- V 1.00      21/04/2016  D.Cheung   CR_03 		  Intial Draft
-- V 1.01      13/07/2016  D.Cheung               I-291 - Change PK on SIPD_ASSOC to add propertynumber
-- V 1.02		   13/10/2016	 S.Badhan               Remove drop of MO_METER_SPID_ASSOC as removed in main table drop sql
----------------------------------------------------------------------------------------

--DROP TABLE MO_METER_SPID_ASSOC purge;
---MO_METER_SPID_ASSOC--
CREATE TABLE MO_METER_SPID_ASSOC
(
METERREF NUMBER(12) CONSTRAINT CH01_METERREFASOC NOT NULL,
STWPROPERTYNUMBER_PK NUMBER(9) CONSTRAINT CH01_STWPROPERTYNUMBERASOC NOT NULL,
MANUFACTURER_PK VARCHAR(32) CONSTRAINT CH01_MANUFACTURERASOC NOT NULL,
MANUFACTURERSERIALNUM_PK VARCHAR(32) CONSTRAINT CH01_MANUFACTURERSERIALNUMASOC NOT NULL,
SPID VARCHAR(13) CONSTRAINT CH01_SPIDASOC NOT NULL
);

-- Add PK & FK constraints
ALTER TABLE MO_METER_SPID_ASSOC ADD CONSTRAINT PK_METERSPIDASSOC_COMP PRIMARY KEY (MANUFACTURER_PK,MANUFACTURERSERIALNUM_PK);
ALTER TABLE MO_METER_SPID_ASSOC ADD CONSTRAINT FK_METER_MANUFACT05 FOREIGN KEY (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);

-- Add Indexes
CREATE INDEX MO_METER_SPID_ASSOC_IDX1 ON MO_METER_SPID_ASSOC (METERREF, STWPROPERTYNUMBER_PK); 
CREATE INDEX MO_METER_SPID_ASSOC_IDX2 ON MO_METER_SPID_ASSOC (METERREF);
CREATE INDEX MO_METER_SPID_ASSOC_IDX3 ON MO_METER_SPID_ASSOC (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, SPID);

-- Add Comments
COMMENT ON TABLE MO_METER_SPID_ASSOC IS 'Meter Supply Point Associations';
COMMENT ON COLUMN MO_METER_SPID_ASSOC.METERREF IS 'STW TARGET NO_EQUIPMENT reference';
COMMENT ON COLUMN MO_METER_SPID_ASSOC.STWPROPERTYNUMBER_PK  IS 'STW TARGET NO_PROPERTY Reference';
COMMENT ON COLUMN MO_METER_SPID_ASSOC.MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter.  STW010 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Manufacturer field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_SPID_ASSOC.MANUFACTURERSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturer’s serial number of a meter. STW011 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Serial field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_SPID_ASSOC.SPID IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';

ALTER TABLE MO_METER_SPID_ASSOC DROP CONSTRAINT PK_METERSPIDASSOC_COMP;
ALTER TABLE MO_METER_SPID_ASSOC ADD CONSTRAINT PK_METERSPIDASSOC_COMP PRIMARY KEY (MANUFACTURER_PK,MANUFACTURERSERIALNUM_PK, STWPROPERTYNUMBER_PK);

Commit;
/

--CREATE NEW TABLE MO_METER_NETWORK
--
-- 1_DDL_MO_METER_NETWORK.sql
-- Create MO_METER_NETWORK table
-- Date - 21/04/2016
-- Written By - Dominic Cheung
--

---------------------------------------------------------------------------------------
-- Version     Date        Author     CP/DEF.     Description
-- ---------   ----------  --------   ---------	  -------------------------------------
-- V 1.00      21/04/2016  D.Cheung   CR_04		    Intial Draft
----------------------------------------------------------------------------------------

---MO_METER_NETWORK-
CREATE TABLE MO_METER_NETWORK
(
MAIN_METERREF NUMBER(12),
MAIN_STWPROPERTYNUMBER_PK NUMBER(9),
MAIN_MANUFACTURER_PK VARCHAR(32) CONSTRAINT CH01_MAINMANUFACTURERNWRK NOT NULL,
MAIN_MANSERIALNUM_PK VARCHAR(32) CONSTRAINT CH01_MAINMANSERIALNUMNWRK NOT NULL,
MAIN_SPID VARCHAR(13) CONSTRAINT CH01_MAINSPIDNWRK NOT NULL,
SUB_METERREF NUMBER(12),
SUB_STWPROPERTYNUMBER_PK NUMBER(9),
SUB_MANUFACTURER_PK VARCHAR(32) CONSTRAINT CH01_SUBMANUFACTURERNWRK NOT NULL,
SUB_MANSERIALNUM_PK VARCHAR(32) CONSTRAINT CH01_SUBMANSERIALNUMNWRK NOT NULL,
SUB_SPID VARCHAR(13) CONSTRAINT CH01_SUBSPIDNWRK NOT NULL
);


-- Add PK & FK constraints
ALTER TABLE MO_METER_NETWORK ADD CONSTRAINT FK_METER_MANUFACT01 FOREIGN KEY (MAIN_MANUFACTURER_PK, MAIN_MANSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
ALTER TABLE MO_METER_NETWORK ADD CONSTRAINT FK_METER_MANUFACT02 FOREIGN KEY (SUB_MANUFACTURER_PK, SUB_MANSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);

-- Add Indexes
CREATE INDEX MO_METER_NETWORK_IDX1 ON MO_METER_NETWORK (MAIN_MANUFACTURER_PK, MAIN_MANSERIALNUM_PK); 
CREATE INDEX MO_METER_NETWORK_IDX2 ON MO_METER_NETWORK (SUB_MANUFACTURER_PK, SUB_MANSERIALNUM_PK);
CREATE INDEX MO_METER_NETWORK_IDX3 ON MO_METER_NETWORK (MAIN_METERREF, MAIN_STWPROPERTYNUMBER_PK);
CREATE INDEX MO_METER_NETWORK_IDX4 ON MO_METER_NETWORK (SUB_METERREF, SUB_STWPROPERTYNUMBER_PK);

-- Add Comments
COMMENT ON TABLE MO_METER_NETWORK IS 'Meter Supply Point Associations';
COMMENT ON COLUMN MO_METER_NETWORK.MAIN_METERREF IS 'STW TARGET NO_EQUIPMENT reference for MAIN meter';
COMMENT ON COLUMN MO_METER_NETWORK.MAIN_STWPROPERTYNUMBER_PK  IS 'STW TARGET NO_PROPERTY Reference for MAIN meter';
COMMENT ON COLUMN MO_METER_NETWORK.MAIN_MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter for MAIN meter.  STW010 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Manufacturer field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_NETWORK.MAIN_MANSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturer’s serial number of a meter for MAIN meter. STW011 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Serial field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_NETWORK.MAIN_SPID IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator for MAIN meter';
COMMENT ON COLUMN MO_METER_NETWORK.SUB_METERREF IS 'STW TARGET NO_EQUIPMENT reference for SUB meter';
COMMENT ON COLUMN MO_METER_NETWORK.SUB_STWPROPERTYNUMBER_PK  IS 'STW TARGET NO_PROPERTY Reference for SUB meter';
COMMENT ON COLUMN MO_METER_NETWORK.SUB_MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter for SUB meter.  STW010 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Manufacturer field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_NETWORK.SUB_MANSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturer’s serial number of a meter for SUB meter. STW011 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Serial field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN MO_METER_NETWORK.SUB_SPID IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator for SUB meter';

Commit;
/
/

exit;



