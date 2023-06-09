------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00011.sql
--
--
-- Subversion $Revision: 4023 $	
--
--Date 						16/03/2016
--Issue: 					Rework on MO_METER table.  A number of columns not required, some renamed
--							and a few additional columns.  Create new compsite primary key as well
--
--
-- Changes applied
--


--DROP COLUMNS FROM MO_METER 
ALTER TABLE MO_METER DROP COLUMN INITIALMETERREADDATE;
ALTER TABLE MO_METER DROP COLUMN METERERASEDFLAG;
ALTER TABLE MO_METER DROP COLUMN NONMARKETMETERFLAG;
ALTER TABLE MO_METER DROP COLUMN WHOLESALERID;
ALTER TABLE MO_METER DROP COLUMN INSTALLEDBYACCREDITEDENTITY;
ALTER TABLE MO_METER DROP COLUMN EFFECTIVEFROMDATE;
ALTER TABLE MO_METER DROP COLUMN EFFECTIVETODATE;
ALTER TABLE MO_METER DROP COLUMN SECADDRESSABLEOBJ ;
ALTER TABLE MO_METER DROP COLUMN PRIMADDRESSABLEOBJ;
ALTER TABLE MO_METER DROP COLUMN ADDRESSLINE01;
ALTER TABLE MO_METER DROP COLUMN ADDRESSLINE02;
ALTER TABLE MO_METER DROP COLUMN ADDRESSLINE03;
ALTER TABLE MO_METER DROP COLUMN ADDRESSLINE04;
ALTER TABLE MO_METER DROP COLUMN ADDRESSLINE05;
ALTER TABLE MO_METER DROP COLUMN POSTCODE;
ALTER TABLE MO_METER DROP COLUMN COUNTRY;
ALTER TABLE MO_METER DROP COLUMN PAFADDRESSKEY;
ALTER TABLE MO_METER DROP COLUMN MAINMETERMANUFACTURER;
ALTER TABLE MO_METER DROP COLUMN MAINMETERMANUFACTURERSERIALNUM;
ALTER TABLE MO_METER DROP COLUMN MAINMETERINITIALMETERREADDATE;
ALTER TABLE MO_METER DROP COLUMN METEROUTREADLOCFREEDESCRIPTOR;

--RENAME COLUMNS IN MO_METER 
ALTER TABLE MO_METER RENAME COLUMN MEASUREUNITFREEATMETER TO MEASUREUNITATMETER;
ALTER TABLE MO_METER RENAME COLUMN METERREADMINFREQUENCY TO METERREADFREQUENCY;
ALTER TABLE MO_METER RENAME COLUMN FREEDESCRIPTION TO OUTREADERLOCFREEDES;

--CREATE NEW COLUMNS IN MO_METER
ALTER TABLE MO_METER ADD METERREF NUMBER(9) CONSTRAINT CH01_METERREF NOT NULL UNIQUE;
ALTER TABLE MO_METER ADD SUPPLYPOINTREF VARCHAR(32);
ALTER TABLE MO_METER ADD METERNETWORKASSOCIATION NUMBER(1) CONSTRAINT CH01_METERNETWORKASSOC NOT NULL;
ALTER TABLE MO_METER ADD MDVOL NUMBER(5,2);

--DISABLE AND DROP CONSTRAINTS
ALTER TABLE MO_METER_DPIDXREF DISABLE CONSTRAINT FK_MANUFACTURER_PK02;
ALTER TABLE MO_METER_DPIDXREF DISABLE CONSTRAINT FK_MANUFACTURERSERIALNUM_PK02;
ALTER TABLE MO_METER_DPIDXREF DROP CONSTRAINT FK_MANUFACTURER_PK02;
ALTER TABLE MO_METER_DPIDXREF DROP CONSTRAINT FK_MANUFACTURERSERIALNUM_PK02;
ALTER TABLE MO_METER_READING DISABLE CONSTRAINT FK_MANUFACTURER_PK01;
ALTER TABLE MO_METER_READING DISABLE CONSTRAINT FK_MANUFACTURERSERIALNUM_PK01;
ALTER TABLE MO_METER_READING DROP CONSTRAINT FK_MANUFACTURER_PK01;
ALTER TABLE MO_METER_READING DROP CONSTRAINT FK_MANUFACTURERSERIALNUM_PK01;

--DROP PRIMARY KEY ON MO_METER
ALTER TABLE MO_METER DISABLE CONSTRAINT PK_MANUFACTURER;
ALTER TABLE MO_METER DROP CONSTRAINT  PK_MANUFACTURER;

--RECREATE PRIMARY KEY ON MO_METER (COMPOSITE ON TWO COLUMNS)
ALTER TABLE MO_METER ADD CONSTRAINT PK_MAN_SERIAL_REF_COMP PRIMARY KEY(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);

--ADD FK CONSTRAINTS
ALTER TABLE MO_METER_DPIDXREF ADD CONSTRAINT FK_METER_READING_MANUFACT01 FOREIGN KEY (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
ALTER TABLE MO_METER_READING ADD CONSTRAINT FK_METER_READING_MANUFACT02  FOREIGN KEY (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);

commit;
exit;


