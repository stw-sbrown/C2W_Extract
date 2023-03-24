------------------------------------------------------------------------------
-- TASK			: 	MOSL RDBMS CREATION of Supporting Lookup tables
--
-- AUTHOR         		: 	Michael Marron
--
-- FILENAME       		: 	01_DDL_MOSL_Lookup_TABLES_ALL.sql
--
-- CREATED        		: 	26/02/2016
--	
-- DESCRIPTION 		   	: 	Creates all supporting lookup tables required for MOSL database
--
-- NOTES  			:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order
-- 							as detailed below (.sql).
--
-- ASSOCIATED FILES		:	MOSL_PDM_CORE.xlsm
-- ASSOCIATED SCRIPTS  		:	02_TRUNC_Lookups_ALL.sql
--					
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          	Author         		Description
-- ---------      	----------     -------            	 ------------------------------------------------
-- V0.01       		26/02/2016    	M.Marron	     	Initial version add LU_SPID_RANGE, LU_CONSTRUCTION_SITE and LU_PUBHEALTHRESITE
-- V0.02		26/02/2016      M.Marron                correct BUILDINGWATERSTATUS on LU_CONSTRUCTION_SITE table, set it to Not Null
-- V0.03            14/03/2016      M.Marron            Added LU_LANDLORD, LU_CROSSBORDER, LU_CLOCKOVER & LU_DATALOGGERS.
-- v0.04            15/03/2016      M.Marron            updated comments on LU_CROSSBORDER  
-- V0.05		17/03/2016	N.Henderson		Field missing from LU_PUBHEALRESITE (SENSITIVE)	
------------------------------------------------------------------------------------------------------------
--
-- Create LU_SPID_RANGE table
CREATE TABLE LU_SPID_RANGE
(
SPID_PK VARCHAR(13) NOT NULL,
-- STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL, removed as not needed
CORESPID_PK VARCHAR(10) NOT NULL,
SERVICECATEGORY VARCHAR(1) NOT NULL
);
--
-- set primary key
ALTER TABLE LU_SPID_RANGE ADD PRIMARY KEY (SPID_PK);
--
-- add table and field comments
COMMENT ON Table "LU_SPID_RANGE" IS 'This table holds the MOSL SPID range allocated to Severn Trent Water';
COMMENT ON COLUMN LU_SPID_RANGE.SPID_PK IS 'Primary key for a SPID. It is the full 13 char value issued by MOSLand is the unique identifer for the supply point';
-- COMMENT ON COLUMN LU_SPID_RANGE.STWPROPERTYNUMBER_PK IS 'Target Property ID~~~STW031 - Property ID'; removed not needed
COMMENT ON COLUMN LU_SPID_RANGE.SERVICECATEGORY IS 'Service Category~~~D2002 - Identifies the Service Category for a Supply Point (Water Services or Sewerage Services) “W” = Water and “S” = Sewerage';
--
--
-- Create LU_PUBHEALTHRESITE table
CREATE TABLE LU_PUBHEALTHRESITE
(
STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL,
NONPUBHEALTHRELSITE  NUMBER(1) NOT NULL,
NONPUBHEALTHRELSITEDSC VARCHAR(255) ,
PUBHEALTHRELSITEARR NUMBER(1) NOT NULL,
SENSITIVE VARCHAR(5) NOT NULL
);
--
-- set primary key & Constriants
ALTER TABLE LU_PUBHEALTHRESITE ADD PRIMARY KEY (STWPROPERTYNUMBER_PK);
ALTER TABLE LU_PUBHEALTHRESITE ADD CONSTRAINT LU_CHK01_NONPUBHEALTHRELSITE  CHECK (NONPUBHEALTHRELSITE  IN (0,1));
ALTER TABLE LU_PUBHEALTHRESITE ADD CONSTRAINT LU_CHK01_PUBHEALTHRELSITEARR CHECK (PUBHEALTHRELSITEARR IN (0,1));
--
-- add table and field comments
COMMENT ON Table "LU_PUBHEALTHRESITE" IS 'This table holds data about SENSITIVE properties, source externally from Target but required from MOSL upload';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.STWPROPERTYNUMBER_PK IS 'Target Property ID~~~STW031 - Property ID';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.NONPUBHEALTHRELSITE  IS 'Non-Public Health Related Site Specific Arrangements Flag~~~D2093 - Indication of whether or not a site specific management plan is in place, and not for public health related reasons';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.NONPUBHEALTHRELSITEDSC IS 'Non-Public Health Related Site Specific Arrangements Free Descriptor~~~D2094 - Free descriptor for indication of the nature of site specific management plan in place, when not for public health related reasons';
COMMENT ON COLUMN LU_PUBHEALTHRESITE.PUBHEALTHRELSITEARR IS 'Public Health Related Site Specific Arrangements Flag~~~D2087 - Boolean flag to Indicate whether or not a site specific management plan is in place for public health related reasons';
--
--
-- Create LU_CONSTRUCTION_SITE table  (updated BUILDINGWATERSTATUS to not null)
CREATE TABLE LU_CONSTRUCTION_SITE
(
STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL,
BUILDINGWATERSTATUS NUMBER(1) NOT NULL
);
--
-- set primary key
ALTER TABLE LU_CONSTRUCTION_SITE ADD PRIMARY KEY (STWPROPERTYNUMBER_PK);
ALTER TABLE LU_CONSTRUCTION_SITE ADD CONSTRAINT LU_CHK01_BUILDINGWATERSTATUS CHECK (BUILDINGWATERSTATUS IN (0,1));
--
-- add table and field comments
COMMENT ON Table "LU_CONSTRUCTION_SITE" IS 'This table holds data about properties which are still under construction, source externally from Target but required from MOSL upload';
COMMENT ON COLUMN LU_CONSTRUCTION_SITE.STWPROPERTYNUMBER_PK IS 'Target Property ID~~~STW031 - Property ID';
COMMENT ON COLUMN LU_CONSTRUCTION_SITE.BUILDINGWATERSTATUS IS 'Building Water Status~~~D2029 - Boolean flag to indicate if the site is a building construction site. ';
--
-- version 0.03 Added LU_LANDLORD, LU_CROSSBORDER, LU_CLOCKOVER & LU_DATALOGGERS
--
-- Create LU_LANDLORD   
CREATE TABLE LU_LANDLORD
(
STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL,
LANDLORDSPID VARCHAR(13) NOT NULL,
SERVICECATEGORY VARCHAR(1) NOT NULL
);
--
-- set primary key
-- 
--
-- add table and field comments
COMMENT ON COLUMN LU_LANDLORD.STWPROPERTYNUMBER_PK IS 'Target Property ID~~~STW031 - Property ID to which the landlord SPID applies'; 
COMMENT ON COLUMN LU_LANDLORD.LANDLORDSPID IS 'Landlord SPID~~~D2070 - Identifies the Landlord Supply point in a multi-occupancy Eligible Premises.  The valid set for this is all SPIDs';
COMMENT ON COLUMN LU_LANDLORD.SERVICECATEGORY IS 'Service Category~~~D2002 - Identifies the Service Category for a Supply Point to which this landlord SPID applies if required';
--
-- 
-- Create LU_CROSSBORDER
CREATE TABLE LU_CROSSBORDER
(
STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL,
CORESPID_PK VARCHAR(10),
SPID_PK VARCHAR(13) NOT NULL,
SERVICECATEGORY VARCHAR(1) NOT NULL,
RETAILERID_PK VARCHAR(12) NOT NULL,
WHOLESALERID_PK VARCHAR(12) NOT NULL
);
--
-- set primary key
--
--
-- add table and field comments
COMMENT ON COLUMN LU_CROSSBORDER.STWPROPERTYNUMBER_PK IS 'Target Property ID - Property ID to which Crossborder Supply Point is related to'; 
COMMENT ON COLUMN LU_CROSSBORDER.CORESPID_PK IS 'CORESPID_PK if supplied by STW for CROSSBORDER';
COMMENT ON COLUMN LU_CROSSBORDER.SERVICECATEGORY IS 'Service Category (D2002) - this applies to i.e W or S. Add a row for each if required';
COMMENT ON COLUMN LU_CROSSBORDER.RETAILERID_PK IS 'Retailer ID who is responsible for this SPID';
COMMENT ON COLUMN LU_CROSSBORDER.SERVICECATEGORY IS 'Wholesaler ID who is responsible for this SPID';
--
--
-- Create LU_CLOCKOVER
CREATE TABLE LU_CLOCKOVER
(
METER_READING_PK NUMBER(12) NOT NULL,
STWMETERREF_PK NUMBER(32) NOT NULL,
METERREADDATE DATE NOT NULL,
METERREAD NUMBER(12) ,
ROLLOVERINDICATOR NUMBER(1) NOT NULL
);
--
-- set primary key
ALTER TABLE LU_CLOCKOVER ADD CONSTRAINT CHK01_LUCLOCKOVERINDICATOR CHECK (ROLLOVERINDICATOR IN (0,1));
--
-- add table and field comments
COMMENT ON COLUMN LU_CLOCKOVER.METER_READING_PK IS 'METER_READING_PK that this clockover relates too';
COMMENT ON COLUMN LU_CLOCKOVER.STWMETERREF_PK IS 'Target METER ID'; 
COMMENT ON COLUMN LU_CLOCKOVER.METERREADDATE IS 'Meter read date, this is the meter read date and used to check that the correct meter reading is flagged'; 
COMMENT ON COLUMN LU_CLOCKOVER.METERREAD IS 'METER Read value at clockover used to check that the correct reading is flagged'; 
COMMENT ON COLUMN LU_CLOCKOVER.ROLLOVERINDICATOR IS 'Rollover Indicator~~~D3020 - Proposes whether the meter read has rolled over or not as part of meter read submission by the Retailer or  Wholesaler';
--
--
-- Create LU_DATALOGGERS
CREATE TABLE LU_DATALOGGERS
(
STWMETERREF_PK NUMBER(32) NOT NULL,
STWPROPERTYNUMBER_PK NUMBER(9) NOT NULL,
MANUFACTURER_PK VARCHAR(32) NOT NULL,
MANUFACTURERSERIALNUM_PK VARCHAR(32) NOT NULL,
DATALOGGERWHOLESALER NUMBER(1) NOT NULL,
DATALOGGERNONWHOLESALER NUMBER(1) NOT NULL
);
--
-- set primary key
ALTER TABLE LU_DATALOGGERS ADD CONSTRAINT CHK01_LUDATALOGGERWHOLE CHECK (DATALOGGERWHOLESALER IN (0,1));
ALTER TABLE LU_DATALOGGERS ADD CONSTRAINT CHK01_LUDATALOGGERNONWHOLE CHECK (DATALOGGERNONWHOLESALER IN (0,1));
--
-- add table and field comments
COMMENT ON COLUMN LU_DATALOGGERS.STWMETERREF_PK IS 'Target METER PK that this logger is attached to';
COMMENT ON COLUMN LU_DATALOGGERS.STWPROPERTYNUMBER_PK IS 'Target Property where meter is installed'; 
COMMENT ON COLUMN LU_DATALOGGERS.MANUFACTURER_PK IS 'Meter manufacture'; 
COMMENT ON COLUMN LU_DATALOGGERS.MANUFACTURERSERIALNUM_PK IS 'METER serial number'; 
COMMENT ON COLUMN LU_DATALOGGERS.DATALOGGERWHOLESALER IS 'Is this a Wholesaler logger Y (1) or No (0)';
COMMENT ON COLUMN LU_DATALOGGERS.DATALOGGERWHOLESALER IS 'Is this a NON Wholesaler logger Y (1) or No (0)';
--
--
--
commit;
exit;

