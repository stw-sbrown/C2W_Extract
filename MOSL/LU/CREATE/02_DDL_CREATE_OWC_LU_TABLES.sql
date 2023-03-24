------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS CREATION of Supporting OWC Lookup tables
--
-- AUTHOR         		: 	Dominic Cheung
--
-- FILENAME       		: 	02_DDL_CREATE_OWC_LU_TABLES.sql
--
-- CREATED        		: 	09/09/2016
-- Subversion $Revision: 6380 $
--	
-- DESCRIPTION 		   	: 	Creates lookup table for OWC mappings
--
-- NOTES  			:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order
-- 							as detailed below (.sql).
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	03_DDL_DROP_OWC_LU_TABLES.sql
--					
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	Date          Author        Description
-- -------      ----------    ------------  ------------------------------------------------
-- V0.01       	09/09/2016    D.Cheung	    Initial version
-- V0.02        20/09/2016    D.Cheung      Move to TRAN area
-- V0.03        04/10/2016    K.Burton      Added LU_OWC_TE_METERS
-- V0.04        12/10/2016    K.Burton      Added LU_SPID_RANGE_DWRCYMRU
-- V0.05        25/10/2016    K.Burton      Added LU_SPID_RANGE_NOSPID
-- V0.06        09/11/2016    K.Burton      Added LU_OWC_NOT_SENSITIVE
-- V0.07        10/11/2016    K.Burton      Added LU_OWC_SSW_SPIDS
-- V0.08        21/11/2016    K.Burton      Added LU_OWC_SAP_FLOCA 
-- V0.09        22/11/2016    K.Burton      Added LU_NOSPID_EXCEPTIONS
-- V0.10        23/11/2016    D.Cheung      Add index PK to LU_NOSPID_EXCEPTIONS
------------------------------------------------------------------------------------------------------------
--
-- DROP LU_OWC_TARIFF TABLE
--DROP TABLE LU_OWC_TARIFF PURGE;
-- Create LU_OWC_TARIFF table
CREATE TABLE LU_OWC_TARIFF
(
    TARIFF_TYPE       VARCHAR(5),
    OWCTARIFFCODE_PK	VARCHAR2(32)  CONSTRAINT CH01_LU_OWC_STWTARIFFCODE NOT NULL,
    STWTARIFFCODE_PK	VARCHAR2(32)  CONSTRAINT CH01_LU_OWC_OWCTARIFFCODE NOT NULL,
    DESCRIPTION       VARCHAR2(255),
    WHOLESALERID_PK   VARCHAR2(12)   
);
--
-- set primary key
ALTER TABLE LU_OWC_TARIFF ADD CONSTRAINT PK_LU_OWC_TARIFF PRIMARY KEY (OWCTARIFFCODE_PK, WHOLESALERID_PK);
--
-- add table and field comments
COMMENT ON TABLE LU_OWC_TARIFF IS 'This table holds the MOSL SPID range allocated to Severn Trent Water';
COMMENT ON COLUMN LU_OWC_TARIFF.TARIFF_TYPE IS 'Tariff Type';
COMMENT ON COLUMN LU_OWC_TARIFF.OWCTARIFFCODE_PK IS 'Primary key for a Crossborder Tariff from the OWC';
COMMENT ON COLUMN LU_OWC_TARIFF.STWTARIFFCODE_PK IS 'Mapping of equivilent STW TARIFFCODE';
COMMENT ON COLUMN LU_OWC_TARIFF.DESCRIPTION IS 'Description of TARIFF';
COMMENT ON COLUMN LU_OWC_TARIFF.WHOLESALERID_PK IS 'Wholesaler Company from which the OWC Tariff is from';
--
--
CREATE TABLE LU_OWC_TE_METERS
(
  ACCOUNT_NUMBER                 NUMBER(9),
  STW_PROPERTYNUMBER             NUMBER(9),
  OWC_SPID                       VARCHAR2(13),
  OWC_METERSERIAL                VARCHAR2(32),
  OWC_METERMANUFACTURER          VARCHAR2(32),                                                                                                                                                                                  
  OWC_PROPERTYNUMBER             NUMBER(9),
  QUIS                           NUMBER,
  OWC                            VARCHAR2(30),
  MO_RDY                         VARCHAR2(1)
);

CREATE TABLE LU_SPID_RANGE_DWRCYMRU
(
  DWRCYMRU_SPID VARCHAR2(13 BYTE),
  SPID_PK       VARCHAR2(13 BYTE) NOT NULL ENABLE,
  CORESPID_PK   VARCHAR2(10 BYTE) NOT NULL ENABLE
);


CREATE TABLE LU_SPID_RANGE_NOSPID
(
  NOSPID_SPID VARCHAR2(13 BYTE),
  SPID_PK       VARCHAR2(13 BYTE) NOT NULL ENABLE,
  CORESPID_PK   VARCHAR2(10 BYTE) NOT NULL ENABLE
);

CREATE TABLE LU_OWC_NOT_SENSITIVE
(
  STWPROPERTYNUMBER_PK NUMBER(9,0) NOT NULL ENABLE,
  OWC_SPID             VARCHAR2(13 BYTE),
  OWC                  VARCHAR2(32 BYTE) NOT NULL ENABLE
);

CREATE TABLE LU_OWC_SSW_SPIDS
(
    SSW_SPID VARCHAR2(13 BYTE)
);

CREATE TABLE LU_OWC_SAP_FLOCA
(
  SPID_PK       VARCHAR2(13 BYTE),
  SAPFLOCNUMBER NUMBER(30,0),
  OWC           VARCHAR2(32 BYTE) NOT NULL ENABLE
);

CREATE TABLE LU_NOSPID_EXCEPTIONS
(
  SPID_PK       VARCHAR2(13 BYTE)
);

ALTER TABLE LU_NOSPID_EXCEPTIONS ADD CONSTRAINT PK01_NOSPID_SPID_PK PRIMARY KEY (SPID_PK);

commit;
/
/
show errors;
exit;