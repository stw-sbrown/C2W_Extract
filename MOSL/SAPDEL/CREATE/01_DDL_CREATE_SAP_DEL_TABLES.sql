------------------------------------------------------------------------------
-- TASK				: 	MOSL DELIVERY CREATION 
--
-- AUTHOR         		: 	Kevin Burton
--
-- FILENAME       		: 	01_DDL_CREATE_SAP_DEL_TABLES.sql
--
-- CREATED        		: 	23/05/2016
--	
-- Subversion $Revision: 5845 $
--
-- DESCRIPTION 		   	: 	Creates all database tables for SAP upload
--
-- NOTES  			:	
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS 	 	:	
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date        	 	Author         	Description
-- --------------------------------------------------------------------------------------------------------
-- V0.01	       	23/05/2016   	 	K.Burton	Initial version
-- V0.02          24/05/2016      K.Burton  Added PARENTLEGACYRECNUM to SAP_DEL_PODMO for linking to child table
--                                          Added COL_COUNT to SAP_DEL_OUTPUT global temp table for controlling
--                                          NULL value outputs. Also increased number of COL_XX columns to 50
--                                          to accomodate all SAP file requirements
-- V0.03          27/05/2016      K.Burton  Added Connection Object (COB) tables. Also added primary and foreign
--                                          key constraints
-- V0.04          03/06/2016      K.Burton  Added new POD table SAP_DEL_PODSRV for service assignment file
--                                          Extend STWMETERREF length to 15 on DEVice tables
-- V0.05          08/06/2016      K.Burton  Added DEV, COM and METER READ tables
-- V0.06          14/06/2016      K.Burton  Added METER INSTALL, BP and REG tables
-- V0.07          23/06/2016      K.Burton  Changed SAP_DEL_COB to replace GPSX and GPSY with single GPSCOORDINATES column
--                                          - this is a SAP placeholder - Issue SI-014
-- V0.08          29/06/2016      K.Burton  CR_005 - additional columns added to SAP_DEL_DEV and SAP_DEL_METER_INSTALL
-- V0.09          30/06/2016      K.Burton  CR_006 - Removed YEARLYVOLESTIMATE from SAP_DEL_METER_INSTALL
-- V0.10          12/07/2016      K.Burton  CR_009 - Moved CALD_TXT (D4003) from SAP_DEL_SCM and SAP_DEL_SCMTE to SAP_DEL_SCMTEMO
--                                          CR_011 - Add SPECIALAGREEMENTFACTOR and SPECIALAGREEMENTFLAG to SAP_DEL_SCMTEMO
--                                          CR_013 - Move Special Instruction and Special Location from DVLCRT to DVLUPDATE
-- V0.11          14/07/2016      K.Burton  CR_016 - Renamed VOL_LIM to VOLUME_LIMIT in SAP_DEL_SCMMO
-- V0.12          15/07/2016      K.Burton  CR_018 - Added TREATMENTWORKS column to SAP_DEL_COB
-- V0.13          18/07/2016      K.Burton  CR_017 - removed primary key constraint on SAP_DEL_BP due to multi sensitive reason codes
--                                                   and extended CUSTOMERCLASSIFICATION field length
-- V0.14          20/07/2016      K.Burton  CR_020 - added changes fro SAP_DPID_TYPE lookup - changed TECOMPONENTTYPE column for DPID_TYPE
--                                                   in SAP_DEL_SCM and SAP_DEL_SCMTE
-- V0.15          11/08/2016      O.Badmus  Allow nulls in certain columns in SAP_DEL_COB and SAP_DEL_DEV
-- V0.16          15/08/2016      S.Badhan  I-331.Amend constraint for SAP_DEL_DEVMO to allow null on SEWCHARGEABLEMETERSIZE if PRIVATETE.
-- V0.17          15/08/2016      K.Burton  CR_021 / Defect 172 - Only single LE/BP should be uploaded to SAP per property. If we have 2
--                                          different LEs for a property the Water customer should take preference. New view SAP_PROPERTY_CUSTOMER_V.
-- V0.18          24/08/2016      K.Burton  CR_022 - Added Yearly Volume Estimate to INSTE section
-- V0.19          31/08/2016      D.Cheung  DEF 197 - Misalignment between Meter Read and Meter Install due to NULL SAP FLOCA in Eligible premises
-- V0.20          15/09/2016      K.Burton  CR_017 - Added SENSITIVE_REASON and SAP_IDENTIFICATION_TYPE to SAP_DEL_BP and LU_SAP_SENSITIVITY_CODES lookup
-- V0.21          12/10/2016      S.Badhan  Remove comments (--) at end of statements causing compilation errors on server.
-- V0.22          14/10/2016      K.Burton  Added additional indexes to SAP_DEL_COB, SAP_DEL_PREM and SAP_DEL_SCM
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-- GENERIC OUTPUT TABLE
--------------------------------------------------------------------------------------------------------
CREATE GLOBAL TEMPORARY TABLE SAP_DEL_OUTPUT
  (
    COL_COUNT  NUMBER,
    KEY_COL    VARCHAR2(30 BYTE),
    SECTION_ID VARCHAR2(20 BYTE),
    COL_01     VARCHAR2(250 BYTE),
    COL_02     VARCHAR2(250 BYTE),
    COL_03     VARCHAR2(250 BYTE),
    COL_04     VARCHAR2(250 BYTE),
    COL_05     VARCHAR2(250 BYTE),
    COL_06     VARCHAR2(250 BYTE),
    COL_07     VARCHAR2(250 BYTE),
    COL_08     VARCHAR2(250 BYTE),
    COL_09     VARCHAR2(250 BYTE),
    COL_10     VARCHAR2(250 BYTE),
    COL_11     VARCHAR2(250 BYTE),
    COL_12     VARCHAR2(250 BYTE),
    COL_13     VARCHAR2(250 BYTE),
    COL_14     VARCHAR2(250 BYTE),
    COL_15     VARCHAR2(250 BYTE),
    COL_16     VARCHAR2(250 BYTE),
    COL_17     VARCHAR2(250 BYTE),
    COL_18     VARCHAR2(250 BYTE),
    COL_19     VARCHAR2(250 BYTE),
    COL_20     VARCHAR2(250 BYTE),
    COL_21     VARCHAR2(250 BYTE),
    COL_22     VARCHAR2(250 BYTE),
    COL_23     VARCHAR2(250 BYTE),
    COL_24     VARCHAR2(250 BYTE),
    COL_25     VARCHAR2(250 BYTE),
    COL_26     VARCHAR2(250 BYTE),
    COL_27     VARCHAR2(250 BYTE),
    COL_28     VARCHAR2(250 BYTE),
    COL_29     VARCHAR2(250 BYTE),
    COL_30     VARCHAR2(250 BYTE),
    COL_31     VARCHAR2(250 BYTE),
    COL_32     VARCHAR2(250 BYTE),
    COL_33     VARCHAR2(250 BYTE),
    COL_34     VARCHAR2(250 BYTE),
    COL_35     VARCHAR2(250 BYTE),
    COL_36     VARCHAR2(250 BYTE),
    COL_37     VARCHAR2(250 BYTE),
    COL_38     VARCHAR2(250 BYTE),
    COL_39     VARCHAR2(250 BYTE),
    COL_40     VARCHAR2(250 BYTE),
    COL_41     VARCHAR2(250 BYTE),
    COL_42     VARCHAR2(250 BYTE),
    COL_43     VARCHAR2(250 BYTE),
    COL_44     VARCHAR2(250 BYTE),
    COL_45     VARCHAR2(250 BYTE),
    COL_46     VARCHAR2(250 BYTE),
    COL_47     VARCHAR2(250 BYTE),
    COL_48     VARCHAR2(250 BYTE),
    COL_49     VARCHAR2(250 BYTE),
    COL_50     VARCHAR2(250 BYTE)
  );
  
-- ADD INDEX TO GLOBAL TEMP TABLE  
CREATE INDEX KEY_IDX ON SAP_DEL_OUTPUT (KEY_COL);

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_POD
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_POD
  (
    LEGACYRECNUM                  VARCHAR2(30) CONSTRAINT CH01_POD_LEGACYRECNUM NOT NULL,
    SAPFLOCNUMBER                 VARCHAR2(10) CONSTRAINT CH01_POD_SAPFLOCNUMBER NOT NULL,
    SPID_PK                          VARCHAR2(13) CONSTRAINT CH01_POD_SPID NOT NULL,
    STWPROPERTYNUMBER                VARCHAR2(9) CONSTRAINT CH01_POD_PROPERTYNUMBER NOT NULL,
    SERVICECATEGORY               VARCHAR2(1) CONSTRAINT CH01_SERVICECATEGORY NOT NULL,
    SUPPLYPOINTEFFECTIVEFROMDATE  DATE,
    NEWCONNECTIONTYPE             VARCHAR2(3),
    ACCREDITEDENTITYFLAG          NUMBER(1) CONSTRAINT CH01_ACCREDITEDENTITYFLAG NOT NULL,
    GAPSITEALLOCATIONMETHOD       VARCHAR2(4),
    OTHERSERVICECATPROVIDED       NUMBER(1) CONSTRAINT CH01_OTHERSERVICECATPROVIDED NOT NULL,
    OTHERSERVICECATPROVIDEDREASON VARCHAR2(2),
    VOLTRANSFERFLAG               NUMBER(1) CONSTRAINT CH01_VOLTRANSFERFLAG NOT NULL,
    INTERIMDUTYSUPPLYPOINT        NUMBER(1) CONSTRAINT CH01_INTERIMDUTYSUPPLYPOINT NOT NULL, -- D2077 - Not defined in Transform area
    SPIDSTATUS                    VARCHAR2(9) CONSTRAINT CH01_SPIDSTATUS NOT NULL,
    LATEREGAPPLICATION            NUMBER(1) CONSTRAINT CH01_LATEREGAPPLICATION NOT NULL,
    OTHERSPID                     VARCHAR2(12)
  ) ;

-- CREATE INDEX ON SAP_DEL_POD
CREATE INDEX SPID_IDX ON SAP_DEL_POD (SPID_PK);

-- ADD PRIMARY AND UNIQUE KEYS TO TABLES
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT SAP_DEL_POD_PK PRIMARY KEY (LEGACYRECNUM);
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT SAP_DEL_POD_SPID_UK UNIQUE (SPID_PK);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_POD
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_SERVICECATEGORY CHECK (SERVICECATEGORY IN ('W','S'));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_ACCREDITEDENTITYFLAG CHECK (ACCREDITEDENTITYFLAG IN (0,1));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_GAPSITEALLOCATIONMETHOD CHECK (GAPSITEALLOCATIONMETHOD IN ('SEQ', 'SPID'));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_OTHERSERVICECATPROVIDED CHECK (OTHERSERVICECATPROVIDED IN (0,1));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_OTHERSERVICREASON CHECK (OTHERSERVICECATPROVIDEDREASON IN ('NS', 'NA', 'NR'));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_VOLTRANSFERFLAG CHECK (VOLTRANSFERFLAG IN (0,1));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_SPIDSTATUS CHECK (SPIDSTATUS IN ('NEW', 'PARTIAL', 'TRADABLE', 'REJECTED', 'DEREG'));
ALTER TABLE SAP_DEL_POD ADD CONSTRAINT RF01_LATEREGAPPLICATION CHECK (LATEREGAPPLICATION IN(0,1));

--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_POD  IS 'Point of Delivery header table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_POD
COMMENT ON COLUMN SAP_DEL_POD.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_POD.SERVICECATEGORY IS 'Service Category~~~D2002 - Identifies the Service Category for a Supply Point (Water Services or Sewerage Services) â??Wâ?? = Water and â??Sâ?? = Sewerage';
COMMENT ON COLUMN SAP_DEL_POD.SUPPLYPOINTEFFECTIVEFROMDATE IS 'Supply Point Effective From Date~~~D2013 - Date that the Supply Point takes effect in the Central Systems.  This can be the date the connection was completed for newly established Supply Points, or the date that a Gap Site of Entry Change of Use takes effect.';
COMMENT ON COLUMN SAP_DEL_POD.NEWCONNECTIONTYPE IS 'New Connection Type~~~D2023 - Identifies the type of connection for a new Supply Point';
COMMENT ON COLUMN SAP_DEL_POD.ACCREDITEDENTITYFLAG IS '~~~D2033 - Declares whether the work being notified to the Market Operator was carried out by an Accredited Entity';
COMMENT ON COLUMN SAP_DEL_POD.GAPSITEALLOCATIONMETHOD IS 'Gap Site Allocation Method~~~D2034 - Identifies how the Market Operator has allocated a Gap Site to a Retailer';
COMMENT ON COLUMN SAP_DEL_POD.OTHERSERVICECATPROVIDED IS 'Other Service Category Provided Flag~~~D2041 - Flag indicating if services or no services provided of the other Service Category compared to a SPID at an Eligible Premises. May be required if we know of a SPID without supplies';
COMMENT ON COLUMN SAP_DEL_POD.OTHERSERVICECATPROVIDEDREASON IS 'Other Service Category Provided Flag Reason~~~D2042 - Reason to explain the value of the Other Service Category Provided Flag. May be required if we know of a SPID without supplies';
COMMENT ON COLUMN SAP_DEL_POD.VOLTRANSFERFLAG IS 'Volume Transfer Flag~~~D2052 - Indicates when a SPID is included in a Volume Transfer process';
COMMENT ON COLUMN SAP_DEL_POD.SPIDSTATUS IS 'SPID Status~~~D2088 - The logical status of a SPID';
COMMENT ON COLUMN SAP_DEL_POD.LATEREGAPPLICATION IS 'Late Partial Registration Application~~~D2089 - Flag indicating when a SPID is awaiting a partial registration application but it has not been received.';
COMMENT ON COLUMN SAP_DEL_POD.OTHERSPID IS 'Other SPID~~~D2091 - Unique identifier for second supply point where required in a transaction';
COMMENT ON COLUMN SAP_DEL_POD.STWPROPERTYNUMBER IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_POD.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_POD.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
--------------------------------------------------------------------------------------------------------
-- SAP_DEL_PODMO
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_PODMO
  (
    LEGACYRECNUM           VARCHAR2(30) CONSTRAINT CH01_PODMO_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM     VARCHAR2(30) CONSTRAINT CH01_PODMO_PARENTLEGACYRECNUM NOT NULL,
    SAPFLOCNUMBER          VARCHAR2(10) CONSTRAINT CH01_PODMO_SAPFLOCNUMBER NOT NULL,
    SPID_PK                VARCHAR2(13) CONSTRAINT CH01_PODMO_SPID NOT NULL,
    STWPROPERTYNUMBER      VARCHAR2(9) CONSTRAINT CH01_PODMO_PROPERTYNUMBER NOT NULL,
    EFFECTIVEFROMDATE      DATE,
    DISCONRECONDEREGSTATUS VARCHAR2(5),
    MULTIPLEWHOLESALERFLAG NUMBER(1) CONSTRAINT CH02_MULTIPLEWHOLESALERFLAG NOT NULL,
    LANDLORDSPID           VARCHAR2(32)
  ) ;

--ADD FORIEGN KEYS
ALTER TABLE SAP_DEL_PODMO ADD CONSTRAINT FK01_PODMO_LEGACYRECNUM FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_POD (LEGACYRECNUM);
ALTER TABLE SAP_DEL_PODMO ADD CONSTRAINT FK01_PODMO_SPID FOREIGN KEY (SPID_PK)  REFERENCES SAP_DEL_POD (SPID_PK);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_PODMO
ALTER TABLE SAP_DEL_PODMO ADD CONSTRAINT RF01_DISCONRECONDEREGSTATUS CHECK (DISCONRECONDEREGSTATUS IN ('REC','TDISC'));

--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_PODMO IS 'Point of Delivery MOSL data table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_PODMO
COMMENT ON COLUMN SAP_DEL_PODMO.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_PODMO.EFFECTIVEFROMDATE IS 'Supply Point Effective From Date~~~D2013 - Date that the Supply Point takes effect in the Central Systems.  This can be the date the connection was completed for newly established Supply Points, or the date that a Gap Site of Entry Change of Use takes effect.';
COMMENT ON COLUMN SAP_DEL_PODMO.DISCONRECONDEREGSTATUS IS 'Disconnection/Reconnection/Deregistration~~~D2025 - Declares a Supply Point Disconnection, Reconnection or Deregistration. Also enables the distinction between a Temporary Disconnection and a Permanent Disconnection. Must contain â??TDISCâ?? if Supply Point is temporarily disconnected, otherwise must be unpopulated'; 
COMMENT ON COLUMN SAP_DEL_PODMO.MULTIPLEWHOLESALERFLAG IS 'Multiple Wholesalers Flag~~~D2053 - Boolean flag to indicate that there are multiple wholesalers for the same category of SPID at one site (where only the lead wholesaler is identified in the market and associated with the SPID)';
COMMENT ON COLUMN SAP_DEL_PODMO.LANDLORDSPID IS 'Landlord SPID~~~D2070 - Identifies the Landlord Supply point in a multi-occupancy Eligible Premises.  The valid set for this is all SPIDs'; 
COMMENT ON COLUMN SAP_DEL_PODMO.PARENTLEGACYRECNUM IS 'Foreign Key to SAP_DEL_POD table LEGACYRECNUM';
COMMENT ON COLUMN SAP_DEL_PODMO.STWPROPERTYNUMBER IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_PODMO.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_PODMO.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
--------------------------------------------------------------------------------------------------------
-- SAP_DEL_PODSRV
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_PODSRV
  (
    LEGACYRECNUM          VARCHAR2(30) CONSTRAINT CH01_PODSRV_LEGACYRECNUM NOT NULL,
    SAPFLOCNUMBER            VARCHAR2(10) CONSTRAINT CH01_PODSRV_SAPFLOCNUMBER NOT NULL,
    SPID_PK                  VARCHAR2(13) CONSTRAINT CH01_PODSRV_SPID NOT NULL,
    STWPROPERTYNUMBER        VARCHAR2(9) CONSTRAINT CH01_PODSRV_PROPERTYNUMBER NOT NULL,
    REGISTRATIONSTARTDATE DATE,
    ORGTYPE               VARCHAR2(1) CONSTRAINT CH01_PODSRV_ORGTYPE NOT NULL,
    ORGID_PK              VARCHAR2(12) CONSTRAINT CH01_POD_ORGID NOT NULL
  );

--ADD FORIEGN KEYS
ALTER TABLE SAP_DEL_PODSRV ADD CONSTRAINT FK01_PODSRV_SPID FOREIGN KEY (SPID_PK)  REFERENCES SAP_DEL_POD (SPID_PK);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_PODSRV
ALTER TABLE SAP_DEL_PODSRV ADD CONSTRAINT RF01_ORGTYPE CHECK (ORGTYPE IN ('W','R'));

--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_PODSRV IS 'Point of Delivery Service Assignment data table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_PODSRV
COMMENT ON COLUMN SAP_DEL_PODSRV.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_PODSRV.REGISTRATIONSTARTDATE IS 'Registration Start Date~~~D4002 - Date SPID becomes registered to a Retailer';
COMMENT ON COLUMN SAP_DEL_PODSRV.ORGTYPE IS 'Retailer (R) or Whole        saler (W) - taken from MO_ORG.ORGTYPE - acts as ~~~D4012 which is not in MOSL delivery';
COMMENT ON COLUMN SAP_DEL_PODSRV.ORGID_PK IS 'Wholesaler ID/Retailer ID/~~~D4025 also covers D4011, D4018 - Unique ID identifying the Wholesaler. STW003 - FK implementing relationship to Tariff and Tariff Band (if required).';
COMMENT ON COLUMN SAP_DEL_PODSRV.STWPROPERTYNUMBER IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_PODSRV.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_PODSRV.SPID_PK IS 'SPID~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
--------------------------------------------------------------------------------------------------------
-- SAP_DEL_COB
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_COB
  (
    LEGACYRECNUM              VARCHAR2(30) CONSTRAINT CH01_COB_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM        VARCHAR2(30) CONSTRAINT CH01_COB_PARENTLEGACYRECNUM NOT NULL, -- CONNECTION OBJECT LEGACY RECNUM (SAPFLOCNUMBER)
    SAPFLOCNUMBER             VARCHAR2(10) CONSTRAINT CH01_COB_SAPFLOCNUMBER NOT NULL,
    STWPROPERTYNUMBER            VARCHAR2(9) CONSTRAINT CH01_COB_PROPERTYNUMBER NOT NULL,
    UPRNREASONCODE            VARCHAR2(2 BYTE),
    PAIRINGREFREASONCODE      VARCHAR2(12 BYTE),
    CITY                            VARCHAR2(255 byte),
    DISTRICT                        VARCHAR2(255 BYTE),
    POSTCODE                        VARCHAR2(8 BYTE)    CONSTRAINT CH01_COB_POSTCODE NOT NULL ENABLE,
    STREET                          VARCHAR2(255 BYTE), -- V0.15  CONSTRAINT CH01_COB_STREET NOT NULL ENABLE,
    HOUSENUMBER                     VARCHAR2(255 byte),  -- V0.15  CH01_COB_HOUSENUMBER NOT NULL ENABLE,    
    STREET2                         VARCHAR2(255 BYTE),
    STREET3                         VARCHAR2(100 byte),
    STREET4                         VARCHAR2(255 BYTE),
    PAFADDRESSKEY                   NUMBER(9,0),    
    STREET5                         VARCHAR2(255 BYTE),
    COUNTRY                         VARCHAR2(32 BYTE),
    UPRN                            NUMBER(13,0),    
    GPSCOORDINATES                  VARCHAR2(15),
--    GPSX                            NUMBER(7,1), -- V0.07
--    GPSY                            NUMBER(7,1) -- V0.07
    TREATMENTWORKS              VARCHAR2(10) -- V0.12 
  );
  
-- ADD PRIMARY KEY TO TABLE
ALTER TABLE SAP_DEL_COB ADD CONSTRAINT SAP_DEL_COB_PK PRIMARY KEY (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_COB 
ALTER TABLE SAP_DEL_COB ADD CONSTRAINT RF01_PAIRINGREFREASONCODE CHECK (PAIRINGREFREASONCODE IN ('NOSPID', 'NOTELIGIBLE'));

CREATE INDEX IDX_STWPROP_COB ON SAP_DEL_COB(STWPROPERTYNUMBER);

--ADD COMMENT ON TABLE
COMMENT ON TABLE SAP_DEL_COB IS 'Connection Object header table';

-- ADD COMMENTS OF COLUMNS IN SAP_DEL_COB
COMMENT ON COLUMN SAP_DEL_COB.UPRN IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN SAP_DEL_COB.PAFADDRESSKEY IS 'PAF Address Key~~~D5011 - PAF Address Key if known';
COMMENT ON COLUMN SAP_DEL_COB.UPRNREASONCODE IS 'UPRN Reason Code~~~D2040 - Code to explain the absence or duplicate of a UPRN (in valid set)';
COMMENT ON COLUMN SAP_DEL_COB.PAIRINGREFREASONCODE IS 'Pairing Reference Reason Code~~~D2086 - Reason code for the absence of a pairing reference when requesting a new SPID. Must be populated with â??NOSPIDâ?? if there is no pair for this Supply Point, otherwise it must be unpopulated';
COMMENT ON COLUMN SAP_DEL_COB.CITY IS 'Address Line 5~~~D5008 - Fifth line of address, or fifth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_COB.DISTRICT IS 'Address Line 4~~~D5007 - Fourth line of address, or fourth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_COB.POSTCODE IS 'Postcode~~~D5009 - Postcode (without spaces)';
COMMENT ON COLUMN SAP_DEL_COB.STREET IS 'STREET~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_COB.HOUSENUMBER IS 'HOUSENUMBER~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_COB.STREET2 IS '~~~D5002 - BS7666 Secondary Addressable Object if available';
COMMENT ON COLUMN SAP_DEL_COB.STREET3 IS '~~~D5003 - BS7666 Primary Addressable Object if available';
COMMENT ON COLUMN SAP_DEL_COB.STREET4 IS 'Address Line 2~~~D5005 - Second line of address, or second line of address after Secondary Addressable Object and Primary Addressable Object if used';  
COMMENT ON COLUMN SAP_DEL_COB.PAFADDRESSKEY IS 'PAF Address Key~~~D5011 - PAF Address Key if known';  
COMMENT ON COLUMN SAP_DEL_COB.STREET5 IS 'Address Line 3~~~D5006 - Third line of address, or third line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_COB.COUNTRY IS 'Country~~~D5010 - Country, if address is outside the UK';
COMMENT ON COLUMN SAP_DEL_COB.UPRN IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN SAP_DEL_COB.GPSCOORDINATES IS 'Replaces GPSX and GPSY - SAP expected coordinates for property but we do not have that data so will pass NULL value in file';
--COMMENT ON COLUMN SAP_DEL_COB.GPSX IS 'Meter GISX~~~D3017 - Specifies the X coordinate of the location of the meter, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
--COMMENT ON COLUMN SAP_DEL_COB.GPSY IS 'Meter GISY~~~D3018 - Specifies the Y coordinate of the location of the meter, in OSGB all numeric northings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_COB.TREATMENTWORKS IS 'Sewerage Treatment Works'; --V0.12

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_COBMO
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_COBMO
  (
    LEGACYRECNUM           VARCHAR2(30) CONSTRAINT CH01_COBMO_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM     VARCHAR2(30) CONSTRAINT CH01_COBMO_PARENTLEGACYRECNUM NOT NULL,
    SAPFLOCNUMBER          VARCHAR2(10)CONSTRAINT CH01_COBMO_SAPFLOCNUMBER NOT NULL,
--    SAPEQUIPMENT           NUMBER(10,0),
    STWPROPERTYNUMBER         VARCHAR2(9) CONSTRAINT CH01_COBMO_PROPERTYNUMBER NOT NULL,
    EFFECTIVEFROMDATE      DATE,
    RATEABLEVALUE          NUMBER(10,2),
    OCCUPENCYSTATUS        VARCHAR2(12 BYTE) CONSTRAINT CH01_OCCUPENCYSTATUS NOT NULL,
    BUILDINGWATERSTATUS    NUMBER(1,0) CONSTRAINT CH01_BUILDINGWATERSTATUS NOT NULL,
    VACANCYCHALLENGEFLAG   VARCHAR2(1) CONSTRAINT CH01_VACANCYCHALLENGEFLAG NOT NULL, -- D2032 - Not defined in transform area
    VOABAREFERENCE         VARCHAR2(32 BYTE),
    VOABAREFRSNCODE        VARCHAR2(2 BYTE),
    SECTION154             NUMBER(4,0),
    PUBHEALTHRELSITEARR    NUMBER(1,0) CONSTRAINT CH01_PUBHEALTHRELSITEARR NOT NULL,
    NONPUBHEALTHRELSITE    NUMBER(1,0) CONSTRAINT CH01_NONPUBHEALTHRELSITE NOT NULL,
    NONPUBHEALTHRELSITEDSC VARCHAR2(255 BYTE)
  );
  
--ADD FORIEGN KEYS TO TABLE
ALTER TABLE SAP_DEL_COBMO ADD CONSTRAINT FK01_COBMO_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_COB (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_COBMO
ALTER TABLE SAP_DEL_COBMO ADD CONSTRAINT RF01_OCCUPENCYSTATUS CHECK (OCCUPENCYSTATUS         IN ('OCCUPIED', 'VACANT'));
ALTER TABLE SAP_DEL_COBMO ADD CONSTRAINT RF01_VOABAREFRSNCODE CHECK (VOABAREFRSNCODE         IN ('NR','ME','AG','SR','MT','IP','SP','OT'));
ALTER TABLE SAP_DEL_COBMO ADD CONSTRAINT RF01_BUILDINGWATERSTATUS CHECK (BUILDINGWATERSTATUS IN (0,1));
ALTER TABLE SAP_DEL_COBMO ADD CONSTRAINT RF01_NONPUBHEALTHRELSITE CHECK (NONPUBHEALTHRELSITE IN (0,1));
ALTER TABLE SAP_DEL_COBMO ADD CONSTRAINT RF01_PUBHEALTHRELSITEARR CHECK (PUBHEALTHRELSITEARR IN (0,1));

--ADD COMMENT ON TABLE
COMMENT ON TABLE SAP_DEL_COBMO IS 'Connection Object MOSL data table';

-- ADD COMMENTS OF COLUMNS IN SAP_DEL_COBMO
COMMENT ON COLUMN SAP_DEL_COBMO.RATEABLEVALUE IS 'Rateable Value~~~D2011 - Rateable Value of Eligible Premises in Â£';
COMMENT ON COLUMN SAP_DEL_COBMO.OCCUPENCYSTATUS IS 'Occupancy Status~~~D2015 - Declares premises for the SPID as Vacant or Occupied';
COMMENT ON COLUMN SAP_DEL_COBMO.VOABAREFERENCE IS 'VOA BA Reference~~~D2037 - Valuation Office Agency Billing Authority Reference Number';
COMMENT ON COLUMN SAP_DEL_COBMO.VOABAREFRSNCODE IS 'VOA BA Reference Reason Code~~~D2038 - Code to explain the absence or duplication of a Valuation Office Agency Billing Authority Reference. (in valid set)';
COMMENT ON COLUMN SAP_DEL_COBMO.BUILDINGWATERSTATUS IS 'Building Water Status~~~D2029 - Boolean flag to indicate if the site is a building construction site. ';
COMMENT ON COLUMN SAP_DEL_COBMO.NONPUBHEALTHRELSITE IS 'Non-Public Health Related Site Specific Arrangements Flag~~~D2093 - Indication of whether or not a site specific management plan is in place, and not for public health related reasons';
COMMENT ON COLUMN SAP_DEL_COBMO.NONPUBHEALTHRELSITEDSC IS 'Non-Public Health Related Site Specific Arrangements Free Descriptor~~~D2094 - Free descriptor for indication of the nature of site specific management plan in place, when not for public health related reasons';
COMMENT ON COLUMN SAP_DEL_COBMO.PUBHEALTHRELSITEARR IS 'Public Health Related Site Specific Arrangements Flag~~~D2087 - Boolean flag to Indicate whether or not a site specific management plan is in place for public health related reasons';
COMMENT ON COLUMN SAP_DEL_COBMO.SECTION154 IS 'Section 154A Dwelling Units~~~D2074 - The number of dwelling units at an Eligible Premises that are eligible to receive Section 154A payments under the Water Industry Act 1991';
COMMENT ON COLUMN SAP_DEL_COBMO.STWPROPERTYNUMBER IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_COBMO.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
--COMMENT ON COLUMN SAP_DEL_COBMO.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_PREM
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_PREM
  (
    LEGACYRECNUM           VARCHAR2(30) CONSTRAINT CH01_PREM_LEGACYRECNUM NOT NULL,
    CONOBJLEGACYRECNUM     VARCHAR2(30) CONSTRAINT CH01_PREM_PARENTLEGACYRECNUM NOT NULL,
    SAPFLOCNUMBER          VARCHAR2(10) CONSTRAINT CH01_PREM_SAPFLOCNUMBER NOT NULL,
--    SAPEQUIPMENT           NUMBER(10,0),
    STWPROPERTYNUMBER      VARCHAR2(9) CONSTRAINT CH01_PREM_PROPERTYNUMBER NOT NULL
  ) ;  

-- ADD PRIMARY KEY TO TABLE  
ALTER TABLE SAP_DEL_PREM ADD CONSTRAINT SAP_DEL_PREM_PK PRIMARY KEY (LEGACYRECNUM);  


--ADD FORIEGN KEYS TO TABLES
ALTER TABLE SAP_DEL_PREM ADD CONSTRAINT FK01_PREM_LEGACYRECNUM  FOREIGN KEY (CONOBJLEGACYRECNUM)  REFERENCES SAP_DEL_COB (LEGACYRECNUM);

CREATE INDEX IDX_STWPROP_PREM ON SAP_DEL_PREM(STWPROPERTYNUMBER);

--ADD COMMENT ON TABLE
COMMENT ON TABLE SAP_DEL_PREM IS 'Connection Object Premises Create table';

-- ADD COMMENTS OF COLUMNS IN SAP_DEL_PREM
COMMENT ON COLUMN SAP_DEL_PREM.STWPROPERTYNUMBER IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_PREM.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
--COMMENT ON COLUMN SAP_DEL_PREM.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
--------------------------------------------------------------------------------------------------------
-- SAP_DEL_DEV
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_DEV
  (
    LEGACYRECNUM             VARCHAR(30) CONSTRAINT CH01_DEV_LEGACYRECNUM NOT NULL,
    --DEVICE HEADER
    METERTREATMENT           VARCHAR2(32 BYTE) CONSTRAINT CH01_DEV_METERTREATMENT NOT NULL,
    MANUFACTURER_PK          VARCHAR2(32 BYTE),
    MANUFACTURERSERIALNUM_PK VARCHAR2(32 BYTE),
    MANUFACTURERMODEL        VARCHAR2(30),
    NUMBEROFDIGITS           NUMBER CONSTRAINT CH01_DEV_NUMBEROFDIGITS NOT NULL,
    --DEVICE CHARACTERISTICS
    METERREADFREQUENCY          VARCHAR(1)   CONSTRAINT CH01_METERREADMINFREQUENCY NOT NULL,
    DATALOGGERWHOLESALER        NUMBER(1)    CONSTRAINT CH01_DATALOGGERWHOLESALER NOT NULL,
    DATALOGGERNONWHOLESALER     NUMBER(1)    CONSTRAINT CH01_DATALOGGERNONWHOLESALER NOT NULL,
    METERLOCATIONCODE           VARCHAR(1)   CONSTRAINT CH01_METERLOCATIONCODE NOT NULL,
    METEROUTREADERGPSX          NUMBER(7,1) ,  -- Changed because in Oracle 6,1 only allows 5 digits before the decimal point
    METEROUTREADERGPSY          NUMBER(7,1) ,  -- Changed because in Oracle 6,1 only allows 5 digits before the decimal point
    METEROUTREADERLOCCODE       VARCHAR(1) ,
    COMBIMETERFLAG              NUMBER(1)    CONSTRAINT CH01_COMBIMETERFLAG NOT NULL,
    REMOTEREADFLAG              NUMBER(1)    CONSTRAINT CH01_REMOTEREADFLAG NOT NULL,
    OUTREADERID                 VARCHAR(32) ,
    OUTREADERPROTOCOL           VARCHAR(255) ,
    REMOTEREADTYPE              VARCHAR(12) ,
    PHYSICALMETERSIZE           NUMBER(4) ,
    --DEVICE LOCATION
    CITY                            VARCHAR2(255 byte),
    DISTRICT                        VARCHAR2(255 BYTE),
    POSTCODE                        VARCHAR2(8 BYTE)    CONSTRAINT CH01_DEV_POSTCODE NOT NULL ENABLE,
    STREET                          VARCHAR2(255 BYTE),  -- V0.15 CONSTRAINT CH01_DEV_STREET NOT NULL ENABLE,
    HOUSENUMBER                     VARCHAR2(255 byte),  -- V0.15 CONSTRAINT CH01_DEV_HOUSENUMBER NOT NULL ENABLE,    
    STREET2                         VARCHAR2(255 BYTE),
    STREET3                         VARCHAR2(100 byte),
    STREET4                         VARCHAR2(255 BYTE),
    PAFADDRESSKEY                   NUMBER(9,0),    
    LOCATIONFREETEXTDESCRIPTOR      VARCHAR2(255 BYTE),
    POBOX                           VARCHAR2(32 BYTE),
    STREET5                         VARCHAR2(255 BYTE),
    COUNTRY                         VARCHAR2(32 BYTE),
    UPRNREASONCODE                  VARCHAR2(2 BYTE),
    GPSX                            NUMBER(7,1),
    GPSY                            NUMBER(7,1),
    UPRN                            NUMBER(13,0),
    --DEVICE MO
    --EFFECTIVEFROMDATE      DATE,
    --YEARLYVOLESTIMATE      NUMBER(13,0),
    --WATERCHARGEMETERSIZE   NUMBER(4,0) CONSTRAINT CH01_WATERCHARGEMETERSIZE NOT NULL,
    --SEWCHARGEABLEMETERSIZE NUMBER(4,0),
    --RETURNTOSEWER          NUMBER(5,2),    
    STWPROPERTYNUMBER_PK	 NUMBER(9,0),
    SAPFLOCNUMBER          NUMBER(30,0),
    SAPEQUIPMENT           NUMBER(10,0),
    STWMETERREF            NUMBER(15,0),
    NONMARKETMETERFLAG     NUMBER(1)
  );

ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT PK_SAP_DEL_DEV PRIMARY KEY (LEGACYRECNUM);

--FOREIGN KEYS
--ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT FK01_DEV_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_COM (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_DEV 
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_METERTREATMENT CHECK (METERTREATMENT IN ('POTABLE', 'NONPOTABLE', 'PRIVATEWATER', 'PRIVATETE', 'SEWERAGE', 'CROSSBORDER'));
--DEVICE CHARACTERISTICS CONSTRAINTS
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF02_METERREADFREQUENCY CHECK (METERREADFREQUENCY IN ('B','M'));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_DATALOGGERWHOLESALER CHECK (DATALOGGERWHOLESALER IN (0,1));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_DATALOGGERNONWHOLESALER CHECK (DATALOGGERNONWHOLESALER IN (0,1));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF02_METERLOCATIONCODE CHECK (METERLOCATIONCODE IN ('I', 'O'));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_METEROUTREADERGPSX CHECK ((OUTREADERID IS NOT NULL AND METEROUTREADERGPSX IS NOT NULL) OR (OUTREADERID IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH02_METEROUTREADERGPSX CHECK ((METEROUTREADERGPSX >= 82644 AND METEROUTREADERGPSX <= 655612) OR (METEROUTREADERGPSX IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_METEROUTREADERGPSY CHECK ((OUTREADERID IS NOT NULL AND METEROUTREADERGPSY IS NOT NULL) OR (OUTREADERID IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH02_METEROUTREADERGPSY CHECK ((METEROUTREADERGPSY >= 5186 AND METEROUTREADERGPSY <= 657421) OR (METEROUTREADERGPSY IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_METERLOCATIONCODE CHECK (METERLOCATIONCODE IN ('I', 'O'));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_COMBIMETERFLAG CHECK (COMBIMETERFLAG IN (1,0));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_REMOTEREADFLAG CHECK (REMOTEREADFLAG IN (1,0));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_OUTREADERID CHECK ((REMOTEREADFLAG = 1 AND OUTREADERID IS NOT NULL) OR (OUTREADERID IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_OUTREADERPROTOCOL CHECK ((REMOTEREADFLAG = 1 AND OUTREADERPROTOCOL IS NOT NULL) OR (OUTREADERPROTOCOL IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT RF01_REMOTEREADTYPE CHECK (REMOTEREADTYPE IN ('TOUCH', '1WAYRAD', '2WRAD', 'GPRS', 'OTHER'));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_REMOTEREADTYPE CHECK ((REMOTEREADFLAG = 1 AND REMOTEREADTYPE IS NOT NULL) OR (REMOTEREADTYPE IS NULL));
ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_SAPFLOCNUMBER CHECK ((NONMARKETMETERFLAG = 0 AND SAPFLOCNUMBER IS NOT NULL) OR (NONMARKETMETERFLAG = 1));
--DEVICE MO CONSTRAINTS
--ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_YEARLYVOLESTIMATE CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER', 'CROSSBORDER')) AND YEARLYVOLESTIMATE IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER'))));
--ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH02_WATERCHARGEMETERSIZE CHECK ((METERTREATMENT IN ('POTABLE','NONPOTABLE') AND WATERCHARGEMETERSIZE > 0) OR (METERTREATMENT NOT IN ('POTABLE','NONPOTABLE') AND WATERCHARGEMETERSIZE = 0));
--ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_SEWCHARGEABLEMETERSIZE CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER', 'CROSSBORDER')) AND SEWCHARGEABLEMETERSIZE IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER'))));
--ALTER TABLE SAP_DEL_DEV ADD CONSTRAINT CH01_RETURNTOSEWER CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER', 'CROSSBORDER')) AND RETURNTOSEWER IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER'))));

COMMENT ON TABLE SAP_DEL_DEV IS 'Device (Meter) header table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DEV
COMMENT ON COLUMN SAP_DEL_DEV.LEGACYRECNUM IS 'Primary Key';
--COMMENT ON COLUMN SAP_DEL_DEV.PARENTLEGACYRECNUM IS 'Foreign Key to Connection Object';

--HEADER
COMMENT ON COLUMN SAP_DEL_DEV.MANUFACTURER_PK IS 'Meter Manufacturer~~~D3013 - Specifies the make and/or manufacturer of a meter.  STW010 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Manufacturer field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN SAP_DEL_DEV.MANUFACTURERSERIALNUM_PK IS 'Manufacturer Meter Serial Number~~~D3014 - Specifies the manufacturerâ??s serial number of a meter. STW011 - A concatination of both the Meter manufacturer value and Meter serial number are to be stored in Meter Serial field.  Reason bng that there are instances where the serial number is not unique.';
COMMENT ON COLUMN SAP_DEL_DEV.METERTREATMENT IS 'Meter Treatment~~~D3022 - Specifies whether the meter is a Wholesaler Water Meter or one of the various types of Private Meter';
COMMENT ON COLUMN SAP_DEL_DEV.NUMBEROFDIGITS IS 'Number of Digits~~~D3004 - The number of digits required to provide a reading in m3. For the avoidance of doubt, this is irrespective of the actual number of dials or digits on the meter, as meters may record volumes to a higher or lower resolution than 1m3. However, this Data Item is required with reference to m3 for the purposes of rollover detection. This will also be the number of digits required for the maximum volume in m3 that can be recorded by the meter';
--CHARACTERISTICS
COMMENT ON COLUMN SAP_DEL_DEV.METERREADFREQUENCY IS 'Meter Read Minimum Frequency~~~D3011 - The minimum frequency with which the Retailer must read a meter';
COMMENT ON COLUMN SAP_DEL_DEV.DATALOGGERWHOLESALER IS 'Datalogger (Wholesaler)~~~D3015 - Specifies the presence of a Wholesaler datalogger';
COMMENT ON COLUMN SAP_DEL_DEV.DATALOGGERNONWHOLESALER IS 'Datalogger (Non-Wholesaler)~~~D3016 - Specifies the presence of a non-Wholesaler datalogger';
COMMENT ON COLUMN SAP_DEL_DEV.METERLOCATIONCODE IS 'Meter Location Code~~~D3025 - Indicates Meter Location as either inside or outside (of a building)';
COMMENT ON COLUMN SAP_DEL_DEV.METEROUTREADERGPSX IS 'Meter Outreader GISX~~~D3030 - Specifies the X coordinate of the location of the meter outreader, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DEV.METEROUTREADERGPSY IS 'Meter Outreader GISY~~~D3031 - Specifies the Y coordinate of the location of the meter outreader, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DEV.METEROUTREADERLOCCODE IS 'Meter Outreader Location Code~~~D3033 - Indicates Meter Outreader Location as either inside or outside (of a building)';
COMMENT ON COLUMN SAP_DEL_DEV.COMBIMETERFLAG IS 'Combi Meter Flag~~~D3034 - Indicates if meter is part of a combi meter. Each part of a combi meter must have this flag set.';
COMMENT ON COLUMN SAP_DEL_DEV.REMOTEREADFLAG IS 'Remote Read Flag~~~D3037 - Indicates if a meter has the capability to be read remotely, including via an Outreader';
COMMENT ON COLUMN SAP_DEL_DEV.OUTREADERID IS 'Outreader ID~~~D3039 - Free text Data Item for the encoder reference, radio ID or logger number, or any other reference which will assist the Retailer in reading the meter';
COMMENT ON COLUMN SAP_DEL_DEV.OUTREADERPROTOCOL IS 'Outreader Protocol~~~D3040 - Free text providing details of how the reading is accessed by the outreader. Will typically identify the manufacturers  protocol in use';
COMMENT ON COLUMN SAP_DEL_DEV.REMOTEREADTYPE IS 'Remote Read Type~~~D3038 - Indicates the type of remote read capability for the meter';
COMMENT ON COLUMN SAP_DEL_DEV.PHYSICALMETERSIZE IS 'Physical Meter Size~~~D3003 - Nominal size of the meter in mm e.g. for a DN15 meter the Physical Meter Size is 15';
--LOCATION
COMMENT ON COLUMN SAP_DEL_DEV.CITY IS 'Address Line 5~~~D5008 - Fifth line of address, or fifth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DEV.DISTRICT IS 'Address Line 4~~~D5007 - Fourth line of address, or fourth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DEV.POSTCODE IS 'Postcode~~~D5009 - Postcode (without spaces)';
COMMENT ON COLUMN SAP_DEL_DEV.STREET IS 'STREET~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DEV.HOUSENUMBER IS 'HOUSENUMBER~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DEV.STREET2 IS '~~~D5002 - BS7666 Secondary Addressable Object if available';
COMMENT ON COLUMN SAP_DEL_DEV.STREET3 IS '~~~D5003 - BS7666 Primary Addressable Object if available';
COMMENT ON COLUMN SAP_DEL_DEV.STREET4 IS 'Address Line 2~~~D5005 - Second line of address, or second line of address after Secondary Addressable Object and Primary Addressable Object if used';  
COMMENT ON COLUMN SAP_DEL_DEV.PAFADDRESSKEY IS 'PAF Address Key~~~D5011 - PAF Address Key if known';  
COMMENT ON COLUMN SAP_DEL_DEV.LOCATIONFREETEXTDESCRIPTOR IS 'Free Descriptor~~~D5001 - Free text descriptor for address/location  details';  
COMMENT ON COLUMN SAP_DEL_DEV.POBOX IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN SAP_DEL_DEV.STREET5 IS 'Address Line 3~~~D5006 - Third line of address, or third line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DEV.COUNTRY IS 'Country~~~D5010 - Country, if address is outside the UK';
COMMENT ON COLUMN SAP_DEL_DEV.GPSX IS 'Meter GISX~~~D3017 - Specifies the X coordinate of the location of the meter, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DEV.GPSY IS 'Meter GISY~~~D3018 - Specifies the Y coordinate of the location of the meter, in OSGB all numeric northings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
--MO UPDATE
--COMMENT ON COLUMN SAP_DEL_DEV.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
--COMMENT ON COLUMN SAP_DEL_DEV.WATERCHARGEMETERSIZE IS 'Water Chargeable Meter Size~~~D3002 - Meter size for Water Services tariff charge calculation purposes in mm. In most cases this will equal physical meter size';
--COMMENT ON COLUMN SAP_DEL_DEV.RETURNTOSEWER IS 'Return to Sewer~~~D3007 - The fraction of the volume which is deemed to return to sewer for a particular meter in %';
--COMMENT ON COLUMN SAP_DEL_DEV.SEWCHARGEABLEMETERSIZE IS 'Sewerage Chargeable Meter Size~~~D3005 - Meter size for Foul Sewerage Services tariff charge calculation purposes in mm';
--COMMENT ON COLUMN SAP_DEL_DEV.YEARLYVOLESTIMATE IS 'Yearly Volume Estimate~~~D2010 - An estimate of the annual Volume supplied in relation to a meter or Discharge Point on the basis of the relevant Eligible Premises being Occupied Premises. Value in m3/a';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DEV FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_DEV.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_DEV.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_DEV.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_DEV.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';
--OTHER COMMENTS FOR NON_OUTPUT FIElDS
COMMENT ON COLUMN SAP_DEL_DEV.MANUFACTURERMODEL IS 'Device Model Number - N/A - SAP only placeholder field';
COMMENT ON COLUMN SAP_DEL_DEV.UPRNREASONCODE IS 'UPRN Reason Code~~~D2040 - Code to explain the absence or duplicate of a UPRN (in valid set)';
COMMENT ON COLUMN SAP_DEL_DEV.UPRN IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN SAP_DEL_DEV.NONMARKETMETERFLAG IS 'Flag to indicate if Marketable or NON-Marketable Meter';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_DEVMO
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_DEVMO
  (
    LEGACYRECNUM            VARCHAR(30) CONSTRAINT CH01_DMO_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM      VARCHAR(30) CONSTRAINT CH01_DMO_PARENTLEGACYRECNUM NOT NULL,
    EFFECTIVEFROMDATE       DATE,
    YEARLYVOLESTIMATE       NUMBER(13,0),
    WATERCHARGEMETERSIZE    NUMBER(4,0) CONSTRAINT CH01_WATERCHARGEMETERSIZE NOT NULL,
    SEWCHARGEABLEMETERSIZE  NUMBER(4,0),
    RETURNTOSEWER           NUMBER(5,2),
    METERTREATMENT          VARCHAR2(32 BYTE) CONSTRAINT CH01_METERTREATMENT NOT NULL,
    STWPROPERTYNUMBER_PK	  NUMBER(9,0),
    SAPFLOCNUMBER           NUMBER(30,0),
    SAPEQUIPMENT            NUMBER(10,0),
    STWMETERREF             NUMBER(15,0)
  );

ALTER TABLE SAP_DEL_DEVMO ADD CONSTRAINT PK_SAP_DEL_DEVMO PRIMARY KEY (LEGACYRECNUM);

ALTER TABLE SAP_DEL_DEVMO ADD CONSTRAINT FK01_DMO_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_DEVMO 
ALTER TABLE SAP_DEL_DEVMO ADD CONSTRAINT CH01_YEARLYVOLESTIMATE CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER', 'CROSSBORDER')) AND YEARLYVOLESTIMATE IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER'))));
ALTER TABLE SAP_DEL_DEVMO ADD CONSTRAINT CH02_WATERCHARGEMETERSIZE CHECK ((METERTREATMENT IN ('POTABLE','NONPOTABLE') AND WATERCHARGEMETERSIZE > 0) OR (METERTREATMENT NOT IN ('POTABLE','NONPOTABLE') AND WATERCHARGEMETERSIZE = 0));
ALTER TABLE SAP_DEL_DEVMO ADD CONSTRAINT CH01_SEWCHARGEABLEMETERSIZE CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATEWATER', 'CROSSBORDER')) AND SEWCHARGEABLEMETERSIZE IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATEWATER'))));


ALTER TABLE SAP_DEL_DEVMO ADD CONSTRAINT CH01_RETURNTOSEWER CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER', 'CROSSBORDER')) AND RETURNTOSEWER IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATETE', 'PRIVATEWATER'))));

COMMENT ON TABLE SAP_DEL_DEVMO IS 'Device (Meter) child MO table';

-- ADD COMMENTS OF COLUMNS IN SAP_DEL_DEVMO (FOR MOSL UPDATE)
COMMENT ON COLUMN SAP_DEL_DEVMO.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_DEVMO.PARENTLEGACYRECNUM IS 'Foreign Key to Device Header';
--MO DETAILS
COMMENT ON COLUMN SAP_DEL_DEVMO.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN SAP_DEL_DEVMO.WATERCHARGEMETERSIZE IS 'Water Chargeable Meter Size~~~D3002 - Meter size for Water Services tariff charge calculation purposes in mm. In most cases this will equal physical meter size';
COMMENT ON COLUMN SAP_DEL_DEVMO.RETURNTOSEWER IS 'Return to Sewer~~~D3007 - The fraction of the volume which is deemed to return to sewer for a particular meter in %';
COMMENT ON COLUMN SAP_DEL_DEVMO.SEWCHARGEABLEMETERSIZE IS 'Sewerage Chargeable Meter Size~~~D3005 - Meter size for Foul Sewerage Services tariff charge calculation purposes in mm';
COMMENT ON COLUMN SAP_DEL_DEVMO.YEARLYVOLESTIMATE IS 'Yearly Volume Estimate~~~D2010 - An estimate of the annual Volume supplied in relation to a meter or Discharge Point on the basis of the relevant Eligible Premises being Occupied Premises. Value in m3/a';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DEVMO FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_DEVMO.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_DEVMO.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_DEVMO.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_DEVMO.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';
--OTHER COMMENTS FOR NON_OUTPUT FIElDS
COMMENT ON COLUMN SAP_DEL_DEVMO.METERTREATMENT IS 'Meter Treatment~~~D3022 - Specifies whether the meter is a Wholesaler Water Meter or one of the various types of Private Meter';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_DEVCHAR
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_DEVCHAR
  (
    LEGACYRECNUM             VARCHAR(30) CONSTRAINT CH01_DEC_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM       VARCHAR(30) CONSTRAINT CH01_DEC_PARENTLEGACYRECNUM NOT NULL,
    --DEVICE CHARACTERISTICS
    METERREADFREQUENCY          VARCHAR(1)   CONSTRAINT CH02_METERREADMINFREQUENCY NOT NULL,
    DATALOGGERWHOLESALER        NUMBER(1)    CONSTRAINT CH02_DATALOGGERWHOLESALER NOT NULL,
    DATALOGGERNONWHOLESALER     NUMBER(1)    CONSTRAINT CH02_DATALOGGERNONWHOLESALER NOT NULL,
    METERLOCATIONCODE           VARCHAR(1)   CONSTRAINT CH02_METERLOCATIONCODE NOT NULL,
    METEROUTREADERGPSX          NUMBER(7,1) ,  -- Changed because in Oracle 6,1 only allows 5 digits before the decimal point
    METEROUTREADERGPSY          NUMBER(7,1) ,  -- Changed because in Oracle 6,1 only allows 5 digits before the decimal point
    METEROUTREADERLOCCODE       VARCHAR(1) ,
    COMBIMETERFLAG              NUMBER(1)    CONSTRAINT CH02_COMBIMETERFLAG NOT NULL,
    REMOTEREADFLAG              NUMBER(1)    CONSTRAINT CH02_REMOTEREADFLAG NOT NULL,
    OUTREADERID                 VARCHAR(32) ,
    OUTREADERPROTOCOL           VARCHAR(255) ,
    REMOTEREADTYPE              VARCHAR(12) ,
    PHYSICALMETERSIZE           NUMBER(4) ,
    METERTREATMENT              VARCHAR2(32 BYTE),
    STWPROPERTYNUMBER_PK	 NUMBER(9,0),
    SAPFLOCNUMBER          NUMBER(30,0),
    SAPEQUIPMENT           NUMBER(10,0),
    STWMETERREF            NUMBER(15,0)
  );

ALTER TABLE SAP_DEL_DEVCHAR ADD CONSTRAINT PK_SAP_DEL_DEVCHAR PRIMARY KEY (LEGACYRECNUM);

--FOREIGN KEYS
ALTER TABLE SAP_DEL_DEVCHAR ADD CONSTRAINT FK01_DEC_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);

COMMENT ON TABLE SAP_DEL_DEVCHAR IS 'Device (Meter) Characteristics table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DEVCHAR
COMMENT ON COLUMN SAP_DEL_DEVCHAR.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.PARENTLEGACYRECNUM IS 'Foreign Key to Device Header';
--CHARACTERISTICS
COMMENT ON COLUMN SAP_DEL_DEVCHAR.METERREADFREQUENCY IS 'Meter Read Minimum Frequency~~~D3011 - The minimum frequency with which the Retailer must read a meter';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.DATALOGGERWHOLESALER IS 'Datalogger (Wholesaler)~~~D3015 - Specifies the presence of a Wholesaler datalogger';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.DATALOGGERNONWHOLESALER IS 'Datalogger (Non-Wholesaler)~~~D3016 - Specifies the presence of a non-Wholesaler datalogger';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.METERLOCATIONCODE IS 'Meter Location Code~~~D3025 - Indicates Meter Location as either inside or outside (of a building)';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.METEROUTREADERGPSX IS 'Meter Outreader GISX~~~D3030 - Specifies the X coordinate of the location of the meter outreader, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.METEROUTREADERGPSY IS 'Meter Outreader GISY~~~D3031 - Specifies the Y coordinate of the location of the meter outreader, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.METEROUTREADERLOCCODE IS 'Meter Outreader Location Code~~~D3033 - Indicates Meter Outreader Location as either inside or outside (of a building)';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.COMBIMETERFLAG IS 'Combi Meter Flag~~~D3034 - Indicates if meter is part of a combi meter. Each part of a combi meter must have this flag set.';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.REMOTEREADFLAG IS 'Remote Read Flag~~~D3037 - Indicates if a meter has the capability to be read remotely, including via an Outreader';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.OUTREADERID IS 'Outreader ID~~~D3039 - Free text Data Item for the encoder reference, radio ID or logger number, or any other reference which will assist the Retailer in reading the meter';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.OUTREADERPROTOCOL IS 'Outreader Protocol~~~D3040 - Free text providing details of how the reading is accessed by the outreader. Will typically identify the manufacturers  protocol in use';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.REMOTEREADTYPE IS 'Remote Read Type~~~D3038 - Indicates the type of remote read capability for the meter';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.PHYSICALMETERSIZE IS 'Physical Meter Size~~~D3003 - Nominal size of the meter in mm e.g. for a DN15 meter the Physical Meter Size is 15';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.METERTREATMENT IS 'Meter Treatment~~~D3022 - Specifies whether the meter is a Wholesaler Water Meter or one of the various types of Private Meter';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DEVCHAR FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_DEVCHAR.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_DEVCHAR.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_DVLCRT
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_DVLCRT
  (
    LEGACYRECNUM             VARCHAR(30) CONSTRAINT CH01_DLC_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM       VARCHAR(30) CONSTRAINT CH01_DLC_PARENTLEGACYRECNUM NOT NULL,
    COBLEGACYRECNUM         VARCHAR(30) CONSTRAINT CH01_DLC_COBLEGACYRECNUM NOT NULL,
--    METERLOCSPECIALINSTR	  VARCHAR2(100),    --CR_013
--    METERLOCSPECIALLOC	    VARCHAR2(100),    --CR_013
    STWPROPERTYNUMBER_PK	 NUMBER(9,0),
    SAPFLOCNUMBER          NUMBER(30,0),
    SAPEQUIPMENT           NUMBER(10,0),
    STWMETERREF            NUMBER(15,0)
  );

ALTER TABLE SAP_DEL_DVLCRT ADD CONSTRAINT PK_SAP_DEL_DVLCRT PRIMARY KEY (LEGACYRECNUM);

--FOREIGN KEYS
ALTER TABLE SAP_DEL_DVLCRT ADD CONSTRAINT FK01_DLC_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);
ALTER TABLE SAP_DEL_DVLCRT ADD CONSTRAINT FK02_DLC_LEGACYRECNUM  FOREIGN KEY (COBLEGACYRECNUM)  REFERENCES SAP_DEL_COB (LEGACYRECNUM);

COMMENT ON TABLE SAP_DEL_DVLCRT IS 'Device (Meter) Location create table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DVLCRT
COMMENT ON COLUMN SAP_DEL_DVLCRT.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_DVLCRT.PARENTLEGACYRECNUM IS 'Foreign Key to Device Header';
COMMENT ON COLUMN SAP_DEL_DVLCRT.COBLEGACYRECNUM IS 'Foreign Key to Connection Object';
--COMMENT ON COLUMN SAP_DEL_DVLCRT.METERLOCSPECIALLOC IS 'D3019~~~Meter Location Free Descriptor~~~ DS_LOCATION part';              --CR_013
--COMMENT ON COLUMN SAP_DEL_DVLCRT.METERLOCSPECIALINSTR IS 'D3019~~~Meter Location Free Descriptor~~~ TXT_SPECIAL_INSTR part';      --CR_013

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DVLCRT FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_DVLCRT.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_DVLCRT.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_DVLCRT.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_DVLCRT.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_DVLUPDATE
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_DVLUPDATE
  (
    LEGACYRECNUM             VARCHAR(30) CONSTRAINT CH01_DLU_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM       VARCHAR(30) CONSTRAINT CH01_DLU_PARENTLEGACYRECNUM NOT NULL,
    --DEVICE LOCATION
    CITY                            VARCHAR2(255 byte),
    DISTRICT                        VARCHAR2(255 BYTE),
    POSTCODE                        VARCHAR2(8 BYTE)    CONSTRAINT CH01_DLU_POSTCODE NOT NULL ENABLE,
    STREET                          VARCHAR2(255 BYTE),  -- V0.15 CONSTRAINT CH01_DLU_STREET NOT NULL ENABLE,
    HOUSENUMBER                     VARCHAR2(255 byte),  -- V0.15 CONSTRAINT CH01_DLU_HOUSENUMBER NOT NULL ENABLE,    
    STREET2                         VARCHAR2(255 BYTE),
    STREET3                         VARCHAR2(100 byte),
    STREET4                         VARCHAR2(255 BYTE),
    PAFADDRESSKEY                   NUMBER(9,0),    
    LOCATIONFREETEXTDESCRIPTOR      VARCHAR2(255 BYTE),
    POBOX                           VARCHAR2(32 BYTE),
    STREET5                         VARCHAR2(255 BYTE),
    COUNTRY                         VARCHAR2(32 BYTE),
    UPRNREASONCODE                  VARCHAR2(2 BYTE),
    GPSX                            NUMBER(7,1),
    GPSY                            NUMBER(7,1),
    UPRN                            NUMBER(13,0),
    METERLOCATIONDESC	              VARCHAR2(100),
    METERLOCSPECIALINSTR	          VARCHAR2(100),    --CR_013
    METERLOCSPECIALLOC	            VARCHAR2(100),    --CR_013
    STWPROPERTYNUMBER_PK	 NUMBER(9,0),
    SAPFLOCNUMBER          NUMBER(30,0),
    SAPEQUIPMENT           NUMBER(10,0),
    STWMETERREF            NUMBER(15,0)
  );

ALTER TABLE SAP_DEL_DVLUPDATE ADD CONSTRAINT PK_SAP_DEL_DVLUPDATE PRIMARY KEY (LEGACYRECNUM);

--FOREIGN KEYS
ALTER TABLE SAP_DEL_DVLUPDATE ADD CONSTRAINT FK01_DLU_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);

COMMENT ON TABLE SAP_DEL_DVLUPDATE IS 'Device (Meter) Location Update table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DVLUPDATE
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.PARENTLEGACYRECNUM IS 'Foreign Key to Device Header';
--LOCATION
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.CITY IS 'Address Line 5~~~D5008 - Fifth line of address, or fifth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.DISTRICT IS 'Address Line 4~~~D5007 - Fourth line of address, or fourth line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.POSTCODE IS 'Postcode~~~D5009 - Postcode (without spaces)';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STREET IS 'STREET~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.HOUSENUMBER IS 'HOUSENUMBER~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STREET2 IS '~~~D5002 - BS7666 Secondary Addressable Object if available';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STREET3 IS '~~~D5003 - BS7666 Primary Addressable Object if available';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STREET4 IS 'Address Line 2~~~D5005 - Second line of address, or second line of address after Secondary Addressable Object and Primary Addressable Object if used';  
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.PAFADDRESSKEY IS 'PAF Address Key~~~D5011 - PAF Address Key if known';  
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.LOCATIONFREETEXTDESCRIPTOR IS 'Free Descriptor~~~D5001 - Free text descriptor for address/location  details';  
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.POBOX IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STREET5 IS 'Address Line 3~~~D5006 - Third line of address, or third line of address after Secondary Addressable Object and Primary Addressable Object if used';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.COUNTRY IS 'Country~~~D5010 - Country, if address is outside the UK';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.GPSX IS 'Meter GISX~~~D3017 - Specifies the X coordinate of the location of the meter, in OSGB all numeric eastings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.GPSY IS 'Meter GISY~~~D3018 - Specifies the Y coordinate of the location of the meter, in OSGB all numeric northings from south west corner of SV square. This must be submitted to a resolution of 0.1m, however the measurement does not need to be to an accuracy of 0.1m.';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.METERLOCATIONDESC IS 'D3019~~~Meter Location Free Descriptor~~~ Meter Location Desc part';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.METERLOCSPECIALLOC IS 'D3019~~~Meter Location Free Descriptor~~~ DS_LOCATION part';              --CR_013
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.METERLOCSPECIALINSTR IS 'D3019~~~Meter Location Free Descriptor~~~ TXT_SPECIAL_INSTR part';      --CR_013

--ADD COMMENTS OF COLUMNS IN SAP_DEL_DVLUPDATE FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';
--OTHER COMMENTS FOR NON_OUTPUT FIElDS
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.UPRNREASONCODE IS 'UPRN Reason Code~~~D2040 - Code to explain the absence or duplicate of a UPRN (in valid set)';
COMMENT ON COLUMN SAP_DEL_DVLUPDATE.UPRN IS 'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_METER_READ
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_METER_READ
  (
    LEGACYRECNUM                  VARCHAR(30) CONSTRAINT CH01_READ_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM            VARCHAR(30) CONSTRAINT CH01_READ_PARENTLEGACYRECNUM NOT NULL,
    METERREAD                     NUMBER(12) CONSTRAINT CH01_METERREAD NOT NULL,
    METERREADTYPE                 VARCHAR(2) ,
      METERREADERNUMBER             NUMBER(10),
    METERREADDATE                 DATE CONSTRAINT CH01_METERREADDATE NOT NULL,
      ACTUALREADDATE                 DATE,
    REREADFLAG	                  NUMBER(1), 
    ROLLOVERINDICATOR             NUMBER(1) CONSTRAINT CH01_ROLLOVERINDICATOR NOT NULL,
    ROLLOVERFLAG	                NUMBER(1),
    ESTIMATEDREADREASONCODE       VARCHAR(2) ,
    ESTIMATEDREADREMEDIALWORKIND  NUMBER(1) ,
    METERREADMETHOD               VARCHAR(9) CONSTRAINT CH01_METERREADMETHOD NOT NULL,
    STWPROPERTYNUMBER_PK	        NUMBER(9,0),
    SAPFLOCNUMBER                 NUMBER(30,0),
    SAPEQUIPMENT                  NUMBER(10,0),
    STWMETERREF                   NUMBER(15,0)
  );

ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT PK_SAP_DEL_METER_READ PRIMARY KEY (LEGACYRECNUM, METERREADDATE);

ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT FK02_DEV_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_METER_READ
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT CH01_ESTIMATEDREADREASONCODE CHECK ((ESTIMATEDREADREASONCODE IS NOT NULL AND METERREADMETHOD = 'ESTIMATED') OR (ESTIMATEDREADREASONCODE IS NULL AND METERREADMETHOD <> 'ESTIMATED'));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT CH01_ESTIMATEDREADREMEDIALWORK CHECK ((ESTIMATEDREADREMEDIALWORKIND IS NOT NULL AND METERREADMETHOD = 'ESTIMATED') OR (ESTIMATEDREADREMEDIALWORKIND IS NULL AND METERREADMETHOD <> 'ESTIMATED'));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_METERREADTYPE CHECK (METERREADTYPE IN ('I', 'P'));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_METERREADMETHOD CHECK (METERREADMETHOD IN ('VISUAL', 'CUSTOMER', 'REMOTE', 'ESTIMATED'));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_ROLLOVERINDICATOR CHECK (ROLLOVERINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_ROLLOVERFLAG CHECK (ROLLOVERFLAG IN (0,1));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_ESTIMATEDREADREASONCODE CHECK (ESTIMATEDREADREASONCODE IN ('DM', 'MI', 'NM', 'NA'));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_ESTIMATEDREADREMEDIALWORK CHECK (ESTIMATEDREADREMEDIALWORKIND IN (0,1));
ALTER TABLE SAP_DEL_METER_READ ADD CONSTRAINT RF01_REREADFLAG	CHECK	(REREADFLAG IN (0,1));


COMMENT ON TABLE SAP_DEL_METER_READ IS 'Meter Reading---Historic meter reads - up to 2 years of readings for each meter if available.';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_METER_READ
COMMENT ON COLUMN SAP_DEL_METER_READ.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_METER_READ.PARENTLEGACYRECNUM IS 'Foreign Key to Device Header';
--METER READ
COMMENT ON COLUMN SAP_DEL_METER_READ.METERREAD IS 'Meter Read~~~D3008 - Register advance read from a meter in m3';
COMMENT ON COLUMN SAP_DEL_METER_READ.METERREADTYPE IS 'Meter Read Type~~~D3010 - The type of meter reading';
COMMENT ON COLUMN SAP_DEL_METER_READ.METERREADERNUMBER IS 'NA - SAP Only - not requuired for MOSL';
COMMENT ON COLUMN SAP_DEL_METER_READ.METERREADDATE IS 'Meter Read Date~~~D3009 - Date of meter read';
COMMENT ON COLUMN SAP_DEL_METER_READ.REREADFLAG IS 'Re-Read Flag~~~D3012 - Identifies a meter read as a re-read';
COMMENT ON COLUMN SAP_DEL_METER_READ.ROLLOVERINDICATOR IS 'Rollover Indicator~~~D3020 - Proposes whether the meter read has rolled over or not as part of meter read submission by the Retailer or  Wholesaler';
COMMENT ON COLUMN SAP_DEL_METER_READ.ROLLOVERFLAG IS 'Rollover Flag~~~D3021 - Set by the Market Operator to indicate whether the Market Operator believes the meter read has rolled over or not';
COMMENT ON COLUMN SAP_DEL_METER_READ.ESTIMATEDREADREASONCODE IS 'Estimated Read Reason Code~~~D3028 - Identifies the reason for use of a Transfer Read with Meter Read Method of “Estimated”';
COMMENT ON COLUMN SAP_DEL_METER_READ.ESTIMATEDREADREMEDIALWORKIND IS 'Estimated Read Remedial Work Indicator~~~D3029 - Identifies whether remedial action has been obtained for a meter associated with a transfer, when a Transfer Read with Meter Read Method of “Estimated” is submitted';
COMMENT ON COLUMN SAP_DEL_METER_READ.METERREADMETHOD IS 'Meter Read Method~~~D3044 - The method of meter reading';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_METER_READ FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_METER_READ.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_METER_READ.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_METER_READ.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_METER_READ.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';
--OTHER COMMENTS FOR NON_OUTPUT FIElDS
COMMENT ON COLUMN SAP_DEL_METER_READ.ACTUALREADDATE IS 'Actual Read Date - N/A - SAP only placeholder field';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_SCM
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_SCM 
(
	 LEGACYRECNUM                   VARCHAR2(30)  CONSTRAINT CH01_COM_LEGACYRECNUM NOT NULL,
	 PARENTLEGACYRECNUM             VARCHAR2(30)  CONSTRAINT CH01_COM_PARENTLEGACYRECNUM  NOT NULL,
   SERVICECOMPONENTTYPE           VARCHAR2(4),
   EFFECTIVEFROMDATE              DATE,
   DPID_PK                        VARCHAR2(32),
   NO_IWCS	                      NUMBER(12),
   DPID_TYPE                      VARCHAR2(10), -- V0.14 
--   TECOMPONENTTYPE                VARCHAR2(4), -- V0.14 
   NO_SAMPLE_POINT	              NUMBER(8),
   CONSENT_NO	                    VARCHAR(7),
   VOLUME_LIMIT                   NUMBER(10,2), -- V0.11
--   CALD_TXT	                      VARCHAR2(250), -- V0.10  
   MAININST	                      VARCHAR2(13),
   SPID_PK                        VARCHAR2(13)  CONSTRAINT CH01_COM_SPID NOT NULL,
   SAPFLOCNUMBER	                NUMBER(12) 	  CONSTRAINT CH01_COM_SAPFLOCNUMBER NOT NULL,
   STWPROPERTYNUMBER_PK	          NUMBER(9)     CONSTRAINT CH01_COM_STWPROPERTYNUMBER NOT NULL
) ;

-- ADD PRIMARY KEY TO TABLE
ALTER TABLE SAP_DEL_SCM ADD CONSTRAINT PK_SAP_DEL_COM PRIMARY KEY (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS
ALTER TABLE SAP_DEL_SCM ADD CONSTRAINT RF01_COM_SERVICECOMPONENTTYPE CHECK (SERVICECOMPONENTTYPE IN ('MPW','MNPW','AW','UW','MS','AS','US','SW','HD','TE','WCA','SCA'));

CREATE INDEX IDX_STWPROP_SCM ON SAP_DEL_SCM(STWPROPERTYNUMBER_PK);
CREATE INDEX IDX_STWSPID_SCM ON SAP_DEL_SCM(SPID_PK);

--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_SCM  IS 'Service Component header table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_SCM
COMMENT ON COLUMN SAP_DEL_SCM.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_SCM.PARENTLEGACYRECNUM IS 'Parent table Primary Key';
COMMENT ON COLUMN SAP_DEL_SCM.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN SAP_DEL_SCM.SERVICECOMPONENTTYPE IS 'Service Component~~~D2043 - Service Component of a water or sewerage SPID';
COMMENT ON COLUMN SAP_DEL_SCM.SPID_PK IS '~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN SAP_DEL_SCM.SAPFLOCNUMBER IS 'SAP FLOC Number~~~STW015 - alternative internal key to enable joins';
COMMENT ON COLUMN SAP_DEL_SCM.STWPROPERTYNUMBER_PK IS 'Target no_property';
COMMENT ON COLUMN SAP_DEL_SCM.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler.';
COMMENT ON COLUMN SAP_DEL_SCM.NO_IWCS IS 'Target NO_IWCS';
--COMMENT ON COLUMN SAP_DEL_SCM.TECOMPONENTTYPE IS 'Service Component typre for TE'; -- V0.14 
COMMENT ON COLUMN SAP_DEL_SCM.DPID_TYPE IS 'DPID type - could be Consent,	Agreement, SVL or STDA';
COMMENT ON COLUMN SAP_DEL_SCM.NO_SAMPLE_POINT IS 'Target NO_SAMPLE_POINT';
COMMENT ON COLUMN SAP_DEL_SCM.CONSENT_NO IS 'Target CONSENT_NO';
COMMENT ON COLUMN SAP_DEL_SCM.MAININST IS 'TE Service Component key';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_SCMMO
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_SCMMO 
(
    LEGACYRECNUM                  VARCHAR2(30)  CONSTRAINT CH01_SCMMO_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM            VARCHAR2(30)  CONSTRAINT CH01_SCMMO_PARENTLEGACYRECNUM NOT NULL,
    EFFECTIVEFROMDATE             DATE          CONSTRAINT CH01_SCMMO_EFFECTIVEFROMDATE NOT NULL,
    SERVICECOMPONENTTYPE          VARCHAR2(4),
    SPECIALAGREEMENTFACTOR	      NUMBER(5,2),  
    SPECIALAGREEMENTFLAG	        NUMBER(1),
    HWAYSURFACEAREA	              VARCHAR2(32),
    UNMEASUREDTYPEACOUNT	        NUMBER(3),
    UNMEASUREDTYPEBCOUNT	        NUMBER(3),
    UNMEASUREDTYPECCOUNT	        NUMBER(3),
    UNMEASUREDTYPEDCOUNT	        NUMBER(3),
    UNMEASUREDTYPEECOUNT	        NUMBER(3),
    UNMEASUREDTYPEFCOUNT	        NUMBER(3),
    UNMEASUREDTYPEGCOUNT	        NUMBER(3),
    UNMEASUREDTYPEHCOUNT	        NUMBER(3),
    ADJUSTMENTSVOLADJTYPE	        VARCHAR2(16),
    ADJUSTMENTSVOLADJUNIQREF_PK	  VARCHAR2(16),
    ADJUSTMENTSVOLUME	            NUMBER(12),
    ASSESSEDDVOLUMETRICRATE	      NUMBER(12),
    UNMEASUREDTYPEADESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPEBDESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPECDESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPEDDESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPEEDESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPEFDESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPEGDESCRIPTION	  VARCHAR2(255),
    UNMEASUREDTYPEHDESCRIPTION	  VARCHAR2(255),
    ASSESSEDCHARGEMETERSIZE	      NUMBER(4),
    PIPESIZE	                    NUMBER(4),
    TARIFFCODE_PK	                VARCHAR2(32)    CONSTRAINT CH01_SCMMO_TARIFFCODE NOT NULL,
    SERVICECOMPONENTENABLED     	NUMBER(1),
    SRFCWATERAREADRAINED	        NUMBER(12),
    METEREDPWMAXDAILYDEMAND	      VARCHAR2(32),
    DAILYRESERVEDCAPACITY	        NUMBER(12),
    ASSESSEDTARIFBAND	            NUMBER(2),
    SRFCWATERCOMMUNITYCONFLAG	    NUMBER(1),
    SPECIALAGREEMENTREF	          VARCHAR2(122),
    SPID_PK                       VARCHAR2(13)  CONSTRAINT CH01_SCMMO_SPID_PK NOT NULL,
    SAPFLOCNUMBER	                NUMBER(12) 	  CONSTRAINT CH01_SCMMO_SAPFLOCNUMBER NOT NULL,
    STWPROPERTYNUMBER_PK	        NUMBER(9)     CONSTRAINT CH01_SCMMO_STWPROPERTYNUMBER NOT NULL
) ;

-- ADD PRIMARY KEY TO TABLE
ALTER TABLE SAP_DEL_SCMMO ADD CONSTRAINT PK_SAP_DEL_SCMMO PRIMARY KEY (LEGACYRECNUM);

--ADD FORIEGN KEYS TO TABLE
ALTER TABLE SAP_DEL_SCMMO ADD CONSTRAINT FK01_SCMMO_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_SCM (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS
ALTER TABLE SAP_DEL_SCMMO ADD CONSTRAINT RF01_COM_SERVICECOMPONENTEN CHECK (SERVICECOMPONENTENABLED IN (0,1));
ALTER TABLE SAP_DEL_SCMMO ADD CONSTRAINT RF01_COM_SPECIALAGREEMENTFLAG CHECK (SPECIALAGREEMENTFLAG IN (0,1));
ALTER TABLE SAP_DEL_SCMMO ADD CONSTRAINT RF01_COM_SRFCWATERCOMMUNITYCON CHECK (SRFCWATERCOMMUNITYCONFLAG IN (0,1));
ALTER TABLE SAP_DEL_SCMMO ADD CONSTRAINT RF02_TARIFFCODE_COMTYPE UNIQUE (TARIFFCODE_PK, SPID_PK, SERVICECOMPONENTTYPE);

--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_SCMMO IS 'Service Component(exc TE) MOSL data table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_SCMMO
COMMENT ON COLUMN SAP_DEL_SCMMO.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_SCMMO.PARENTLEGACYRECNUM IS 'Parent table Primary Key';
COMMENT ON COLUMN SAP_DEL_SCMMO.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN SAP_DEL_SCMMO.SERVICECOMPONENTTYPE IS 'Service Component~~~D2043 - Service Component of a water or sewerage SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.SERVICECOMPONENTENABLED IS 'Service Component Enabled~~~D2076 - Identifies if the Service Component is switched on or off';
COMMENT ON COLUMN SAP_DEL_SCMMO.SPECIALAGREEMENTFACTOR IS 'Special Agreement Factor~~~D2003 - Percentage factor applied to a Service Component or a DPID where a Special Arrangement exists. When set to zero it results in a zero charge, when set to 100% no adjustment is applied, and can be set to > 100%';
COMMENT ON COLUMN SAP_DEL_SCMMO.SPECIALAGREEMENTFLAG IS 'Special Agreement Flag~~~D2004 - Identifies the presence of a Special Agreement at a Service Component or a DPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.SPECIALAGREEMENTREF IS 'Special Agreement Reference~~~D2090 - Ofwat Reference for any S.142(2)(b) Special Agreement in place for the Service Component';
COMMENT ON COLUMN SAP_DEL_SCMMO.METEREDPWMAXDAILYDEMAND IS 'Maximum Daily Demand~~~D2079 - Maximum daily demand in m3 for a Metered Service Component, for maximum demand tariffs';
COMMENT ON COLUMN SAP_DEL_SCMMO.DAILYRESERVEDCAPACITY IS 'Daily Reserved Capacity~~~D2080 - Daily reserved capacity in m3 for a Metered Service Component';
COMMENT ON COLUMN SAP_DEL_SCMMO.HWAYSURFACEAREA IS 'Surface Area~~~D2012 - Indicates the Surface area in m2 of Eligible Premises for Highway Drainage calculations';
COMMENT ON COLUMN SAP_DEL_SCMMO.ASSESSEDDVOLUMETRICRATE IS 'Assessed Volumetric Rate~~~D2049 - The Volume (in m3 per year) to be used in charge calculations for Assessed Service Components';
COMMENT ON COLUMN SAP_DEL_SCMMO.ASSESSEDCHARGEMETERSIZE IS 'Assessed Chargeable Meter Size~~~D2068 - Meter size in mm for charge calculation purposes, for Assessed Service Components';
COMMENT ON COLUMN SAP_DEL_SCMMO.ASSESSEDTARIFBAND IS 'Tariff band~~~D2081 - Tariff band number to be applied for Service Components where banded tariffs are permitted';
COMMENT ON COLUMN SAP_DEL_SCMMO.SRFCWATERAREADRAINED IS 'Area drained~~~D2078 - Area drained at the Eligible Premises in m2, for the purposes of calculating Surface Water drainage charges';
COMMENT ON COLUMN SAP_DEL_SCMMO.SRFCWATERCOMMUNITYCONFLAG IS 'Community Concession Flag~~~D2085 - Boolean Flag indicating if Community Concession is to be applied to a Surface Water or Highway Drainage Service Component.';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEACOUNT IS 'Unmeasured Items Type A Count~~~D2018 - Indicates how many of Unmeasured Items Type A are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEBCOUNT IS 'Unmeasured Items Type B Count~~~D2019 - Indicates how many of Unmeasured Items Type B are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPECCOUNT IS 'Unmeasured Items Type C Count~~~D2020 - Indicates how many of Unmeasured Items Type C are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEDCOUNT IS 'Unmeasured Items Type D Count~~~D2021 - Indicates how many of Unmeasured Items Type D are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEECOUNT IS 'Unmeasured Items Type E Count~~~D2022 - Indicates how many of Unmeasured Items Type E are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEFCOUNT IS 'Unmeasured Items Type F Count~~~D2024 - Indicates how many of Unmeasured Items Type F are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEGCOUNT IS 'Unmeasured Items Type G Count~~~D2046 - Indicates how many of Unmeasured Items Type G are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEHCOUNT IS 'Unmeasured Items Type H Count~~~D2048 - Indicates how many of Unmeasured Items Type H are present at the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEADESCRIPTION IS 'Unmeasured Items Type A Description~~~D2058 - Free text description of the Unmeasured Items of Type A applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEBDESCRIPTION IS 'Unmeasured Items Type B Description~~~D2059 - Free text description of the Unmeasured Items of Type B applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPECDESCRIPTION IS 'Unmeasured Items Type C Description~~~D2060 - Free text description of the Unmeasured Items of Type C applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEDDESCRIPTION IS 'Unmeasured Items Type D Description~~~D2061 - Free text description of the Unmeasured Items of Type D applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEEDESCRIPTION IS 'Unmeasured Items Type E Description~~~D2062 - Free text description of the Unmeasured Items of Type E applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEFDESCRIPTION IS 'Unmeasured Items Type F Description~~~D2064 - Free text description of the Unmeasured Items of Type F applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEGDESCRIPTION IS 'Unmeasured Items Type G Description~~~D2065 - Free text description of the Unmeasured Items of Type F applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.UNMEASUREDTYPEHDESCRIPTION IS 'Unmeasured Items Type H Description~~~D2069 - Free text description of the Unmeasured Items of Type H applied to the SPID';
COMMENT ON COLUMN SAP_DEL_SCMMO.PIPESIZE IS 'PIPESIZE~~~D2071 - Pipe Size - value in mm';
COMMENT ON COLUMN SAP_DEL_SCMMO.SPID_PK IS '~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN SAP_DEL_SCMMO.ADJUSTMENTSVOLADJTYPE IS 'Volumetric Adjustment Type~~~D2044 - The type of volumetric adjustment to be applied to the Service Component';
COMMENT ON COLUMN SAP_DEL_SCMMO.ADJUSTMENTSVOLADJUNIQREF_PK IS 'Volumetric Adjustment UNIQUE Reference~~~D2045 - THE UNIQUE REFERENCE OF THE volumetric adjustment TO be applied TO THE Service Component';
COMMENT ON COLUMN SAP_DEL_SCMMO.ADJUSTMENTSVOLUME IS 'Adjustment Volume~~~D2047 - THE signed volume OF THE adjustment IN m3. This should be positive FOR cases WHERE THE meter IS UNDER-recording, AND negative TO give allowances FOR firefighting, bursts, etc.';
COMMENT ON COLUMN SAP_DEL_SCMMO.TARIFFCODE_PK IS 'Tariff Code~~~D7001 - A short code specified by the Wholesaler for the Tariff. Covers off 8 tariff related D Numbers for SAP - D2016, D2017, D2051, D2056, D2057, D2063, D2066 and D2067';
COMMENT ON COLUMN SAP_DEL_SCMMO.SAPFLOCNUMBER IS 'SAP FLOC Number~~~STW015 - alternative internal key to enable joins';
COMMENT ON COLUMN SAP_DEL_SCMMO.STWPROPERTYNUMBER_PK IS 'Target no_property';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_SCMTE
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_SCMTE 
(
 	 LEGACYRECNUM                   VARCHAR2(30)  CONSTRAINT CH01_SCMTE_LEGACYRECNUM NOT NULL,
	 PARENTLEGACYRECNUM            VARCHAR2(30)  CONSTRAINT CH01_SCMTE_PARENTLEGACYRECNUM NOT NULL,
   SERVICECOMPONENTTYPE           VARCHAR2(4),
   EFFECTIVEFROMDATE              DATE,
   DPID_PK                        VARCHAR2(32),
   NO_IWCS	                      NUMBER(12),
   DPID_TYPE                      VARCHAR2(10), -- V0.14 
--   TECOMPONENTTYPE                VARCHAR2(4)   CONSTRAINT CH01_SCMTE_TE NOT NULL, -- V0.14 
   NO_SAMPLE_POINT	              NUMBER(8),
   CONSENT_NO	                    VARCHAR(7),
   VOLUME_LIMIT                   NUMBER(10,2), -- V0.11
--   CALD_TXT	                      VARCHAR2(250), -- V0.10  
   MAININST	                      VARCHAR2(30),
   SPID_PK                        VARCHAR2(13)  CONSTRAINT CH01_SCMTE_SPID NOT NULL,
   SAPFLOCNUMBER	                NUMBER(12) 	  CONSTRAINT CH01_SCMTE_SAPFLOCNUMBER NOT NULL,
   STWPROPERTYNUMBER_PK	          NUMBER(9)     CONSTRAINT CH01_SCMTE_STWPROPERTYNUMBER NOT NULL
);

-- ADD PRIMARY KEY TO TABLE
ALTER TABLE SAP_DEL_SCMTE ADD CONSTRAINT PK_SAP_DEL_SCMTE PRIMARY KEY (LEGACYRECNUM);

--ADD FORIEGN KEYS TO TABLE
ALTER TABLE SAP_DEL_SCMTE ADD CONSTRAINT FK01_SCMTE_LEGACYRECNUM  FOREIGN KEY (MAININST)  REFERENCES SAP_DEL_SCM (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS
ALTER TABLE SAP_DEL_SCMTE ADD CONSTRAINT RF01_SCMTE_SERVICECOMPTYPE CHECK (SERVICECOMPONENTTYPE = ('TE'));
--ALTER TABLE SAP_DEL_SCMTE ADD CONSTRAINT RF01_SCMTE_TECOMPTYPE CHECK (TECOMPONENTTYPE = ('TE'));
ALTER TABLE SAP_DEL_SCMTE ADD CONSTRAINT RF01_SCMTE_DPIDTYPE CHECK (DPID_TYPE IN ('Consent','Agreement','SVL','STDA'));

--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_SCMTE IS 'TE Service Component(exc TE) header table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_SCMTE
COMMENT ON COLUMN SAP_DEL_SCMTE.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_SCMTE.PARENTLEGACYRECNUM IS 'Parent table SAPFLOCA';
COMMENT ON COLUMN SAP_DEL_SCMTE.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler.';
COMMENT ON COLUMN SAP_DEL_SCMTE.EFFECTIVEFROMDATE IS 'DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';
COMMENT ON COLUMN SAP_DEL_SCMTE.SPID_PK IS '~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN SAP_DEL_SCMTE.SERVICECOMPONENTTYPE IS 'Service Component~~~D2043 - Service Component of a water or sewerage SPID';
COMMENT ON COLUMN SAP_DEL_SCMTE.NO_IWCS IS 'Target NO_IWCS';
--COMMENT ON COLUMN SAP_DEL_SCMTE.TECOMPONENTTYPE IS 'Service Component typre for TE'; -- V0.14 
COMMENT ON COLUMN SAP_DEL_SCMTE.DPID_TYPE IS 'DPID type - could be Consent,	Agreement, SVL or STDA';
COMMENT ON COLUMN SAP_DEL_SCMTE.NO_SAMPLE_POINT IS 'Target NO_SAMPLE_POINT';
COMMENT ON COLUMN SAP_DEL_SCMTE.CONSENT_NO IS 'Target CONSENT_NO';
COMMENT ON COLUMN SAP_DEL_SCMTE.SAPFLOCNUMBER IS 'SAP FLOC Number~~~STW015 - alternative internal key to enable joins';
COMMENT ON COLUMN SAP_DEL_SCMTE.STWPROPERTYNUMBER_PK IS 'Target no_property';
COMMENT ON COLUMN SAP_DEL_SCMTE.MAININST IS 'TE Service Component key';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_SCMTEMO
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_SCMTEMO  
(
    LEGACYRECNUM                    VARCHAR2(30)  CONSTRAINT CH01_TEMO_LEGACYRECNUM NOT NULL,
    PARENTLEGACYRECNUM              VARCHAR2(30)  CONSTRAINT CH01_TEMO_PARENTLEGACYRECNUM NOT NULL,
    SCEFFECTIVEFROMDATE             DATE,
    NO_IWCS	                        NUMBER(12)    CONSTRAINT CH01_TEMO_NO_IWCS NOT NULL,    
    DPID_PK                         VARCHAR2(32),
    SPECIALAGREEMENTFACTOR  	      NUMBER(5,2),  -- V0.10
    SPECIALAGREEMENTFLAG	          NUMBER(1), -- V0.10
    TEYEARLYVOLESTIMATE             NUMBER(13), -- V0.18
    AMMONIANITROCAL	                NUMBER(12),
    CHARGEABLEDAILYVOL	            NUMBER(12),
    CHEMICALOXYGENDEMAND	          NUMBER(12),
    SUSPENDEDSOLIDSLOAD	            NUMBER(12),
    TREFODCHEMOXYGENDEMAND	        NUMBER(12),
    TREFODCHEMSUSPSOLDEMAND	        NUMBER(12),
    NOTIFIEDVOLUME	                NUMBER(12),
    DOMMESTICALLOWANCE	            NUMBER(12),
    SEASONALFACTOR	                NUMBER(12),
    TREFODCHEMAMONIANITROGENDEMAND	NUMBER(12),
    PERCENTAGEALLOWANCE	            NUMBER(5,2),
    FIXEDALLOWANCE	                VARCHAR2(12),
    RECEPTIONTREATMENTINDICATOR	    NUMBER(1),
    PRIMARYTREATMENTINDICATOR	      NUMBER(1),
    MARINETREATMENTINDICATOR	      NUMBER(1),
    BIOLOGICALTREATMENTINDICATOR	  NUMBER(1),
    SLUDGETREATMENTINDICATOR	      NUMBER(1),
    AMMONIATREATMENTINDICATOR	      NUMBER(1),
    TARRIFCODE	                    VARCHAR2(32)  CONSTRAINT CH01_TEMO_TARIFFCODE NOT NULL,
    DISCHARGETYPE	                  VARCHAR2(12),
    CALCDISCHARGEID_PK	            VARCHAR2(15),
    TARRIFBAND	                    NUMBER(2),
    SUBMISSIONFREQ	                VARCHAR2(1),
    TREFODCHEMCOMPXDEMAND	          NUMBER(9),
    TREFODCHEMCOMPYDEMAND	          NUMBER(9),
    TREFODCHEMCOMPZDEMAND	          NUMBER(9),
    TEFXTREATMENTINDICATOR	        NUMBER(1),
    TEFYTREATMENTINDICATOR	        NUMBER(1),
    TEFZTREATMENTINDICATOR	        NUMBER(1),
    TEFAVAILABILITYDATAX	          NUMBER(9),
    TEFAVAILABILITYDATAY	          NUMBER(9),
    TEFAVAILABILITYDATAZ	          NUMBER(9),
    SEWERAGEVOLUMEADJMENTHOD	      VARCHAR(8),
    CALD_TXT	                      VARCHAR2(250),    --D4003 -- V0.10  
    SPID_PK                         VARCHAR2(13),
    SAPFLOCNUMBER	                  NUMBER(12),
    STWPROPERTYNUMBER_PK	          NUMBER(9)
);

-- ADD PRIMARY KEY TO TABLE
--ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT PK_SAP_DEL_SCMTEMO PRIMARY KEY (LEGACYRECNUM);


--ADD FORIEGN KEYS TO TABLE
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT FK01_TEMO_LEGACYRECNUM  FOREIGN KEY (PARENTLEGACYRECNUM)  REFERENCES SAP_DEL_SCMTE (LEGACYRECNUM);

--ADD DEFAULT VALUE CONSTRAINTS
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_RECEPTIONTREATIND CHECK (RECEPTIONTREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_PRIMARYTREATMENTIND CHECK (PRIMARYTREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_MARINETREATMENTIND CHECK (MARINETREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_BIOLOGICALTREATIND CHECK (BIOLOGICALTREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_AMMONIATREATMENTIND CHECK (AMMONIATREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_TEFXTREATMENTIND CHECK (TEFXTREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_TEFYTREATMENTIND CHECK (TEFYTREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_TEFZTREATMENTIND CHECK (TEFZTREATMENTINDICATOR IN (0,1));
ALTER TABLE SAP_DEL_SCMTEMO ADD CONSTRAINT RF01_TEMO_SEWERAGEVOLUMEADJM CHECK (SEWERAGEVOLUMEADJMENTHOD IN ('NONE', 'DA', 'SUBTRACT'));


--ADD COMMENTS ON TABLE
COMMENT ON TABLE SAP_DEL_SCMTEMO IS 'TE Service Component MOSL data table';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_SCMTEMO
COMMENT ON COLUMN SAP_DEL_SCMTEMO.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.PARENTLEGACYRECNUM IS 'Parent table Primary Key';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.NO_IWCS IS 'Target NO_IWCS';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.NOTIFIEDVOLUME IS 'Notified Volume~~~D6008 - Volume in m3 notified as having been discharged for a Calculated Discharge';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.DISCHARGETYPE IS 'Discharge Type~~~D6022 -  Discharge type.';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.CALCDISCHARGEID_PK IS 'Calculated Discharge ID~~~D6023 - Unique reference for the discharge. STW - FK implementing relationship to Calculated Discharge';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SUBMISSIONFREQ IS 'Submission Frequency~~~D6025 - The frequency that the Retailer must submit Calculated Discharges';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.DPID_PK IS 'DPID~~~D6001 - The unique identifier per Wholesaler allocated to each Discharge Point by the Wholesaler.';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TARRIFCODE IS 'Tariff Code~~~D7001 - A short code specified by the Wholesaler for the Tariff. The Tariff Code should be meaningful. This code is unique to the Wholesaler for the Service Component and can never be changed. STW004 - FK implementing relationship to Tariff and (if used) the Tariff Band. Covers off D6020';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.CHARGEABLEDAILYVOL IS 'Chargeable Daily Volume~~~D6003 - Trade Effluent Availability Data: chargeable daily volume in m3/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.AMMONIANITROCAL IS 'cANâ??~~~D6002 - Trade Effluent Availability Data: chargeable ammoniacal nitrogen load in kg/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.CHEMICALOXYGENDEMAND IS 'cCODâ??~~~D6004 - Trade Effluent Availability Data: chargeable chemical oxygen demand load (or other parameter as may be determined by the Wholesaler) in kg/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SUSPENDEDSOLIDSLOAD IS 'cSSâ??~~~D6005 - Trade Effluent Availability Data: chargeable suspended solids load (or other parameter as may be determined by the Wholesaler) in kg/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.DOMMESTICALLOWANCE IS 'Domestic Allowance~~~D6009 - The annual Volume in m3 of Water Services in relation to water meters associated with a Discharge Point that is being used for domestic purposes and is discharged as Trade Effluent Services in relation to the Discharge Point. If the Sewerage Volume Adjustment Method is set to â??DAâ??, then the Domestic Allowance must be specified. If the Sewerage Volume Adjustment Method is set to either â??NONEâ?? or â??SUBTRACTâ??, then the Domestic Allowance must not be specified. For the avoidance of doubt, if a Domestic Allowance of zero (0) is specified, this will result in a zero volume being applied to the Foul Sewerage calculation in respect of the applicable meters';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SEASONALFACTOR IS 'Seasonal Factor~~~D6010 - Trade Effluent Availability Data: premium to the Trade Effluent Charges in accordance with the Wholesale Tariff Document';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.PERCENTAGEALLOWANCE IS 'Percentage Allowance~~~D6012 - The part of the Volume of Water Services of all meters associated with the Discharge Point which is expressed as a percentage and is not discharged to the Sewerage Wholesalerâ??s sewer, for example due to evaporation or because it is used in production. The Percentage Allowance is applied after the Fixed Allowance. The Percentage Allowance must be supplied';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.FIXEDALLOWANCE IS 'Fixed Allowance~~~D6013 - The part of the Volume of Water Services of all meters associated with the Discharge Point which is expressed as an annual Volume in m3 and is not discharged to the Sewerage Wholesalerâ??s sewer; for example due to evaporation, because it is used in production or because it is Surface Water recorded by a Private Trade Effluent Meter in respect of which Surface Water Drainage Service charges are already applied. The Fixed Allowance is applied before the Percentage Allowance. The Fixed Allowance must be supplied';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.RECEPTIONTREATMENTINDICATOR IS 'Reception Treatment Indicator~~~D6014 - Flag to indicate whether Reception Charges apply to Trade Effluent from the Discharge Point Variable name: RTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.PRIMARYTREATMENTINDICATOR IS 'Primary Treatment Indicator~~~D6015 - Flag to indicate Primary/Volumetric Charges apply to Trade Effluent from the Discharge Point Variable name: PTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.MARINETREATMENTINDICATOR IS 'Marine Treatment Indicator~~~D6016 - Flag to indicate whether Outfall (marine) Charges apply to Trade Effluent from the Discharge Point Variable name: MTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.BIOLOGICALTREATMENTINDICATOR IS 'Biological Treatment Indicator~~~D6017 - Flag to indicate whether Secondary (Biological) Charges apply to Trade Effluent from the Discharge Point Variable name: BTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SLUDGETREATMENTINDICATOR IS 'Sludge Treatment Indicator~~~D6018 - Flag to indicate whether Sludge Treatment Charges apply to Trade Effluent from the Discharge Point Variable name: STI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.AMMONIATREATMENTINDICATOR IS 'Ammonia Treatment Indicator~~~D6019 - Flag to indicate whether ammonia charges apply to Trade Effluent from the Discharge Point Variable name: ATI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEFXTREATMENTINDICATOR IS 'Trade Effluent Component X Treatment Indicator~~~D6029 - Flag to indicate whether Trade Effluent Component X applies at the Discharge Point. Variable name: XTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEFYTREATMENTINDICATOR IS 'Trade Effluent Component Y Treatment Indicator~~~D6030 - Flag to indicate whether Trade Effluent Component Y applies at the Discharge Point. Variable name: YTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEFZTREATMENTINDICATOR IS 'Trade Effluent Component Z Treatment Indicator~~~D6031 - Flag to indicate whether Trade Effluent Component Z applies at the Discharge Point Variable name: ZTI';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEFAVAILABILITYDATAX IS 'cXâ??~~~D6032 - Trade Effluent Availability Data: Trade Effluent Component X Demand load in kg/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEFAVAILABILITYDATAY IS 'cYâ??~~~D6033 - Trade Effluent Availability Data: Trade Effluent Component Y Demand load in kg/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEFAVAILABILITYDATAZ IS 'cZâ??~~~D6034 - Trade Effluent Availability Data: Trade Effluent Component Z Demand load in kg/day';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TARRIFBAND IS 'Tariff Band~~~D2081 - Tariff band number to be applied for Service Components where banded tariffs are permitted. Covers off D6024';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SEWERAGEVOLUMEADJMENTHOD IS 'Sewerage Volume Adjustment Method~~~D6035 - The method by which Sewerage volumes are adjusted, if required, due to the DPID';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TREFODCHEMOXYGENDEMAND IS 'Ot~~~D6006 - Trade Effluent Operating Data: chemical oxygen demand (or other parameter as may be determined by the Wholesaler) in mg/l';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TREFODCHEMSUSPSOLDEMAND IS 'St~~~D6007 - Trade Effluent Operating Data: suspended solids (or other parameter as may be determined by the Wholesaler) in mg/l';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TREFODCHEMAMONIANITROGENDEMAND IS 'At~~~D6011 - Trade Effluent Operating Data: ammoniacal nitrogen content of the Trade Effluent in mg/l';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TREFODCHEMCOMPXDEMAND IS 'Xt~~~D6026 - Trade Effluent Operating Data: Trade Effluent Component X content in mg/l';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TREFODCHEMCOMPYDEMAND IS 'Yt~~~D6027 - Trade Effluent Operating Data: Trade Effluent Component Y content in mg/l';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TREFODCHEMCOMPZDEMAND IS 'Zt~~~D6028 - Trade Effluent Operating Data: Trade Effluent Component Z content in mg/l';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.CALD_TXT IS 'Calculated Discharge Free Descriptor ~~~D4003';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SPID_PK IS '~~~D2001 - Unique identifier allocated to each Supply Point by the Market Operator';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SAPFLOCNUMBER IS 'SAP FLOC Number~~~STW015 - alternative internal key to enable joins';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.STWPROPERTYNUMBER_PK IS 'Target no_property';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SPECIALAGREEMENTFACTOR IS 'Special Agreement Factor~~~D2003 - Percentage factor applied to a Service Component or a DPID where a Special Arrangement exists. When set to zero it results in a zero charge, when set to 100% no adjustment is applied, and can be set to > 100%';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.SPECIALAGREEMENTFLAG IS 'Special Agreement Flag~~~D2004 - Identifies the presence of a Special Agreement at a Service Component or a DPID';
COMMENT ON COLUMN SAP_DEL_SCMTEMO.TEYEARLYVOLESTIMATE IS 'TE Yearly Volume Estimate~~~D2010 - An estimate of the annual Volume supplied in relation to a meter or Discharge Point on the basis of the relevant Eligible Premises being Occupied Premises. Value in m3/a';
--------------------------------------------------------------------------------------------------------
-- SAP_DEL_METER_INSTALL
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_METER_INSTALL
  (
    LEGACYRECNUM                  VARCHAR(30) CONSTRAINT CH01_MIC_LEGACYRECNUM NOT NULL,
    --DI_INT
    DVLLEGACYRECNUM               VARCHAR(30),
    SCMLEGACYRECNUM               VARCHAR(30) CONSTRAINT CH01_MIC_SCMLEGACYRECNUM NOT NULL,
    INITIALMETERREADDATE          DATE,
    INSTALLTYPE                   VARCHAR(2),
    --DI_ZW
    METERREAD                     VARCHAR(36),
--    YEARLYVOLESTIMATE             NUMBER(13),  
    NUMBEROFDIGITS                NUMBER CONSTRAINT CH01_MTR_NUMBEROFDIGITS NOT NULL,
    MEASUREUNITATMETER            VARCHAR2(12) CONSTRAINT CH01_MTR_MEASUREUNITATMETER NOT NULL,
    DEVLEGACYRECNUM               VARCHAR(30) CONSTRAINT CH01_MIC_DEVLEGACYRECNUM NOT NULL,
    --DI_GER    
    PERCENTAGEDISCHARGE	          NUMBER(5,2),
    ACTIVITYREASON                VARCHAR(2),
    EFFECTIVEFROMDATE	            DATE,
    --OTHER KEYS
    STWPROPERTYNUMBER_PK	        NUMBER(9,0),
    SAPFLOCNUMBER                 NUMBER(30,0),
    SPID                          VARCHAR(13) CONSTRAINT CH01_MIC_SPID NOT NULL,
    SAPEQUIPMENT                  NUMBER(10,0),
    STWMETERREF                   NUMBER(15,0)
  );

ALTER TABLE SAP_DEL_METER_INSTALL ADD CONSTRAINT PK_SAP_DEL_METER_INSTALL PRIMARY KEY (LEGACYRECNUM);

ALTER TABLE SAP_DEL_METER_INSTALL ADD CONSTRAINT FK03_DEV_LEGACYRECNUM  FOREIGN KEY (DEVLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);


--ALTER TABLE SAP_DEL_METER_INSTALL ADD CONSTRAINT RF01_MEASUREUNITATMETER CHECK (MEASUREUNITATMETER IN ('METRICm3','METRICNONm3'));

COMMENT ON TABLE SAP_DEL_METER_INSTALL IS 'Meter Install - Physical and Logical device (METER) installation associations.';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_METER_INSTALL
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.DVLLEGACYRECNUM IS 'Foreign Key to Device Location Create Header';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.SCMLEGACYRECNUM IS 'Foreign Key to Service Component Installation Header';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.DEVLEGACYRECNUM IS 'Foreign Key to Device Header';

COMMENT ON COLUMN SAP_DEL_METER_INSTALL.INITIALMETERREADDATE IS '~~~D3042 - Initial Meter Read Date';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.INSTALLTYPE IS '01 = Technical (FULL) Installation 04 = Billing Installation';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.METERREAD IS '~~~D3008 - Initial Meter Reading';
--COMMENT ON COLUMN SAP_DEL_METER_INSTALL.YEARLYVOLESTIMATE IS 'Yearly Volume Estimate~~~D2010 - An estimate of the annual Volume supplied in relation to a meter or Discharge Point on the basis of the relevant Eligible Premises being Occupied Premises. Value in m3/a';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.NUMBEROFDIGITS IS 'Number of Digits~~~D3004 - The number of digits required to provide a reading in m3. For the avoidance of doubt, this is irrespective of the actual number of dials or digits on the meter, as meters may record volumes to a higher or lower resolution than 1m3. However, this Data Item is required with reference to m3 for the purposes of rollover detection. This will also be the number of digits required for the maximum volume in m3 that can be recorded by the meter';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.MEASUREUNITATMETER IS 'Measurement Units at Meter~~~D3036 - Indicates the measurement units of the meter itself';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.PERCENTAGEDISCHARGE IS 'MDVol~~~D3024 - For a meter Discharge Point association, the percentage of the volume associated with a meter which is discharged to the Discharge Point';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.ACTIVITYREASON IS '~~~D3045 or ~~~D3046 - Reason for the addition or removal of a meter';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.EFFECTIVEFROMDATE IS 'Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_METER_INSTALL FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.SPID IS 'Supply Point ID used to build key';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_METER_INSTALL.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_REG
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_REG
  (
    LEGACYRECNUM                  VARCHAR(30) CONSTRAINT CH01_REG_LEGACYRECNUM NOT NULL,
    --REGREL
    TIMESLICEDATE                 DATE,
    DEVLEGACYRECNUM               VARCHAR(30) CONSTRAINT CH01_REG_DEVLEGACYRECNUM NOT NULL,
    OPERATIONCODE                 NUMBER(2),
    STWPROPERTYNUMBER_PK	        NUMBER(9,0),
    SAPFLOCNUMBER                 NUMBER(30,0),
    SAPEQUIPMENT                  NUMBER(10,0),
    STWMETERREF                   NUMBER(15,0)
  );

--ALTER TABLE SAP_DEL_REG ADD CONSTRAINT PK_SAP_DEL_REG PRIMARY KEY (LEGACYRECNUM, DEVLEGACYRECNUM, OPERATIONCODE);

ALTER TABLE SAP_DEL_REG ADD CONSTRAINT FK04_DEV_LEGACYRECNUM  FOREIGN KEY (DEVLEGACYRECNUM)  REFERENCES SAP_DEL_DEV (LEGACYRECNUM);

COMMENT ON TABLE SAP_DEL_REG IS 'Device Relationship Register Table.';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_REG
COMMENT ON COLUMN SAP_DEL_REG.LEGACYRECNUM IS 'Primary Key';
COMMENT ON COLUMN SAP_DEL_REG.DEVLEGACYRECNUM IS 'Foreign Key to Device Header';

COMMENT ON COLUMN SAP_DEL_REG.TIMESLICEDATE IS 'Date from which time slice is valid';
COMMENT ON COLUMN SAP_DEL_REG.OPERATIONCODE IS 'Operation code: Role of register in relationship';

--ADD COMMENTS OF COLUMNS IN SAP_DEL_REG FOR KEY FIELDS
COMMENT ON COLUMN SAP_DEL_REG.STWPROPERTYNUMBER_PK IS 'TARGET NO_PROPERTY';
COMMENT ON COLUMN SAP_DEL_REG.SAPFLOCNUMBER IS 'SAP FLOCA NUMBER used to build key';
COMMENT ON COLUMN SAP_DEL_REG.SAPEQUIPMENT IS 'SAP EQUIPMENT NUMBER used to build key for existing devices';
COMMENT ON COLUMN SAP_DEL_REG.STWMETERREF IS 'TARGET METERREF used to build key for NEW devices';

--------------------------------------------------------------------------------------------------------
-- SAP_DEL_BP
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAP_DEL_BP
(
    LEGACYRECNUM             VARCHAR2(30) CONSTRAINT CH01_VP_LEGACYRECNUM NOT NULL,
    SAPFLOCNUMBER            VARCHAR2(10),
    CUSTOMERNUMBER_PK        NUMBER(9,0),
    STWPROPERTYNUMBER_PK     NUMBER(9,0) CONSTRAINT CH01_BP_STWPROPERTYNUMBER NOT NULL ENABLE,
--    CUSTOMERCLASSIFICATION   VARCHAR2(5 BYTE) CONSTRAINT CH01_CUSTOMERCLASSIFICATION NOT NULL,  -- V0.13 
    SENSITIVE_REASON         VARCHAR2(200 BYTE), -- V0.20 
    SAP_IDENTIFICATION_TYPE  VARCHAR2(20 BYTE), -- V0.20 
    CUSTOMERCLASSIFICATION   VARCHAR2(50 BYTE) CONSTRAINT CH01_CUSTOMERCLASSIFICATION NOT NULL, -- V0.13 
    CUSTOMERNAME             VARCHAR2(255 BYTE) CONSTRAINT CH01_CUSTOMERNAME NOT NULL ENABLE,
    CUSTOMERBANNERNAME       VARCHAR2(255 BYTE),
    STDINDUSTRYCLASSCODE     VARCHAR2(12 BYTE),
    STDINDUSTRYCLASSCODETYPE VARCHAR2(4 BYTE)
);

-- ADD PRIMARY AND UNIQUE KEYS TO TABLES
--ALTER TABLE SAP_DEL_BP ADD CONSTRAINT SAP_DEL_BP_PK PRIMARY KEY (LEGACYRECNUM); -- V0.13 

--ADD DEFAULT VALUE CONSTRAINTS TO FIELDS in SAP_DEL_POD
ALTER TABLE SAP_DEL_BP ADD CONSTRAINT RF01_CUSTOMERCLASSIFICATION CHECK (CUSTOMERCLASSIFICATION     IN ('SEMDV', 'NA'));
ALTER TABLE SAP_DEL_BP ADD CONSTRAINT RF02_CUSTOMERCLASSIFICATION CHECK ((CUSTOMERCLASSIFICATION = 'NA' AND SENSITIVE_REASON IS NULL) OR (CUSTOMERCLASSIFICATION = 'SEMDV' AND SENSITIVE_REASON IS NOT NULL));
ALTER TABLE SAP_DEL_BP ADD CONSTRAINT RF01_STDINDUSTRYCLASSCODETYPE CHECK (STDINDUSTRYCLASSCODETYPE IN ('1980', '1992', '2003', '2007'));

COMMENT ON COLUMN SAP_DEL_BP.CUSTOMERNUMBER_PK IS 'Customer Number~~~STW017 - Legal Entity Number Unique identifier for the CUSTOMER';
COMMENT ON COLUMN SAP_DEL_BP.CUSTOMERCLASSIFICATION IS 'Customer Classification~~~D2005 - Customer classification for a Supply Point, for identification of where a customer is defined as vulnerable for the purposes of the Security and Emergency Measures (Water and Sewerage Undertakers) Directions ';
COMMENT ON COLUMN SAP_DEL_BP.CUSTOMERNAME IS 'Customer Name~~~D2027 - The customer name associated with a given Supply Point';
COMMENT ON COLUMN SAP_DEL_BP.CUSTOMERBANNERNAME IS 'Customer Banner Name~~~D2050 - The Trading Name of the Customer at a given Eligible Premises, if known';
COMMENT ON COLUMN SAP_DEL_BP.STDINDUSTRYCLASSCODE IS 'Standard Industrial Classification Code~~~D2008 - Standard Industrial Classification Code applicable to a Supply Point';
COMMENT ON COLUMN SAP_DEL_BP.STDINDUSTRYCLASSCODETYPE IS 'Standard Industrial Classification Code Type~~~D2092 - Identifies the version of the Standard Industrial Classification Code provided';
COMMENT ON TABLE SAP_DEL_BP IS 'BUSINESS PARTNER / CUSTOMER TABLE.';

--------------------------------------------------------------------------------------------------------
-- SAP_PROPERTY_CUSTOMER_V
--------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SAP_PROPERTY_CUSTOMER_V AS
SELECT MEP.STWPROPERTYNUMBER_PK,
       WSPID.CUSTOMERNUMBER_PK WATER_CUSTOMERNUMBER,
       SSPID.CUSTOMERNUMBER_PK SEWERAGE_CUSTOMERNUMBER,
       DECODE(NVL(WSPID.CUSTOMERNUMBER_PK,0),0,'S','W') SERVICECATEGORY
FROM SAPTRAN.MO_ELIGIBLE_PREMISES MEP
LEFT JOIN SAPTRAN.MO_SUPPLY_POINT WSPID ON (MEP.STWPROPERTYNUMBER_PK = WSPID.STWPROPERTYNUMBER_PK AND WSPID.SERVICECATEGORY = 'W')
LEFT JOIN SAPTRAN.MO_SUPPLY_POINT SSPID ON (MEP.STWPROPERTYNUMBER_PK = SSPID.STWPROPERTYNUMBER_PK AND SSPID.SERVICECATEGORY = 'S');

--------------------------------------------------------------------------------------------------------
-- LU_SAP_SENSITIVITY_CODES
--------------------------------------------------------------------------------------------------------
CREATE TABLE SAPDEL.LU_SAP_SENSITIVITY_CODES
  (
    SENSITIVE_REASON        VARCHAR2(50 BYTE),
    SAP_IDENTIFICATION_TYPE VARCHAR2(10 BYTE)
  );

exit;
   
   


