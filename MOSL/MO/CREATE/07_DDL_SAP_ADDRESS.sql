--Associated Address tables for SAP
-- O.Badmus
--02/06/2016
CREATE TABLE SAP_ADDRESS
  (
    ADDRESS_PK                 NUMBER(9,0),
    UPRN                       NUMBER(13,0),
    UPRNREASONCODE             VARCHAR2(2 BYTE),
    PAFADDRESSKEY              NUMBER(9,0),
    STREET2                    VARCHAR2(255 BYTE),
    STREET3                    varchar2(100 byte),
    HOUSENUMBER                varchar2(255 byte),
    STREET                     VARCHAR2(255 BYTE),
    STREET4                    VARCHAR2(255 BYTE),
    STREET5                    VARCHAR2(255 BYTE),
    DISTRICT                   VARCHAR2(255 BYTE),
    CITY                       varchar2(255 byte),
    POSTCODE                   VARCHAR2(8 BYTE) CONSTRAINT CH02_SA_POSTCODE NOT NULL ENABLE,
    COUNTRY                    VARCHAR2(32 BYTE),
    POBOX                      VARCHAR2(32 BYTE),
    LOCATIONFREETEXTDESCRIPTOR VARCHAR2(255 BYTE),
    CUSTOMERNUMBER_PK          NUMBER(9,0),
    STWPROPERTYNUMBER          number(9,0)
  );
   
CREATE TABLE SAP_CUST_ADDRESS
(
ADDRESSPROPERTY_PK NUMBER(9)  ,
ADDRESS_PK NUMBER(9) ,
CUSTOMERNUMBER_PK NUMBER(9) CONSTRAINT CH01_SCA_CUSTOMERNUMBER NOT NULL,
ADDRESSUSAGEPROPERTY VARCHAR(10) ,
EFFECTIVEFROMDATE DATE ,
EFFECTIVETODATE DATE,
STWPROPERTYNUMBER_PK NUMBER(9) 
);

CREATE TABLE SAP_PROPERTY_ADDRESS
(
ADDRESSPROPERTY_PK NUMBER(9)  ,
ADDRESS_PK NUMBER(9) ,
STWPROPERTYNUMBER_PK NUMBER(9) CONSTRAINT CH04_SPA_STWPROPERTYNUMBER NOT NULL,
ADDRESSUSAGEPROPERTY VARCHAR(10) ,
EFFECTIVEFROMDATE DATE ,
EFFECTIVETODATE DATE 
);

CREATE TABLE SAP_METER_ADDRESS
(
ADDRESSPROPERTY_PK NUMBER(9)  ,
METERSERIALNUMBER_PK VARCHAR(32) CONSTRAINT CH01_SMA_METERSERIALNUMBER NOT NULL,
ADDRESS_PK NUMBER(9) ,
ADDRESSUSAGEPROPERTY VARCHAR(10) ,
EFFECTIVEFROMDATE DATE ,
EFFECTIVETODATE DATE 
);

--SAP_ADDRESS
--ALTER TABLE SAP_ADDRESS ADD FOREIGN_ADDRESS VARCHAR2 (10); --ADDED ON 27/07/2016
ALTER TABLE SAP_ADDRESS ADD CONSTRAINT PK_04_SA_ADDRESS PRIMARY KEY (ADDRESS_PK);
ALTER TABLE SAP_ADDRESS ADD CONSTRAINT RF02_SA_UPRNREASONCODE CHECK (UPRNREASONCODE IN ('ME','SR','MT','IP','PL','BW','SP','OT'));

--SAP_CUST_ADDRESS--
ALTER TABLE SAP_CUST_ADDRESS ADD CONSTRAINT PK_01_SCA_ADDRESSPROPERTY PRIMARY KEY (ADDRESSPROPERTY_PK);
ALTER TABLE SAP_CUST_ADDRESS ADD CONSTRAINT FK_SCA_ADDRESS_PK01 FOREIGN KEY ("ADDRESS_PK") REFERENCES "SAP_ADDRESS"("ADDRESS_PK");


--SAP_METER_ADDRESS--
alter table SAP_METER_ADDRESS add MANUFCODE varchar2(32);
ALTER TABLE SAP_meter_address ADD installedpropertynumber NUMBER(9,0);
alter table SAP_METER_ADDRESS add MANUFACTURER_PK varchar2(32);
ALTER TABLE SAP_METER_ADDRESS ADD CONSTRAINT PK_03_SMA_ADDRESSPROPERTY PRIMARY KEY (ADDRESSPROPERTY_PK);
alter table SAP_METER_ADDRESS add constraint FK_SMA_METER_ADDRESS_MAN_COMP foreign key (MANUFACTURER_PK, METERSERIALNUMBER_PK) references MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
ALTER TABLE SAP_METER_ADDRESS ADD CONSTRAINT FK_SMA_ADDRESS_PK03 FOREIGN KEY ("ADDRESS_PK") REFERENCES "SAP_ADDRESS"("ADDRESS_PK");

--SAP_PROPERTY_ADDRESS
ALTER TABLE SAP_PROPERTY_ADDRESS ADD CONSTRAINT PK_02_SPA_ADDRESSPROPERTY PRIMARY KEY (ADDRESSPROPERTY_PK);
ALTER TABLE SAP_PROPERTY_ADDRESS ADD CONSTRAINT FK_SPA_ADDRESS_PK02 FOREIGN KEY ("ADDRESS_PK") REFERENCES "SAP_ADDRESS"("ADDRESS_PK");
ALTER TABLE SAP_PROPERTY_ADDRESS ADD CONSTRAINT FK_SPA_STWPROPERTYNUMBER_PK03 FOREIGN KEY ("STWPROPERTYNUMBER_PK") REFERENCES "MO_ELIGIBLE_PREMISES"("STWPROPERTYNUMBER_PK");

COMMENT ON COLUMN SAP_ADDRESS.UPRN
IS
  'UPRN~~~D2039 - Unique Property Reference Number (UPRN) as published in the NLPG';
  COMMENT ON COLUMN SAP_ADDRESS.PAFADDRESSKEY
IS
  'PAF Address Key~~~D5011 - PAF Address Key if known';
  COMMENT ON COLUMN SAP_ADDRESS.STWPROPERTYNUMBER
IS
  '~~~STW020 - Property number';
  COMMENT ON COLUMN SAP_ADDRESS.CUSTOMERNUMBER_PK
is
  '~~~Legal Entity';
  COMMENT ON COLUMN SAP_ADDRESS.UPRNREASONCODE
IS
  'UPRN Reason Code~~~D2040 - Code to explain the absence or duplicate of a UPRN (in valid set)';
  COMMENT ON COLUMN SAP_ADDRESS.STREET2
IS
  '~~~D5002 - BS7666 Secondary Addressable Object if available';
  COMMENT ON COLUMN SAP_ADDRESS.STREET3
IS
  '~~~D5003 - BS7666 Primary Addressable Object if available';
  COMMENT ON COLUMN SAP_ADDRESS.HOUSENUMBER
is
  'HOUSENUMBER~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
  COMMENT ON COLUMN SAP_ADDRESS.STREET
is
'STREET~~~D5004 - First line of address, or first line of address after Secondary Addressable Object and Primary Addressable Object if used';
  COMMENT ON COLUMN SAP_ADDRESS.STREET4
IS
  'Address Line 2~~~D5005 - Second line of address, or second line of address after Secondary Addressable Object and Primary Addressable Object if used';
  COMMENT ON COLUMN SAP_ADDRESS.STREET5
IS
  'Address Line 3~~~D5006 - Third line of address, or third line of address after Secondary Addressable Object and Primary Addressable Object if used';
  COMMENT ON COLUMN SAP_ADDRESS.DISTRICT
IS
  'Address Line 4~~~D5007 - Fourth line of address, or fourth line of address after Secondary Addressable Object and Primary Addressable Object if used';
  COMMENT ON COLUMN SAP_ADDRESS.CITY
IS
  'Address Line 5~~~D5008 - Fifth line of address, or fifth line of address after Secondary Addressable Object and Primary Addressable Object if used';
  COMMENT ON COLUMN SAP_ADDRESS.POSTCODE
IS
  'Postcode~~~D5009 - Postcode (without spaces)';
  COMMENT ON COLUMN SAP_ADDRESS.COUNTRY
IS
  'Country~~~D5010 - Country, if address is outside the UK';
  COMMENT ON COLUMN SAP_ADDRESS.LOCATIONFREETEXTDESCRIPTOR
is
  'Free Descriptor~~~D5001 - Free text descriptor for address/location  details';

COMMENT ON COLUMN SAP_CUST_ADDRESS.ADDRESSPROPERTY_PK IS '~~~';
COMMENT ON COLUMN SAP_CUST_ADDRESS.ADDRESS_PK IS '~~~STW014-Internal key to enable joins - SAP_ADDRESS.ADDRESS_PK';
COMMENT ON COLUMN SAP_CUST_ADDRESS.CUSTOMERNUMBER_PK IS '~~~STW014-Internal key to enable joins - MO_CUSTOMER.CUSTOMERNUMBER_PK';

COMMENT ON COLUMN SAP_PROPERTY_ADDRESS.ADDRESS_PK IS '~~~STW014-Internal key to enable joins - MO_ADDRESS.ADDRESS_PK';
COMMENT ON COLUMN SAP_PROPERTY_ADDRESS.STWPROPERTYNUMBER_PK IS '~~~STW014-Internal key to enable joins - MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK';

COMMENT ON COLUMN "SAPTRAN"."SAP_METER_ADDRESS"."METERSERIALNUMBER_PK"
IS
  '~~~STW014-Internal key to enable joins - MO_METER.MANUFATURERSERIALNUM_PK';
  COMMENT ON COLUMN "SAPTRAN"."SAP_METER_ADDRESS"."ADDRESS_PK"
is
  '~~~STW014-Internal key to enable joins - MO_ADDRESS.ADDRESS_pk';
commit;
exit;