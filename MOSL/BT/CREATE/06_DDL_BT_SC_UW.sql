--Create table BT_SC_UW 
-- Subversion $Revision: 4454 $
--N.Henderson - 12/04/2016
--Attempt to drop the table first. Will generate warnign if does not
--exist but can be ignored.

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      15/06/2016  S.Badhan   removed schema name from comment.
-----------------------------------------------------------------------------------------

--DROP TABLE BT_SC_UW;

  CREATE TABLE BT_SC_UW 
   (NO_PROPERTY NUMBER(9,0) NOT NULL ENABLE, 
	NO_SERV_PROV NUMBER(3,0) NOT NULL ENABLE, 
	NO_COMBINE_054 NUMBER(9,0) NOT NULL ENABLE, 
	CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	D2018_TYPEACOUNT NUMBER(3,0), 
	D2019_TYPEBCOUNT NUMBER(3,0), 
	D2020_TYPECCOUNT NUMBER(3,0), 
	D2021_TYPEDCOUNT NUMBER(3,0), 
	D2022_TYPEECOUNT NUMBER(3,0), 
	D2024_TYPEFCOUNT NUMBER(3,0), 
	D2046_TYPEGCOUNT NUMBER(3,0), 
	D2048_TYPEHCOUNT NUMBER(3,0), 
	D2058_TYPEADESCRIPTION VARCHAR2(255 BYTE), 
	D2059_TYPEBDESCRIPTION VARCHAR2(255 BYTE), 
	D2060_TYPECDESCRIPTION VARCHAR2(255 BYTE), 
	D2061_TYPEDDESCRIPTION VARCHAR2(255 BYTE), 
	D2062_TYPEEDESCRIPTION VARCHAR2(255 BYTE), 
	D2064_TYPEFDESCRIPTION VARCHAR2(255 BYTE), 
	D2065_TYPEGDESCRIPTION VARCHAR2(255 BYTE), 
	D2069_TYPEHDESCRIPTION VARCHAR2(255 BYTE), 
	D2067_TARIFFCODE VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	NO_TARIFF_GROUP NUMBER(3,0) NOT NULL ENABLE, 
	NO_TARIFF_SET NUMBER(3,0) NOT NULL ENABLE, 
	CONSTRAINT BT_SC_UW_PK PRIMARY KEY (NO_COMBINE_054, NO_TARIFF_GROUP, NO_TARIFF_SET)
 );

   COMMENT ON TABLE BT_SC_UW  IS 'BT_SC_UW';

   commit;
   exit;


