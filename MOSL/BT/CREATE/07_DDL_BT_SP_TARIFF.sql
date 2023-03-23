--Create table BT_SP_TARIFF 
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4023 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--DROP TABLE BT_SP_TARIFF;

  CREATE TABLE BT_SP_TARIFF 
   (CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_SERVICE_ABBREV CHAR(8 BYTE), 
	DS_SERVICE VARCHAR2(20 BYTE) NOT NULL ENABLE, 
	CD_TARIFF CHAR(10 BYTE) NOT NULL ENABLE, 
	DS_TARIFF VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	CD_REV_CLASS_150 CHAR(5 BYTE) NOT NULL ENABLE, 
	DS_REV_CLASS_150 VARCHAR2(60 BYTE) NOT NULL ENABLE
   ) ;

   COMMENT ON TABLE MOUTRAN.BT_SP_TARIFF  IS 'Active Target Tariffs by Service Provision';

   commit;
   exit;
