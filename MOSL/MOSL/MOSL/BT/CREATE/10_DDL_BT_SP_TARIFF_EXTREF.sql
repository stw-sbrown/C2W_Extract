--Create table BT_SP_TARIFF_EXTREF
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4023 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--DROP TABLE BT_SP_TARIFF_EXTREF;

  CREATE TABLE BT_SP_TARIFF_EXTREF 
   (CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_TARIFF CHAR(10 BYTE) NOT NULL ENABLE, 
	CD_ALGORITHM CHAR(5 BYTE) NOT NULL ENABLE, 
	TP_ENTITY_332 CHAR(5 BYTE) NOT NULL ENABLE, 
	NO_EXT_REFERENCE NUMBER(3,0) NOT NULL ENABLE, 
	DS_EXT_REFERENCE VARCHAR2(30 BYTE), 
	 CONSTRAINT BT_SP_TARIFF_EXTREF_PK PRIMARY KEY (CD_SERV_PROV, CD_TARIFF, CD_ALGORITHM, TP_ENTITY_332, NO_EXT_REFERENCE)
  ) ;

   COMMENT ON TABLE BT_SP_TARIFF_EXTREF  IS 'External References used by Tariff/Alg';

   commit;
   exit;


