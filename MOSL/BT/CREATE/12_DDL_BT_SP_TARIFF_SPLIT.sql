--Create table BT_SP_TARIFF_SPLIT
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4023 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--DROP TABLE BT_SP_TARIFF_SPLIT PURGE;


  CREATE TABLE BT_SP_TARIFF_SPLIT 
   (CD_SERVICE_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_TARIFF CHAR(10 BYTE) NOT NULL ENABLE, 
	CD_BILL_ALG_ITEM CHAR(5 BYTE), 
	CD_REF_TAB CHAR(5 BYTE), 
	CD_SPLIT_TARIFF VARCHAR2(16 BYTE), 
	DS_SPLIT_TARIFF VARCHAR2(4000 BYTE)
   );

  COMMENT ON TABLE BT_SP_TARIFF_SPLIT  IS 'BT_SP_TARIFF_SPLIT';

  CREATE INDEX BT_SP_TARIFF_SPLIT_IDX1 ON BT_SP_TARIFF_SPLIT (CD_TARIFF);
  CREATE INDEX BT_SP_TARIFF_SPLIT_IDX2 ON BT_SP_TARIFF_SPLIT (CD_TARIFF, CD_BILL_ALG_ITEM, CD_REF_TAB);

  commit;
  exit;
