--Create table BT_SP_TARIFF_ALGITEM
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4023 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--DROP TABLE BT_SP_TARIFF_ALGITEM;


  CREATE TABLE BT_SP_TARIFF_ALGITEM 
   (	CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_TARIFF CHAR(10 BYTE) NOT NULL ENABLE, 
	CD_ALGORITHM CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_BILL_ALG_ITEM CHAR(5 BYTE) NOT NULL ENABLE, 
	DS_BILL_ALG_ITEM VARCHAR2(40 BYTE) NOT NULL ENABLE
   ) ;

   COMMENT ON TABLE BT_SP_TARIFF_ALGITEM  IS 'Algorithm Items used by Tariff, Algorithm';

   commit;
   exit;


