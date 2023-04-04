--Create table BT_SPR_TARIFF 
--N.Henderson - 12/04/2016
-- Subversion $Revision: 5681 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.02      29/09/2016  S.Badhan   Create index on NO_COMBINE_054 and CD_COMPANY_SYSTEM
-- V 0.01      15/07/2016  S.Badhan   I-301. Change BT_SPR_TARIFF_IDX1 to be just on NO_COMBINE_054
-----------------------------------------------------------------------------------------

--DROP TABLE BT_SPR_TARIFF PURGE;


  CREATE TABLE BT_SPR_TARIFF 
   (CD_COMPANY_SYSTEM CHAR(4 BYTE) NOT NULL ENABLE, 
	NO_PROPERTY NUMBER(9,0) NOT NULL ENABLE, 
	NO_SERV_PROV NUMBER(3,0) NOT NULL ENABLE, 
	NO_ACCOUNT NUMBER(9,0), 
	NO_COMBINE_054 NUMBER(9,0) NOT NULL ENABLE, 
	CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_TARIFF CHAR(10 BYTE) NOT NULL ENABLE, 
	NO_TARIFF_GROUP NUMBER(3,0) NOT NULL ENABLE, 
	NO_TARIFF_SET NUMBER(3,0) NOT NULL ENABLE, 
	DT_START DATE NOT NULL ENABLE, 
	DT_END DATE
   ) ;

  COMMENT ON TABLE BT_SPR_TARIFF  IS 'List of Tariffs for Property';

  CREATE INDEX BT_SPR_TARIFF_IDX1 ON BT_SPR_TARIFF (NO_COMBINE_054)  ;
  CREATE INDEX BT_SPR_TARIFF_IDX2 ON BT_SPR_TARIFF (NO_PROPERTY, NO_SERV_PROV, NO_ACCOUNT)  ;
  CREATE INDEX BT_SPR_TARIFF_IDX3 ON BT_SPR_TARIFF (CD_COMPANY_SYSTEM, NO_PROPERTY)  ;
  CREATE INDEX BT_SPR_TARIFF_IDX4 ON BT_SPR_TARIFF (NO_COMBINE_054, DT_END, CD_TARIFF)  ;
  CREATE INDEX BT_SPR_TARIFF_IDX5 ON BT_SPR_TARIFF (NO_COMBINE_054, CD_COMPANY_SYSTEM)  ;

  commit;
  exit;
