--Create table BT_SP_TARIFF_ALG
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4441 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      15/06/2016  S.Badhan   removed schema name from comment.
-----------------------------------------------------------------------------------------

--DROP TABLE BT_SP_TARIFF_ALG;


  CREATE TABLE BT_SP_TARIFF_ALG 
   (CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_TARIFF CHAR(10 BYTE) NOT NULL ENABLE, 
	CD_ALGORITHM CHAR(5 BYTE) NOT NULL ENABLE, 
	DS_ALGORITHM VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	 CONSTRAINT BT_SP_TARIFF_ALG_PK PRIMARY KEY (CD_SERV_PROV, CD_TARIFF, CD_ALGORITHM)
  );
  

   COMMENT ON TABLE BT_SP_TARIFF_ALG  IS 'Algorithms used by Target Tariff';
   
   commit;
   exit;
