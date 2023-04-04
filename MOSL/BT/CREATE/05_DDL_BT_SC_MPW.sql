--Create table BT_SC_MPW
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4441 $
--Attempt to drop the table first. Will generate warnign if does not
--exist but can be ignored.

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      15/06/2016  S.Badhan   removed schema name from comment.
-----------------------------------------------------------------------------------------



--DROP TABLE BT_SC_MPW;

  CREATE TABLE BT_SC_MPW 
   (NO_PROPERTY NUMBER(9,0) NOT NULL ENABLE, 
	NO_SERV_PROV NUMBER(3,0) NOT NULL ENABLE, 
	NO_COMBINE_054 NUMBER(9,0) NOT NULL ENABLE, 
	CD_SERV_PROV CHAR(5 BYTE) NOT NULL ENABLE, 
	D2079_MAXDAILYDMD NUMBER(12,0) NOT NULL ENABLE, 
	D2080_DLYRESVDCAP NUMBER(12,0) NOT NULL ENABLE, 
	D2056_TARIFFCODE VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	CONSTRAINT BT_SC_MPW_PK PRIMARY KEY (NO_COMBINE_054)
	);

   COMMENT ON TABLE BT_SC_MPW  IS 'BT_SC_MPW';

   commit;
   exit;
   
