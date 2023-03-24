--
-- Subversion $Revision: 4673 $
--
--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      29/06/2016  S.Badhan   I-260. Intial version
-----------------------------------------------------------------------------------------

  CREATE TABLE BT_SC_AS 
 (NO_PROPERTY           NUMBER(9,0) NOT NULL ENABLE, 
	NO_SERV_PROV          NUMBER(3,0) NOT NULL ENABLE, 
	NO_COMBINE_054        NUMBER(9,0) NOT NULL ENABLE, 
	CD_SERV_PROV          CHAR(5 BYTE) NOT NULL ENABLE, 
	CD_TARIFF             VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	NO_TARIFF_GROUP       NUMBER(3,0) NOT NULL ENABLE, 
	NO_TARIFF_SET         NUMBER(3,0) NOT NULL ENABLE,
  NO_VALUE              NUMBER,
	CONSTRAINT BT_SC_AS_PK PRIMARY KEY (NO_COMBINE_054, CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET)
 );

  COMMENT ON TABLE BT_SC_AS  IS 'Working table for Assesssed Water Service Components';

   commit;
   exit;


