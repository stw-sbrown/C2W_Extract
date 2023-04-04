--
-- Subversion $Revision: 5228 $
--
--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      25/05/2016  O.Badmus   Initial version
-- V 0.02      24/08/2016  K.Burton   Added address type flags
------------------------------------------------------------------------------------------
CREATE TABLE BT_INSTALL_ADDRESS
  (
    STWPROPERTYNUMBER_PK NUMBER(9,0),
    METER_ADDR_MKR       VARCHAR2(1 BYTE),
    PROPERTY_ADDR_MKR    VARCHAR2(1 BYTE),
    CUST_ADDR_MKR        VARCHAR2(1 BYTE)
  );
CREATE INDEX INST_PROP_NUMBER ON BT_INSTALL_ADDRESS (STWPROPERTYNUMBER_PK);
  

commit;
EXIT;
