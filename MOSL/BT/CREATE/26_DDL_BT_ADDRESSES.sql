--
-- Subversion $Revision: 5284 $
--
--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      23/08/2016  K.Burton   Initial version
-- V 0.02      24/08/2016  K.Burton   Added NO_PO_BOX for SAP
------------------------------------------------------------------------------------------
CREATE TABLE BT_ADDRESSES
  (
    NO_PROPERTY       NUMBER(9,0) NOT NULL ENABLE,
    CD_ADDRESS        NUMBER(9,0) NOT NULL ENABLE,
    UPRN              NUMBER,
    UPRN_REASON_CODE  CHAR(2 BYTE),
    UDPRN             NUMBER,
    CD_ADDR_TYPE      CHAR(3 BYTE) NOT NULL ENABLE,
    NM_ADDR_OBJ_3     VARCHAR2(60 BYTE),
    NM_ADDR_OBJ_2     VARCHAR2(60 BYTE),
    NM_ADDR_OBJ_1     VARCHAR2(60 BYTE),
    NM_ORG_1          VARCHAR2(60 BYTE),
    NM_ORG_2          VARCHAR2(60 BYTE),
    F_CD_ADDRESS      NUMBER(9,0),
    TXT_FRGN_LOC_1    VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_2    VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_3    VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_4    VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_5    VARCHAR2(60 BYTE),
    NO_BLDG           VARCHAR2(10 BYTE),
    NM_STREET_1       VARCHAR2(100 BYTE),
    NM_STREET_TYPE_1  VARCHAR2(20 BYTE),
    NM_STREET_2       VARCHAR2(100 BYTE),
    NM_STREET_TYPE_2  VARCHAR2(20 BYTE),
    NM_DEP_LOC_2      VARCHAR2(35 BYTE),
    NM_DEP_LOC_1      VARCHAR2(35 BYTE),
    NM_TOWN           VARCHAR2(30 BYTE),
    ADDRESS_DET_PC    VARCHAR2(4000 BYTE),
    CD_GEOG_AREA_175  CHAR(5 BYTE),
    NM_COUNTRY        VARCHAR2(50 BYTE),
    NO_PO_BOX         VARCHAR2(32 BYTE),
    FOREIGN_ADDRESS   VARCHAR2(1 BYTE),
    PROPERTY_ADDR_MKR VARCHAR2(1 BYTE),
    METER_ADDR_MKR    VARCHAR2(1 BYTE),
    CUST_ADDR_MKR     VARCHAR2(1 BYTE)
  );

CREATE INDEX IDX_PROPERTY ON BT_ADDRESSES (NO_PROPERTY);
CREATE INDEX IDX_FOREIGN ON BT_ADDRESSES (FOREIGN_ADDRESS);
CREATE INDEX IDX_TYPE ON BT_ADDRESSES (CD_ADDR_TYPE);
CREATE INDEX IDX_ADDRESS ON BT_ADDRESSES (CD_ADDRESS);

 commit;
 /
 EXIT;
