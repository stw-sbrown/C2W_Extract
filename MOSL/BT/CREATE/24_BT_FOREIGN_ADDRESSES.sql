--
-- Subversion $Revision: 5212 $
--
--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      19/07/2016  O.Badmus   
-- V 0.02      26/07/2016  D.Cheung   Add exit
-- V 0.03      23/08/2016  K.Burton   New index on NO_PROPERTY
------------------------------------------------------------------------------------------

CREATE TABLE BT_FOREIGN_ADDRESSES
  (
    CD_ADDRESS      NUMBER(9,0),
    NO_LEGAL_ENTITY NUMBER(9,0),
    NO_ACCOUNT      NUMBER(9,0),
    NO_PROPERTY     NUMBER(9,0),
    NO_COMBINE_024  NUMBER(9,0),
    VAL_1           VARCHAR2(10 BYTE),
    VAL_2           VARCHAR2(8 BYTE),
    NM_TOWN         VARCHAR2(30 BYTE),
    VAL_3           VARCHAR2(121 BYTE),
    NM_DEP_LOC_1    VARCHAR2(35 BYTE),
    NO_BLDG         CHAR(10 BYTE),
    NM_ADDR_OBJ_1   VARCHAR2(60 BYTE),
    NO_PO_BOX       CHAR(14 BYTE),
    AD_LINE_1       VARCHAR2(60 BYTE),
    AD_LINE_2       VARCHAR2(60 BYTE),
    AD_LINE_3       VARCHAR2(60 BYTE),
    AD_LINE_4       VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_1  VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_2  VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_3  VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_4  VARCHAR2(60 BYTE),
    TXT_FRGN_LOC_5  varchar2(60 byte),
    SOURCE_TABLE    CHAR(3 byte),
    NM_COUNTRY      VARCHAR2(50 BYTE)
  );
 CREATE INDEX IND_BT_FORE_NO_ACC ON  BT_FOREIGN_ADDRESSES (NO_ACCOUNT);
 CREATE INDEX IND_BT_FORE_CD ON  BT_FOREIGN_ADDRESSES (CD_ADDRESS);
 CREATE INDEX IND_BT_FORE_PROP ON  BT_FOREIGN_ADDRESSES (NO_PROPERTY);
 
 commit;
 /
 exit;