CREATE TABLE BT_OWC_CUST_SWITCHED_SUPPLIER
  (
    CD_COMPANY_SYSTEM CHAR(4 BYTE),
    NO_ACCOUNT        NUMBER(9,0),
    NO_PROPERTY       NUMBER(9,0),
    CD_SERVICE_PROV   VARCHAR2(5 BYTE),
    NO_COMBINE_054    NUMBER(9,0),
    NO_SERV_PROV      NUMBER(3,0),
    ST_SERV_PROV      VARCHAR2(1 BYTE),
    DT_START DATE,
    DT_END DATE,
    NM_LOCAL_SERVICE  VARCHAR2(25 BYTE),
    CD_PROPERTY_USE   CHAR(1 BYTE),
    NO_COMBINE_024    NUMBER(9,0),
    TP_CUST_ACCT_ROLE CHAR(1 BYTE),
    NO_LEGAL_ENTITY   NUMBER(9,0),
    NC024_DT_START DATE,
    NC024_DT_END DATE,
    IND_LEGAL_ENTITY     CHAR(1 BYTE),
    FG_TOO_HARD          CHAR(1 BYTE),
    CD_PROPERTY_USE_ORIG VARCHAR2(1 BYTE),
    CD_PROPERTY_USE_CURR VARCHAR2(1 BYTE),
    CD_PROPERTY_USE_FUT  VARCHAR2(1 BYTE),
    UDPRN                NUMBER,
    UPRN                 NUMBER,
    VOA_REFERENCE        VARCHAR2(60 BYTE),
    SAP_FLOC             NUMBER(30,0),
    CORESPID             VARCHAR2(10 BYTE),
    AGG_NET              VARCHAR2(1 BYTE),
    FG_CONSOLIDATED      CHAR(1 BYTE),
    FG_TE                CHAR(1 BYTE),
    FG_MECOMS_RDY        CHAR(1 BYTE),
    NO_PROPERTY_MASTER   NUMBER,
    FG_NMM               CHAR(1 BYTE),
    FG_MO_RDY            CHAR(1),
    FG_MO_LOADED         CHAR(1),
    TS_MO_LOADED	       DATE,
    FG_SAP_RDY           CHAR(1),
    FG_SAP_LOADED        CHAR(1),
    TS_SAP_LOADED	       DATE      
  );
  
CREATE INDEX IND_BT_OWC_1 ON  BT_OWC_CUST_SWITCHED_SUPPLIER (CD_SERVICE_PROV);
CREATE INDEX IND_BT_OWC_2 ON  BT_OWC_CUST_SWITCHED_SUPPLIER (NO_PROPERTY);

 commit;
 /
 exit;
