CREATE OR REPLACE
PROCEDURE P_MOU_TRAN_ADDRESS(
    no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
    no_job      IN MIG_JOBREF.NO_JOB%TYPE,
    return_code IN OUT NUMBER )
IS
  ----------------------------------------------------------------------------------------
  -- PROCEDURE SPECIFICATION: Address Transform MO Extract
  --
  -- AUTHOR        : Sreedhar Pallati
  --
  -- FILENAME      : P_MOU_TRAN_ADDRESS .sql
  --
  -- Subversion $Revision: 6127 $
  --
  -- CREATED       : 04/03/2016
  --
  -- DESCRIPTION   : Procedure to create the Address MO Extract
  --                 Will read from key gen and target tables, apply any transformationn
  --                 rules and write to normalised tables.
  -- NOTES :
  -- This package must be run each time the transform batch is run.
  ---------------------------- Modification History ---------------------------------------
  --
  -- Version   Date        Author     Description
  -- -------   ----------  --------   -----------------------------------------------------
  -- V 0.01    04/03/2016  S.pallati  Initial Draft
  -- V 0.02    13/04/2016  O.BADMUS   Issue no I-112 fixed
  -- V 0.03    18/04/2016  Pallati    Check with MO_meter is added IF meter exist in parent table
  -- V 1.00    19/04/2016  S.Badhan   Amend selection sql to use created MO tables (CR-02)
  --                                  and tidyied up code. Also issue I-150 formatting
  --                                  of PRIMARYADDRESSABLEOBJECT.
  -- v 1.01    26/04/2016  O.Badmus   Revisited Issue no I-112 and/or Defect ID : 20
  --                                  and also patched up MO_METER_ADDRESS to ensure all the meters
  --                                  in mo_meter are present in MO_METER_ADDRESS table.
  --                                  Also patched up MO_CUST_ADDRESS to identify a specific address for a customer
  -- v 1.02    28/04/2016  O.Badmus   Tidied up code to write out text (no_property and cd_address), discontinue a record from
  --                                  processing in the next table when an error is trapped from MO_ADDRESS and wrote an
  --                                  if statement that calls the postcode function to do the postcode validations
  -- v 1.03    29/04/2016  O.Badmus   Took out CD_ADDR_PAF from the SQL query and used UDPRN for PAFADDRESSKEY as per F+V change
  -- v 1.06    05/05/2016  O.Badmus   Implemented additional transformation for ADDRESSLINE01 After Concatenation and writing out warnings
  --                                  and dropping invalid PRIMARYADDRESSABLEOBJECT
  -- v 1.07    12/05/2016  O.Badmus   Implemented changes in F+V DOC to addressline01,PRIMARYADDRESSABLEOBJECT and updated
  --                                  cur_prop_addr cursor to pick up more cd addresses
  -- v 1.08    18/05/2016  K.Burton   Added ORDER BY clause to main cursor query to resolve primary key
  --                                  constraint errors on MO_ADDRESS and include non-market meter addresses
  -- v 1.09    19/05/2016  O.Badmus   Added parallel processing to the main cursor and then updated ADDRESSLINE05
  -- v 1.10    23/05/2016  D.Cheung   Added NO_PROPERTY and MANUFCODE fields to METER_ADDRESS.
  -- v 1.11    25/05/2016  O.Badmus   Amended the first query in the main driving cursor.
  -- v 1.12    01/06/2016  D.Cheung   Amend METER cursor to include details for TE
  -- v 1.13    24/06/2016  L.Smith    Performance changes
  -- v 1.14    27/06/2016  L.Smith    I-162 New reconciliation measures for CP28
  -- v 1.15    19/07/2016  O.Badmus   CR_032 - Foreign Addresses and a column FOREIGN_ADDRESS to indentify them
  -- v 2.00    19/08/2016  K.Burton   Major re-write of queries and procs to get around FOREIGN_ADDRESS issues     
  -- V 2.01    24/08/2016  K.Burton   I-348 change to counting for properties for reconciliation measures
  -- V 2.02    24/08/2016  K.Burton   Change to BT_ADDRESSES selection query to use modified BT_INSTALL_ADDRESS table
  -- V 2.03    26/08/2016  S.Badhan   TVMNHHDTL field FG_MECOMS_RDY renamed to PHASE
  -- V 2.04    21/10/2016  S.Badhan   Enable parallel processing.  
  -- V 2.05    21/10/2016  S.Badhan   Performance changes.
  -- V 2.05    07/11/2016  S.Badhan   Performance changes.
  -----------------------------------------------------------------------------------------
  c_module_name   CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_ADDRESS';
  c_company_cd    CONSTANT VARCHAR2(4)  := 'STW1';
  l_error_number  VARCHAR2(255);
  l_error_message VARCHAR2(512);
  l_progress      VARCHAR2(100);
  l_prev_prop BT_TVP054.NO_PROPERTY%TYPE;
  l_job MIG_JOBSTATUS%ROWTYPE;
  l_err MIG_ERRORLOG%ROWTYPE;
  l_no_row_read_prop MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_cd_adr MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_meter MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_cust MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_prop MIG_CPLOG.RECON_MEASURE_TOTAL%type;
  l_no_row_insert_cd_adr MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_cust MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_meter MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_prop MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_cd_adr MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_cust MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_meter MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written BOOLEAN;
  l_adr MO_ADDRESS%ROWTYPE;
  l_prev_cd_address CIS.TADADDRFAST.CD_ADDRESS%TYPE;
  L_NO_UTL_EQUIP          VARCHAR2(40);
  L_REC_EXC               BOOLEAN;
  L_REC_WAR               BOOLEAN;
  L_POSTCODE              VARCHAR2(8);
  l_orig_ins_prop_add_del NUMBER(9);
  l_orig_ins_met_add_del  NUMBER(9);
  
  CURSOR CUR_PROP_ADDR (P_NO_PROPERTY_START BT_TVP054.NO_PROPERTY%TYPE, P_NO_PROPERTY_END BT_TVP054.NO_PROPERTY%TYPE) IS
    SELECT /*+ PARALLEL(bt,60) */
           NO_PROPERTY,
           CD_ADDRESS ADDRESS_PK,
           UPRN,
           UDPRN PAFADDRESSKEY,
           NULL PROPERTYNUMBERPROPERTY,
           NULL CUSTOMERNUMBERPROPERTY,
           UPRN_REASON_CODE UPRNREASONCODE,
           CASE WHEN NM_ADDR_OBJ_2 IS NULL THEN
             NM_ADDR_OBJ_3 || ' ' || NM_ADDR_OBJ_2
           ELSE
            CASE WHEN NM_ADDR_OBJ_3 IS NOT NULL THEN
              NM_ADDR_OBJ_3
            ELSE
              NULL
            END
           END SECONDADDRESABLEOBJECT,
           CASE WHEN NM_ADDR_OBJ_1 IN ('-','#') THEN
             NULL
           ELSE
             NM_ADDR_OBJ_1
           END PRIMARYADDRESSABLEOBJECT,
           NO_BLDG || ' ' || NM_STREET_1 || ' ' || NM_STREET_TYPE_1 ADDRESSLINE01,
           NM_STREET_2 || ' ' || NM_STREET_TYPE_2 ADDRESSLINE02,
           NM_DEP_LOC_2 ADDRESSLINE03,
           NM_DEP_LOC_1 ADDRESSLINE04,
           NM_TOWN ADDRESSLINE05,
           ADDRESS_DET_PC POSTCODE,
           NULL COUNTRY,
           NULL LOCATIONFREETEXTDESCRIPTOR,
           'N' FOREIGN_ADDRESS,
           PROPERTY_ADDR_MKR,
           DECODE(FOREIGN_ADDRESS,'Y','N',CUST_ADDR_MKR) CUST_ADDR_MKR,
           METER_ADDR_MKR
    FROM BT_ADDRESSES bt
    WHERE (PROPERTY_ADDR_MKR = 'Y' OR METER_ADDR_MKR = 'Y' OR CUST_ADDR_MKR = 'Y')
    AND CD_ADDR_TYPE <> '005'
    UNION
    SELECT /*+ PARALLEL(bt,60) */
           NO_PROPERTY,
           F_CD_ADDRESS ADDRESS_PK,
           UPRN,
           UDPRN PAFADDRESSKEY,
           NULL PROPERTYNUMBERPROPERTY,
           NULL CUSTOMERNUMBERPROPERTY,
           UPRN_REASON_CODE UPRNREASONCODE,
           NULL SECONDADDRESABLEOBJECT,
           CASE WHEN NM_ADDR_OBJ_1 IN ('-','#') THEN
             NULL
           ELSE
             NM_ADDR_OBJ_1
           END PRIMARYADDRESSABLEOBJECT,
           TXT_FRGN_LOC_1 ADDRESSLINE01,
           TXT_FRGN_LOC_2 ADDRESSLINE02,
           TXT_FRGN_LOC_3 ADDRESSLINE03,
           TXT_FRGN_LOC_4 ADDRESSLINE04,
           TXT_FRGN_LOC_5 ADDRESSLINE05,
           'FR0' POSTCODE,
           NM_COUNTRY COUNTRY,
           NULL LOCATIONFREETEXTDESCRIPTOR,
           FOREIGN_ADDRESS,
           'N' PROPERTY_ADDR_MKR,
           CUST_ADDR_MKR,
           'N' METER_ADDR_MKR
    FROM BT_ADDRESSES
    WHERE FOREIGN_ADDRESS = 'Y' OR CD_ADDR_TYPE = '005';
    
    --added MTR.MANUFACTURER_PK to pick up all meters in mo_meter
    CURSOR CUR_PROP_METER (p_no_property BT_TVP054.NO_PROPERTY%TYPE) IS
      SELECT DISTINCT MANUFACTURERSERIALNUM_PK,
        MANUFACTURER_PK,
        MANUFCODE -- v1.10
      FROM MO_METER mtr
      WHERE mtr.INSTALLEDPROPERTYNUMBER = p_no_property;

    CURSOR cur_cus_addr (p_no_property BT_TVP054.NO_PROPERTY%TYPE) IS
      SELECT CUSTOMERNUMBER_PK,
        STWPROPERTYNUMBER_PK
      FROM MO_CUSTOMER
      WHERE STWPROPERTYNUMBER_PK = p_no_property;
      
  TYPE tab_prop_addr IS TABLE OF cur_prop_addr%ROWTYPE INDEX BY PLS_INTEGER;
  t_prop_addr tab_prop_addr;
  
  PROCEDURE CREATE_INSTALL_PROPERTY AS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_INSTALL_ADDRESS';
    EXECUTE IMMEDIATE 'ALTER INDEX INST_PROP_NUMBER UNUSABLE';
    
    INSERT /*+ append */
    INTO BT_INSTALL_ADDRESS
    SELECT /*+ PARALLEL(MM,60) PARALLEL(MEP,60) PARALLEL(MC,60) */
            DISTINCT PN.STWPROPERTYNUMBER_PK,
           DECODE(NVL(MM.INSTALLEDPROPERTYNUMBER,0),0,'N','Y') METER_ADDR_MKR,
           DECODE(NVL(MEP.STWPROPERTYNUMBER_PK,0),0,'N','Y') PROPERTY_ADDR_MKR,
           DECODE(NVL(MEP.STWPROPERTYNUMBER_PK,0),0,'N','Y') CUST_ADDR_MKR
    FROM 
    (SELECT DISTINCT STWPROPERTYNUMBER_PK FROM MO_SUPPLY_POINT
    UNION
    SELECT DISTINCT STWPROPERTYNUMBER_PK FROM MO_ELIGIBLE_PREMISES
    UNION
    SELECT DISTINCT INSTALLEDPROPERTYNUMBER STWPROPERTYNUMBER_PK FROM MO_METER) PN,
    MO_METER MM,
    MO_ELIGIBLE_PREMISES MEP,
    MO_CUSTOMER MC
    WHERE PN.STWPROPERTYNUMBER_PK = MM.INSTALLEDPROPERTYNUMBER(+)
    AND PN.STWPROPERTYNUMBER_PK = MEP.STWPROPERTYNUMBER_PK(+)
    AND PN.STWPROPERTYNUMBER_PK = MC.STWPROPERTYNUMBER_PK(+);
    
    COMMIT;
    
    EXECUTE IMMEDIATE 'ALTER INDEX INST_PROP_NUMBER REBUILD';
  END CREATE_INSTALL_PROPERTY;        
  
  PROCEDURE CREATE_FOREIGN_ADDRESS AS
  BEGIN
    EXECUTE immediate 'TRUNCATE TABLE BT_FOREIGN_ADDRESSES';
    EXECUTE immediate 'ALTER INDEX IND_BT_FORE_NO_ACC UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_FORE_CD UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_FORE_PROP UNUSABLE';
    
    --INSERT /*+ append */
    INSERT
    INTO BT_FOREIGN_ADDRESSES    
    SELECT
      /*+ PARALLEL(KGEN,60) PARALLEL(T046,60) PARALLEL(TAD,60) PARALLEL(T022,60) PARALLEL(FGN,60) */
      DISTINCT
      FGN.CD_ADDRESS,
      KGEN.NO_LEGAL_ENTITY,
      KGEN.NO_ACCOUNT,
      KGEN.NO_PROPERTY,
      KGEN.NO_COMBINE_024 ,
      NVL(TO_CHAR(KGEN.DT_END,'YYYY-MM-DD'),' ') val_1 ,
      NVL(REPLACE(tad.AD_PC_AREA ||tad.AD_PC_DIST ||' ' || TAD.AD_PC_SECT ||TAD.AD_PC_UNIT,' ') ,' ') val_2 ,
      TAD.NM_TOWN,
      tad.NM_STREET_1  ||' ' ||tad.NM_STREET_TYPE_1 val_3 ,
      TAD.NM_DEP_LOC_1,
      TAD.NO_BLDG ,
      TAD.NM_ADDR_OBJ_1,
      TAD.NO_PO_BOX,
      FGN.AD_LINE_1,
      FGN.AD_LINE_2,
      FGN.AD_LINE_3 ,
      FGN.AD_LINE_4,
      FGN.TXT_FRGN_LOC_1,
      FGN.TXT_FRGN_LOC_2,
      FGN.TXT_FRGN_LOC_3,
      FGN.TXT_FRGN_LOC_4,
      FGN.TXT_FRGN_LOC_5,
      '022' source_table,
      FGN.NM_COUNTRY
    FROM RECEPTION.TVMNHHDTL KGEN ,
      CIS.TVP046PROPERTY T046 ,
      CIS.TADADDRFAST TAD ,
      CIS.TVP022CUSTACRADUSG T022 ,
      CIS.TADADDRFAST FGN
    WHERE KGEN.PHASE   = '1'
    AND KGEN.TP_CUST_ACCT_ROLE = 'P'
    AND KGEN.NC024_DT_END     IS NULL
    AND KGEN.FG_TOO_HARD      IS NULL
    AND KGEN.ST_SERV_PROV     IN ('A','G')
    AND NVL(KGEN.DT_END,SYSDATE+1) > SYSDATE
    AND NVL(KGEN.DT_END,SYSDATE+1)  <> KGEN.DT_START
    AND T046.NO_PROPERTY       = KGEN.NO_PROPERTY
    AND T046.CD_COMPANY_SYSTEM = 'STW1'
    AND TAD.CD_ADDRESS         = T046.CD_ADDRESS
    AND T022.NO_COMBINE_024    = KGEN.NO_COMBINE_024
    AND T022.CD_COMPANY_SYSTEM = 'STW1'
    AND T022.DT_END           IS NULL
    AND FGN.CD_ADDRESS         = T022.CD_ADDRESS
    AND FGN.AD_PC_AREA         = ' '
    AND FGN.AD_PC_DIST         = ' '
    AND FGN.AD_PC_SECT         = ' '
    AND FGN.AD_PC_UNIT         = ' '
    AND FGN.NM_TOWN            = ' '
    AND FGN.NM_STREET_1        = ' '
    AND FGN.NM_STREET_TYPE_1   = ' '
    AND FGN.NM_DEP_LOC_1       = ' '
    AND FGN.NO_BLDG            = ' '
    AND FGN.NM_ADDR_OBJ_1      = ' '
    AND FGN.NO_PO_BOX          = ' ';    
    
    COMMIT;
    
    --INSERT /*+ append */
    INSERT
    INTO BT_FOREIGN_ADDRESSES       
    SELECT
      /*+ PARALLEL(KGEN,60) PARALLEL(T046,60) PARALLEL(TAD,60) PARALLEL(T037,60) PARALLEL(FGN,60) */
      DISTINCT
      FGN.CD_ADDRESS,
      KGEN.NO_LEGAL_ENTITY ,
      KGEN.NO_ACCOUNT,
      KGEN.NO_PROPERTY ,
      KGEN.NO_COMBINE_024 ,
      NVL(TO_CHAR(KGEN.DT_END,'YYYY-MM-DD'),' ') VAL_1 ,
      NVL(REPLACE (TAD.AD_PC_AREA ||TAD.AD_PC_DIST ||' ' || TAD.AD_PC_SECT ||TAD.AD_PC_UNIT,' ') ,' ') VAL_2 ,
      TAD.NM_TOWN ,
      TAD.NM_STREET_1 ||' ' ||TAD.NM_STREET_TYPE_1 VAL_3 ,
      TAD.NM_DEP_LOC_1 ,
      TAD.NO_BLDG ,
      TAD.NM_ADDR_OBJ_1,
      TAD.NO_PO_BOX,
      FGN.AD_LINE_1,
      FGN.AD_LINE_2,
      FGN.AD_LINE_3 ,
      FGN.AD_LINE_4,
      FGN.TXT_FRGN_LOC_1 ,
      FGN.TXT_FRGN_LOC_2,
      FGN.TXT_FRGN_LOC_3 ,
      FGN.TXT_FRGN_LOC_4 ,
      FGN.TXT_FRGN_LOC_5,
      '037' SOURCE_TABLE,
      FGN.NM_COUNTRY
    FROM RECEPTION.TVMNHHDTL KGEN ,
      CIS.TVP046PROPERTY T046 ,
      CIS.TADADDRFAST TAD ,
      CIS.TVP037LEADDRUSAGE T037 ,
      CIS.TADADDRFAST FGN
    WHERE KGEN.PHASE   = '1'
    AND KGEN.TP_CUST_ACCT_ROLE = 'P'
    AND KGEN.NC024_DT_END     IS NULL
    AND KGEN.FG_TOO_HARD      IS NULL
    AND KGEN.ST_SERV_PROV     IN ('A','G')
    AND NVL(KGEN.DT_END,SYSDATE+1) > SYSDATE
    AND NVL(KGEN.DT_END,SYSDATE+1) <> KGEN.DT_START
    AND T046.NO_PROPERTY       = KGEN.NO_PROPERTY
    AND T046.CD_COMPANY_SYSTEM = 'STW1'
    AND TAD.CD_ADDRESS         = T046.CD_ADDRESS
    AND T037.NO_LEGAL_ENTITY   = KGEN.NO_LEGAL_ENTITY
    AND T037.CD_COMPANY_SYSTEM = 'STW1'
    AND T037.DT_END           IS NULL
    AND FGN.CD_ADDRESS         = T037.CD_ADDRESS
    AND FGN.AD_PC_AREA         = ' '
    AND FGN.AD_PC_DIST         = ' '
    AND FGN.AD_PC_SECT         = ' '
    AND FGN.AD_PC_UNIT         = ' '
    AND FGN.NM_TOWN            = ' '
    AND FGN.NM_STREET_1        = ' '
    AND FGN.NM_STREET_TYPE_1   = ' '
    AND FGN.NM_DEP_LOC_1       = ' '
    AND FGN.NO_BLDG            = ' '
    AND FGN.NM_ADDR_OBJ_1      = ' '
    AND FGN.NO_PO_BOX          = ' '
    AND NOT EXISTS (SELECT 1 FROM BT_FOREIGN_ADDRESSES
                    WHERE NO_LEGAL_ENTITY = KGEN.NO_LEGAL_ENTITY
                    AND NO_ACCOUNT = KGEN.NO_ACCOUNT
                    AND NO_PROPERTY = KGEN.NO_PROPERTY);  
                    
    COMMIT;
    
    DBMS_STATS.gather_table_stats('MOUTRAN', 'BT_FOREIGN_ADDRESSES');
  
    EXECUTE immediate 'ALTER INDEX IND_BT_FORE_NO_ACC REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_FORE_CD REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_FORE_PROP REBUILD';
  END CREATE_FOREIGN_ADDRESS;   
  
  PROCEDURE CREATE_ADDRESSES AS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_ADDRESSES';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_PROPERTY UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_ADDRESS UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_FOREIGN UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_TYPE UNUSABLE';  
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_ADDR_MKR UNUSABLE';  

    
   -- DBMS_STATS.GATHER_TABLE_STATS('MOUTRAN', 'MO_ELIGIBLE_PREMISES');
--    EXECUTE IMMEDIATE 'ALTER INDEX PK_STWPROPERTYNUMBER REBUILD';
    
    INSERT /*+ append */
    INTO BT_ADDRESSES
    SELECT 
        /*+ PARALLEL(adf,60) PARALLEL(T046,60) PARALLEL(ADR,60) PARALLEL(T1,60) PARALLEL(ECT,60) PARALLEL(FA,60) */
        t046.NO_PROPERTY,
        ADF.CD_ADDRESS,
        CASE WHEN LENGTH(ECT.UPRN) > 12 THEN
          NULL
        ELSE
          ECT.UPRN
        END AS UPRN,
        CASE WHEN LENGTH(ECT.UPRN) > 12 OR ECT.UPRN IS NULL THEN
          'OT'
        ELSE
          NULL
        END AS UPRN_REASON_CODE,        
        ect.UDPRN,
        ADR.CD_ADDR_TYPE,
        TRIM(ADF.NM_ADDR_OBJ_3) NM_ADDR_OBJ_3,
        TRIM(ADF.NM_ADDR_OBJ_2) NM_ADDR_OBJ_2,
        TRIM(ADF.NM_ADDR_OBJ_1) NM_ADDR_OBJ_1,
        TRIM(ADF.NM_ORG_1) NM_ORG_1,
        TRIM(adf.NM_ORG_2) NM_ORG_2,
        FA.CD_ADDRESS F_CD_ADDRESS,
        TRIM(FA.TXT_FRGN_LOC_1) TXT_FRGN_LOC_1,
        TRIM(FA.TXT_FRGN_LOC_2) TXT_FRGN_LOC_2,
        TRIM(FA.TXT_FRGN_LOC_3) TXT_FRGN_LOC_3,
        TRIM(FA.TXT_FRGN_LOC_4) TXT_FRGN_LOC_4,
        TRIM(FA.TXT_FRGN_LOC_5) TXT_FRGN_LOC_5,
        TRIM(ADF.NO_BLDG) NO_BLDG,
        TRIM(ADF.NM_STREET_1) NM_STREET_1,
        TRIM(ADF.NM_STREET_TYPE_1) NM_STREET_TYPE_1,
        TRIM(ADF.NM_STREET_2) NM_STREET_2,
        TRIM(ADF.NM_STREET_TYPE_2) NM_STREET_TYPE_2,
        TRIM(ADF.NM_DEP_LOC_2) NM_DEP_LOC_2,
        TRIM(ADF.NM_DEP_LOC_1) NM_DEP_LOC_1,
        TRIM(ADF.NM_TOWN) NM_TOWN,
        CASE WHEN FN_VALIDATE_POSTCODE(TRIM(TRIM(ADF.AD_PC_AREA) || TRIM(ADF.AD_PC_DIST)) || ' ' || TRIM(TRIM(ADF.AD_PC_SECT) || TRIM(ADF.AD_PC_UNIT))) = 'INVALID' THEN
          'A0'
        ELSE
          FN_VALIDATE_POSTCODE(TRIM(TRIM(ADF.AD_PC_AREA) || TRIM(ADF.AD_PC_DIST)) || ' ' || TRIM(TRIM(ADF.AD_PC_SECT) || TRIM(ADF.AD_PC_UNIT)))
        END AS ADDRESS_DET_PC,
        T046.CD_GEOG_AREA_175,
        TRIM(FA.NM_COUNTRY) NM_COUNTRY,
        TRIM(ADF.NO_PO_BOX) NO_PO_BOX,
        DECODE(NVL(FA.NM_COUNTRY,'N'),'N','N','Y') FOREIGN_ADDRESS,
--        DECODE(NVL(MEP.STWPROPERTYNUMBER_PK,0),0,'N','Y') PROPERTY_ADDR_MKR,
--        DECODE(NVL(MA.INSTALLEDPROPERTYNUMBER,0),0,'N','Y') METER_ADDR_MKR,
--        DECODE(NVL(CA.STWPROPERTYNUMBER_PK,0),0,'N','Y') CUST_ADDR_MKR
        T1.PROPERTY_ADDR_MKR,
        T1.METER_ADDR_MKR,
        T1.CUST_ADDR_MKR
      FROM CIS.TADADDRFAST adf,
           CIS.TVP046PROPERTY t046,
           CIS.TADADDRESS ADR,
           BT_INSTALL_ADDRESS T1,
           CIS.ELIGIBILITY_CONTROL_TABLE ECT,
           BT_FOREIGN_ADDRESSES FA
--           (SELECT STWPROPERTYNUMBER_PK FROM MO_ELIGIBLE_PREMISES) MEP,
--           (SELECT DISTINCT INSTALLEDPROPERTYNUMBER FROM MO_METER) MA,
--           (SELECT DISTINCT STWPROPERTYNUMBER_PK FROM MO_CUSTOMER) CA
      WHERE adf.CD_ADDRESS         = t046.CD_ADDRESS
      AND t046.CD_COMPANY_SYSTEM  = 'STW1'
      AND ADR.CD_ADDRESS          = T046.CD_ADDRESS
      AND T1.STWPROPERTYNUMBER_PK = T046.NO_PROPERTY
--      AND T046.NO_PROPERTY        = MEP.STWPROPERTYNUMBER_PK(+)
      AND T046.NO_PROPERTY        = ECT.NO_PROPERTY(+)
      AND T046.NO_PROPERTY        = FA.NO_PROPERTY(+);
--      AND T046.NO_PROPERTY        = MA.INSTALLEDPROPERTYNUMBER(+)
--      AND T046.NO_PROPERTY         = CA.STWPROPERTYNUMBER_PK(+);
  
    COMMIT;
    
    DBMS_STATS.gather_table_stats('MOUTRAN', 'BT_ADDRESSES');
  
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_PROPERTY REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_ADDRESS REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_FOREIGN REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_TYPE REBUILD';   
    EXECUTE IMMEDIATE 'ALTER INDEX IDX_ADDR_MKR REBUILD';      
  END CREATE_ADDRESSES;
BEGIN
  -- initial variables
  l_progress              := 'Start';
  l_err.TXT_DATA          := c_module_name;
  l_err.TXT_KEY           := 0;
  l_job.NO_INSTANCE       := 0;
  l_no_row_read_prop      := 0;
  l_no_row_read_cust      := 0;
  l_no_row_read_meter     := 0;
  l_no_row_read_cd_adr    := 0;
  l_no_row_war            := 0;
  l_no_row_err            := 0;
  l_no_row_exp            := 0;
  l_prev_prop             := 0;
  l_prev_cd_address       := 0;
  l_job.IND_STATUS        := 'RUN';
  l_no_row_insert_prop    := 0;
  l_no_row_insert_cd_adr  := 0;
  l_no_row_insert_cust    := 0;
  l_no_row_insert_meter   := 0;
  l_no_row_dropped_prop   := 0;
  l_no_row_dropped_cd_adr := 0;
  l_no_row_dropped_cust   := 0;
  l_no_row_dropped_meter  := 0;
  l_progress              := 'processing ';
  
  P_MIG_BATCH.FN_STARTJOB(no_batch, 
                          no_job, 
                          c_module_name, 
                          l_job.NO_INSTANCE, 
                          l_job.ERR_TOLERANCE, 
                          l_job.EXP_TOLERANCE, 
                          l_job.WAR_TOLERANCE, 
                          l_job.NO_COMMIT, 
                          l_job.NO_STREAM, 
                          l_job.NO_RANGE_MIN, 
                          l_job.NO_RANGE_MAX, 
                          l_job.IND_STATUS);

   COMMIT;
   
   EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
   COMMIT;
   
  L_PROGRESS := 'Rebuilding BT_INSTALL_ADDRESS';
  CREATE_INSTALL_PROPERTY;
  
  l_progress := 'Rebuilding BT_FOREIGN_ADDRESSES';
  CREATE_FOREIGN_ADDRESS;
  
  L_PROGRESS := 'Rebuilding BT_ADDRESSES';
  CREATE_ADDRESSES;
  
  -- any errors set return code and exit out
  IF l_job.IND_STATUS = 'ERR' THEN
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    return_code := -1;
    RETURN;
  END IF;
  -- start processing all records for range supplied
  OPEN cur_prop_addr(l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);
  l_progress := 'loop processing ';
  LOOP
    FETCH cur_prop_addr BULK COLLECT INTO t_prop_addr LIMIT l_job.NO_COMMIT;
  
    FOR i IN 1..t_prop_addr.COUNT
    LOOP
      L_ERR.TXT_KEY := t_prop_addr(i).NO_PROPERTY || ',' || t_prop_addr(i).ADDRESS_PK;
      l_rec_exc     := FALSE;
      l_rec_war     := FALSE;
      l_progress    := 'Main cursor prop address loop processing';
      l_rec_written := TRUE;
      l_adr         := NULL;

      l_adr.ADDRESSLINE01 := t_prop_addr(i).ADDRESSLINE01;
      L_ADR.ADDRESSLINE02 := t_prop_addr(i).ADDRESSLINE02;
      L_ADR.ADDRESSLINE03 := t_prop_addr(i).ADDRESSLINE03;
      L_ADR.ADDRESSLINE04 := t_prop_addr(i).ADDRESSLINE04;
      L_ADR.ADDRESSLINE05 := t_prop_addr(i).ADDRESSLINE05;

      L_PROGRESS := ' Ensure ADDRESSLINE01 is populated';

      IF TRIM(L_ADR.ADDRESSLINE01) IS NULL THEN
        IF TRIM(L_ADR.ADDRESSLINE02) IS NOT NULL THEN
          L_ADR.ADDRESSLINE01        := L_ADR.ADDRESSLINE02;
          L_ADR.ADDRESSLINE02        := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR    := TRUE;
        ELSIF TRIM(L_ADR.ADDRESSLINE03) IS NOT NULL THEN
          L_ADR.ADDRESSLINE01           := L_ADR.ADDRESSLINE03;
          L_ADR.ADDRESSLINE03           := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR    := TRUE;
        ELSIF TRIM(L_ADR.ADDRESSLINE04) IS NOT NULL THEN
          L_ADR.ADDRESSLINE01           := L_ADR.ADDRESSLINE04;
          L_ADR.ADDRESSLINE04           := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR    := TRUE;
        ELSE
          L_ADR.ADDRESSLINE01 := L_ADR.ADDRESSLINE05;
          L_ADR.ADDRESSLINE05 := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100), L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR    := true;
        END IF;
      END IF;

        -- Quit if we have exceeded warning tolerance
      IF (L_REC_WAR = TRUE AND L_NO_ROW_WAR > L_JOB.WAR_TOLERANCE) THEN
        CLOSE cur_prop_addr;
        L_JOB.IND_STATUS := 'ERR';
        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Warning tolerance level exceeded- Dropping bad data', l_err.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
        COMMIT;
        return_code := -1;
        RETURN;
      END IF;
      
      -- If we get this far insert the record in MO_ADDRESS table
      l_progress           := 'INSERT INTO MO_ADDRESS';
      L_NO_ROW_READ_CD_ADR := L_NO_ROW_READ_CD_ADR + 1;          
      L_REC_WRITTEN := true;
       
          BEGIN
            INSERT
            INTO MO_ADDRESS
              (
                ADDRESS_PK,
                UPRN,
                PAFADDRESSKEY,
                PROPERTYNUMBERPROPERTY,
                CUSTOMERNUMBERPROPERTY,
                UPRNREASONCODE,
                SECONDADDRESABLEOBJECT,
                PRIMARYADDRESSABLEOBJECT,
                ADDRESSLINE01,
                ADDRESSLINE02,
                ADDRESSLINE03,
                ADDRESSLINE04,
                ADDRESSLINE05,
                POSTCODE,
                COUNTRY,
                LOCATIONFREETEXTDESCRIPTOR,
                FOREIGN_ADDRESS
              )
              VALUES
              (
                t_prop_addr(i).ADDRESS_PK,
                t_prop_addr(i).UPRN,
                t_prop_addr(i).PAFADDRESSKEY,
                t_prop_addr(i).PROPERTYNUMBERPROPERTY,
                T_PROP_ADDR(I).CUSTOMERNUMBERPROPERTY,
                T_PROP_ADDR(I).UPRNREASONCODE,
                T_PROP_ADDR(I).SECONDADDRESABLEOBJECT,
                t_prop_addr(i).PRIMARYADDRESSABLEOBJECT,
                L_ADR.ADDRESSLINE01,
                L_ADR.ADDRESSLINE02,
                L_ADR.ADDRESSLINE03,
                L_ADR.ADDRESSLINE04,
                L_ADR.ADDRESSLINE05,
                T_PROP_ADDR(I).POSTCODE,
                T_PROP_ADDR(I).COUNTRY,
                T_PROP_ADDR(I).LOCATIONFREETEXTDESCRIPTOR,
                t_prop_addr(i).FOREIGN_ADDRESS
              );
          EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', 'WARNING: Address already exists in MO_ADDRESS - skipping', L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
            L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
            L_REC_WAR    := true; 
          WHEN OTHERS THEN
            l_no_row_dropped_cd_adr := l_no_row_dropped_cd_adr + 1;
            l_rec_written           := FALSE;
            l_error_number          := SQLCODE;
            L_ERROR_MESSAGE         := SQLERRM;
            P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', SUBSTR(l_error_message,1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            L_NO_ROW_ERR := L_NO_ROW_ERR + 1;
            l_rec_exc    := true; --trap an error from here
          END;
          -- keep count of records written
          IF l_rec_written THEN
            L_NO_ROW_INSERT_CD_ADR := L_NO_ROW_INSERT_CD_ADR + 1;
--            COMMIT; -- temp while testing
          ELSE
            -- if tolearance limit has been exceeded, set error message and exit out
            IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
              P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_cd_adr, 'Distinct addresses read');
              P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_cd_adr, 'Distinct addresses inserted');
              P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_cd_adr, 'Distinct addresses dropped');
              
              CLOSE cur_prop_addr;
              
              l_job.IND_STATUS := 'ERR';
              P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded', l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
              P_MIG_BATCH.FN_UPDATEJOB (no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
              COMMIT;
              
              return_code := -1;
              RETURN;
            END IF;
          END IF;

        L_PROGRESS         := 'INSERT INTO MO_PROPERTY_ADDRESS';
--        l_no_row_read_prop := l_no_row_read_prop + 1; -- V 2.01
--        l_rec_written      := TRUE;

         IF T_PROP_ADDR(I).PROPERTY_ADDR_MKR = 'Y' THEN
            l_no_row_read_prop := l_no_row_read_prop + 1; -- V 2.01
            l_rec_written      := TRUE;
            
            BEGIN
              INSERT
              INTO MO_PROPERTY_ADDRESS
                (
                  ADDRESSPROPERTY_PK,
                  ADDRESS_PK,
                  STWPROPERTYNUMBER_PK,
                  ADDRESSUSAGEPROPERTY,
                  EFFECTIVEFROMDATE,
                  EFFECTIVETODATE
                )
                VALUES
                (
                  ADDRESSPROPERTY_PK_SEQ.NEXTVAL,
                  t_prop_addr(i).ADDRESS_PK,
                  t_prop_addr(i).no_property,
                  'LocatedAt',
                  SYSDATE,
                  NULL
                );
            EXCEPTION
            WHEN OTHERS THEN
                l_no_row_dropped_prop := l_no_row_dropped_prop + 1;
                l_rec_written         := FALSE;
                l_error_number        := SQLCODE;
                l_error_message       := SQLERRM;
                P_MIG_BATCH.FN_ERRORLOG (NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100), L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_ERR := L_NO_ROW_ERR + 1;
            END;
            -- keep count of records written
            IF l_rec_written THEN
              L_NO_ROW_INSERT_PROP := L_NO_ROW_INSERT_PROP + 1;
--              COMMIT; -- temp while testing
            ELSE
              -- if tolearance limit has een exceeded, set error message and exit out
              IF (L_NO_ROW_EXP > L_JOB.EXP_TOLERANCE OR L_NO_ROW_ERR > L_JOB.ERR_TOLERANCE OR L_NO_ROW_WAR > L_JOB.WAR_TOLERANCE) THEN
                P_MIG_BATCH.FN_RECONLOG (NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 971,L_NO_ROW_READ_PROP, 'Distinct eligible properties read');
                P_MIG_BATCH.FN_RECONLOG (NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 991,L_NO_ROW_INSERT_PROP, 'Distinct eligible properties inserted');
                P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 981,l_no_row_dropped_prop, 'Distinct eligible properties dropped');
                
                CLOSE cur_prop_addr;
                l_job.IND_STATUS := 'ERR';
                
                P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded', l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                P_MIG_BATCH.FN_UPDATEJOB (no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                COMMIT;
                
                return_code := -1;
                RETURN;
              END IF;
            END IF;
          END IF;
          
          -- Write meter address record
        IF t_prop_addr(i).METER_ADDR_MKR = 'Y' THEN
          FOR rec_meter IN cur_prop_meter(t_prop_addr(i).no_property)
          LOOP
            l_no_row_read_meter := l_no_row_read_meter + 1;
            l_progress          := 'INSERT INTO MO_METER_ADDRESS';
            L_REC_WRITTEN       := true;

              BEGIN
                INSERT
                INTO MO_METER_ADDRESS
                  (
                    ADDRESSPROPERTY_PK,
                    METERSERIALNUMBER_PK,
                    ADDRESS_PK,
                    ADDRESSUSAGEPROPERTY,
                    EFFECTIVEFROMDATE,
                    EFFECTIVETODATE,
                    MANUFACTURER_PK ,
                    INSTALLEDPROPERTYNUMBER,
                    MANUFCODE
                  ) -- v1.10
                  VALUES
                  (
                    AddressProperty_PK_SEQ.NEXTVAL,
                    REC_METER.MANUFACTURERSERIALNUM_PK,
                    t_prop_addr(i).ADDRESS_PK,
                    'SitedAt',
                    sysdate,
                    NULL,
                    REC_METER.MANUFACTURER_PK ,
                    t_prop_addr(i).NO_PROPERTY,
                    rec_meter.MANUFCODE
                  ); --v1.10
              EXCEPTION
              WHEN OTHERS THEN
                l_no_row_dropped_meter := l_no_row_dropped_meter + 1;
                l_rec_written          := FALSE;
                l_error_number         := SQLCODE;
                l_error_message        := SQLERRM;
                P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', SUBSTR(l_error_message,1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                L_NO_ROW_ERR := L_NO_ROW_ERR + 1;
              END;
              -- keep count of records written
              IF L_REC_WRITTEN THEN
                L_NO_ROW_INSERT_METER := L_NO_ROW_INSERT_METER + 1;
--                COMMIT; -- temp while testing
              ELSE
                -- if tolearance limit has been exceeded, set error message and exit out
                IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
                  P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 972, l_no_row_read_meter, 'Distinct meters read');
                  P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 992, l_no_row_insert_meter, 'Distinct meters inserted');                  
                  P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 982, l_no_row_dropped_meter, 'Distinct meters dropped');
                  
                  CLOSE cur_prop_addr;
                  l_job.IND_STATUS := 'ERR';
                  
                  P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded', l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                  P_MIG_BATCH.FN_UPDATEJOB (no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                  COMMIT;
                  
                  return_code := -1;
                  RETURN;
                END IF;
              END IF;
          END LOOP;
        END IF;
        
          -- Write customer address record
        IF t_prop_addr(i).CUST_ADDR_MKR = 'Y' THEN
          FOR rec_cus IN cur_cus_addr(t_prop_addr(i).no_property)
          LOOP
            L_NO_ROW_READ_CUST := L_NO_ROW_READ_CUST + 1;
            l_progress         := 'INSERT INTO MO_CUST_ADDRESS';
            L_REC_WRITTEN      := TRUE;
            --added STWPROPERTYNUMBER_PK to identify a specific address for a customer at a property
            --to ensure that only one address
            BEGIN
              INSERT
              INTO MO_CUST_ADDRESS
                (
                  ADDRESSPROPERTY_PK,
                  ADDRESS_PK,
                  CUSTOMERNUMBER_PK,
                  ADDRESSUSAGEPROPERTY,
                  EFFECTIVEFROMDATE,
                  EFFECTIVETODATE,
                  STWPROPERTYNUMBER_PK
                )
                VALUES
                (
                  ADDRESSPROPERTY_PK_SEQ.NEXTVAL,
                  t_prop_addr(i).ADDRESS_PK,
                  rec_cus.CUSTOMERNUMBER_PK,
                  'BilledAt',
                  SYSDATE,
                  NULL,
                  rec_cus.STWPROPERTYNUMBER_PK
                );
            EXCEPTION
            WHEN OTHERS THEN
              l_no_row_dropped_cust := l_no_row_dropped_cust + 1;
              l_rec_written         := FALSE;
              l_error_number        := SQLCODE;
              l_error_message       := SQLERRM;
              
              P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', SUBSTR(l_error_message,1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_err := l_no_row_err + 1;
            END;
            -- keep count of records written
            IF l_rec_written THEN
              L_NO_ROW_INSERT_CUST := L_NO_ROW_INSERT_CUST + 1;
--              COMMIT; -- temp while testing
            ELSE
              -- if tolearance limit has een exceeded, set error message and exit out
              IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
                P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 973, l_no_row_read_cust, 'Distinct customers read');
                P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 993, l_no_row_insert_cust, 'Distinct customers inserted');
                P_MIG_BATCH.FN_RECONLOG (no_batch, l_job.NO_INSTANCE, 'CP28', 983, l_no_row_dropped_cust, 'Distinct customers dropped');
                
                CLOSE cur_prop_addr;
                l_job.IND_STATUS := 'ERR';
                
                P_MIG_BATCH.FN_ERRORLOG (no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded', l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
                P_MIG_BATCH.FN_UPDATEJOB (no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                COMMIT;
                return_code := -1;
                RETURN;
              END IF;
            END IF;
          END LOOP;
        END IF; 
      
    END LOOP; -- inner main cursor loop
    
    IF t_prop_addr.COUNT < l_job.NO_COMMIT THEN
      EXIT;
    ELSE
      COMMIT;
    END IF;
  END LOOP; -- outer main cursor loop
  
  CLOSE cur_prop_addr;

  COMMIT; 
  EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
  
  l_progress             := 'Writing Counts';
  
  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_cd_adr, 'Distinct addresses read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_cd_adr, 'Distinct addresses inserted');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 980, L_NO_ROW_DROPPED_CD_ADR, 'Distinct addresses dropped');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 971,L_NO_ROW_READ_PROP, 'Distinct eligible properties read');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 991,L_NO_ROW_INSERT_PROP, 'Distinct eligible properties inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 981,l_no_row_dropped_prop, 'Distinct eligible properties dropped');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 972, l_no_row_read_meter, 'Distinct meters read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 992, l_no_row_insert_meter, 'Distinct meters inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 982, l_no_row_dropped_meter, 'Distinct meters dropped');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 973, l_no_row_read_cust, 'Distinct customers read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 993, l_no_row_insert_cust, 'Distinct customers inserted');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP28', 983, L_NO_ROW_DROPPED_CUST, 'Distinct customers dropped');
  
  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  
  l_progress := 'End';
  COMMIT;
EXCEPTION
WHEN OTHERS THEN
  l_error_number  := SQLCODE;
  L_ERROR_MESSAGE := SQLERRM;
  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', SUBSTR(l_error_message,1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error', l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
  l_job.IND_STATUS := 'ERR';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  RETURN_CODE := -1;
END P_MOU_TRAN_ADDRESS ;
/
show error;

exit;