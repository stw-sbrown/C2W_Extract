create or replace
PROCEDURE P_MOU_TRAN_ADDRESS (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                              no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                              return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Address Transform MO Extract
--
-- AUTHOR        : Sreedhar Pallati
--
-- FILENAME      : P_MOU_TRAN_ADDRESS .sql
--
-- Subversion $Revision: 4041 $
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
-- v 1.10    23/05/2016  D.Cheung   Added NO_PROPERTY and MANUFCODE fields to METER_ADDRESS
-----------------------------------------------------------------------------------------
 
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_ADDRESS';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512); 
  l_progress                    VARCHAR2(100);
  l_prev_prop                   BT_TVP054.NO_PROPERTY%TYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read_prop            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_cd_adr          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_meter           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_cust            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_prop          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_cd_adr        MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_cust          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_meter         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_prop         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE; 
  l_no_row_dropped_cd_adr       MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE; 
  l_no_row_dropped_cust         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE; 
  l_no_row_dropped_meter        MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE; 
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;

  l_adr                         MO_ADDRESS%ROWTYPE;
  l_prev_cd_address             CIS.TADADDRFAST.CD_ADDRESS%TYPE;

  L_NO_UTL_EQUIP                VARCHAR2(40);
  L_REC_EXC                     BOOLEAN;
  L_REC_WAR                     BOOLEAN;
  L_POSTCODE                    VARCHAR2(8);

CURSOR CUR_PROP_ADDR (P_NO_PROPERTY_START   BT_TVP054.NO_PROPERTY%TYPE,
                      P_NO_PROPERTY_END     BT_TVP054.NO_PROPERTY%TYPE)                 
   IS 
  SELECT * FROM (
SELECT  /*+ PARALLEL(t046,4) PARALLEL(adf,4) PARALLEL(spt,4) PARALLEL(t054,4) */
           t046.NO_PROPERTY,
           adf.CD_ADDRESS,
           T054.UPRN,
           ect.UDPRN,
           adr.CD_ADDR_TYPE,
           --adr.CD_ADDR_PAF,  change recorded in F+V doc
           adf.NM_ADDR_OBJ_3,
           adf.NM_ADDR_OBJ_2,
           adf.NM_ADDR_OBJ_1,
           adf.NM_ORG_1,
           adf.NM_ORG_2,
           adf.TXT_FRGN_LOC_1,
           adf.TXT_FRGN_LOC_2,
           adf.TXT_FRGN_LOC_3,
           adf.TXT_FRGN_LOC_4,
           adf.TXT_FRGN_LOC_5,
           adf.NO_BLDG,
           adf.NM_STREET_1,
           adf.NM_STREET_TYPE_1,
           adf.NM_STREET_2,
           adf.NM_STREET_TYPE_2,
           adf.NM_DEP_LOC_2,
           adf.NM_DEP_LOC_1,
           ADF.NM_TOWN,
           TRIM(TRIM(adf.AD_PC_AREA) || TRIM(adf.AD_PC_DIST)) || ' ' || trim(TRIM(adf.AD_PC_SECT) || TRIM(adf.AD_PC_UNIT)) AS ADDRESS_DET_PC,
           t046.CD_GEOG_AREA_175,
           adf.NM_COUNTRY
    FROM   CIS.TADADDRFAST    adf,
           CIS.TADADDRESS     adr,
           CIS.TVP046PROPERTY t046,
           MO_ELIGIBLE_PREMISES t054,
           MO_SUPPLY_POINT      spt,
           CIS.ELIGIBILITY_CONTROL_TABLE ECT
    WHERE  T054.STWPROPERTYNUMBER_PK BETWEEN P_NO_PROPERTY_START AND P_NO_PROPERTY_END
    AND    spt.STWPROPERTYNUMBER_PK = t054.STWPROPERTYNUMBER_PK
    AND    spt.STWPROPERTYNUMBER_PK(+) = ect.no_property
    AND    t046.CD_COMPANY_SYSTEM = 'STW1'
    AND    t046.NO_PROPERTY       = spt.STWPROPERTYNUMBER_PK 
    AND    t046.NO_PROPERTY       > 1
    AND    adf.CD_ADDRESS         = t046.CD_ADDRESS
    AND    ADR.CD_ADDRESS         = T046.CD_ADDRESS
UNION 
/* amended 2nd part*/
SELECT  /*+ PARALLEL(t046,4) PARALLEL(adf,4) PARALLEL(spt,4) PARALLEL(t054,4) */
           t046.NO_PROPERTY,
           ADF.CD_ADDRESS,
           bt.UPRN,
           bt.UDPRN,
           adr.CD_ADDR_TYPE,
           --adr.CD_ADDR_PAF,  change recorded in F+V doc
           adf.NM_ADDR_OBJ_3,
           adf.NM_ADDR_OBJ_2,
           adf.NM_ADDR_OBJ_1,
           adf.NM_ORG_1,
           adf.NM_ORG_2,
           adf.TXT_FRGN_LOC_1,
           adf.TXT_FRGN_LOC_2,
           adf.TXT_FRGN_LOC_3,
           adf.TXT_FRGN_LOC_4,
           adf.TXT_FRGN_LOC_5,
           adf.NO_BLDG,
           adf.NM_STREET_1,
           adf.NM_STREET_TYPE_1,
           adf.NM_STREET_2,
           adf.NM_STREET_TYPE_2,
           adf.NM_DEP_LOC_2,
           adf.NM_DEP_LOC_1,
           ADF.NM_TOWN,
           TRIM(TRIM(adf.AD_PC_AREA) || TRIM(adf.AD_PC_DIST)) || ' ' || trim(TRIM(adf.AD_PC_SECT) || TRIM(adf.AD_PC_UNIT)) AS ADDRESS_DET_PC,
           t046.CD_GEOG_AREA_175,
           adf.NM_COUNTRY
  FROM BT_TVP054 BT
  JOIN Cis.Tvp054servprovresp T054
    ON BT.NO_COMBINE_054 = T054.NO_COMBINE_054
  JOIN Cis.Tvp024custacctrole T024
    ON T024.no_combine_024 = BT.no_combine_024
  JOIN Cis.Tvp046property T046
    ON BT.no_property = t046.no_property
       AND T046.CD_COMPANY_SYSTEM = 'STW1'
       AND   Bt.Cd_Company_System ='STW1'
  JOIN Cis.Tadaddrfast Adf
    ON adf.cd_address = t046.cd_address
  JOIN Cis.Tadaddress  Adr
    ON ADR.CD_ADDRESS = T046.CD_ADDRESS
WHERE Bt.No_Legal_Entity = T024.No_Legal_Entity
   AND Bt.No_Account =  T024.No_Account 
   AND t054.dt_end is null
   AND   t024.cd_company_system='STW1'
   AND   t024.no_account  = t054.no_account
   AND   T024.DT_END IS NULL
   AND T046.NO_PROPERTY > 1)
ORDER BY CD_ADDRESS,NO_PROPERTY;   -- v 1.08
               
--added MTR.MANUFACTURER_PK to pick up all meters in mo_meter
CURSOR CUR_PROP_METER (p_no_property   BT_TVP054.NO_PROPERTY%TYPE) 
  IS
  SELECT DISTINCT mtr.MANUFACTURERSERIALNUM_PK AS NO_UTL_EQUIP,MTR.MANUFACTURER_PK as MANUFACTURER_PK, MTR.MANUFCODE    -- v1.10
  FROM   MO_METER               mtr,
         CIS.TVP063EQUIPMENT    t063,
		     CIS.TVP163EQUIPINST    t163,
         CIS.TVP202SERVPROVEQP  t202
  WHERE  t163.CD_COMPANY_SYSTEM = c_company_cd           
  AND    t163.NO_PROPERTY       = p_no_property
  AND    t163.ST_EQUIP_INST     = 'A'
  AND    t063.NO_EQUIPMENT      = t163.NO_EQUIPMENT
  AND    t202.NO_PROPERTY       = t163.NO_PROPERTY   
  AND    t202.CD_COMPANY_SYSTEM = t163.CD_COMPANY_SYSTEM  
  AND    t202.IND_INST_AT_PROP  = 'Y'
  AND    TRIM(T063.NO_UTL_EQUIP) = MTR.MANUFACTURERSERIALNUM_PK
  AND    t063.NO_EQUIPMENT      = mtr.METERREF;

     
CURSOR cur_cus_addr (p_no_property   BT_TVP054.NO_PROPERTY%TYPE) 
  IS
  SELECT CUSTOMERNUMBER_PK,STWPROPERTYNUMBER_PK
  FROM   MO_CUSTOMER
  WHERE  STWPROPERTYNUMBER_PK = p_no_property;
  
     
TYPE tab_prop_addr IS TABLE OF cur_prop_addr%ROWTYPE INDEX BY PLS_INTEGER;
t_prop_addr  tab_prop_addr;

 
BEGIN
   -- initial variables
     --DBMS_OUTPUT.PUT_LINE('output 1');         
   l_progress := 'Start';
   
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read_prop := 0;
   l_no_row_read_cust := 0;
   l_no_row_read_meter := 0;
   l_no_row_read_cd_adr := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;

   l_prev_prop := 0;
   l_prev_cd_address := 0;

   l_job.IND_STATUS := 'RUN';

   l_no_row_insert_prop := 0;
   l_no_row_insert_cd_adr := 0;
   l_no_row_insert_cust := 0;
   l_no_row_insert_meter := 0;
   l_no_row_dropped_prop := 0; 
   l_no_row_dropped_cd_adr := 0; 
   l_no_row_dropped_cust := 0; 
   l_no_row_dropped_meter := 0;

   l_progress := 'processing '; 

   P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name,
                         l_job.NO_INSTANCE,
                         l_job.ERR_TOLERANCE,               
                         l_job.EXP_TOLERANCE,      
                         l_job.WAR_TOLERANCE,    
                         l_job.NO_COMMIT,  
                         l_job.NO_STREAM,  
                         l_job.NO_RANGE_MIN, 
                         l_job.NO_RANGE_MAX,
                         l_job.IND_STATUS);
    
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
        L_ERR.TXT_KEY := T_PROP_ADDR(I).NO_PROPERTY || ',' || T_PROP_ADDR(I).CD_ADDRESS; 
         L_REC_EXC := FALSE;--ob
         L_REC_WAR := false;
      l_progress := 'Main cursor prop address loop processing'; 
      l_rec_written := TRUE;
      l_adr := null;
      
      -- Transform VALUES
      
      l_progress := 'TRANSFORM VALUES ';    
      
      l_adr.ADDRESS_PK := t_prop_addr(i).CD_ADDRESS;
      
      IF LENGTH(t_prop_addr(i).UPRN) > 12 THEN
         l_adr.UPRN := NULL;
      ELSE
         l_adr.UPRN := t_prop_addr(i).UPRN;
      END IF;

      IF l_adr.UPRN IS NULL THEN 
         l_adr.UPRNReasonCode := 'OT';
      ELSE
         l_adr.UPRNReasonCode := null;
      END IF;
      
      l_adr.PAFADDRESSKEY := t_prop_addr(i).UDPRN;
      
      IF T_PROP_ADDR(I).CD_ADDR_TYPE = '005' THEN 
         --l_adr.PAFADDRESSKEY := NULL; change recorded in F+V doc
        -- l_adr.COUNTRY := NULL; -- mistake in coding
         l_adr.COUNTRY := t_prop_addr(i).NM_COUNTRY;
         l_adr.SECONDADDRESABLEOBJECT := null;
      ELSE
         --l_adr.PAFADDRESSKEY := t_prop_addr(i).CD_ADDR_PAF;  change recorded in F+V doc
       --  l_adr.COUNTRY := t_prop_addr(i).NM_COUNTRY; -- mistake in coding
       l_adr.COUNTRY := NULL;
         
         IF TRIM(T_PROP_ADDR(I).NM_ADDR_OBJ_2) IS NOT NULL THEN
            L_ADR.SECONDADDRESABLEOBJECT := TRIM(T_PROP_ADDR(I).NM_ADDR_OBJ_3) || ' ' || TRIM(T_PROP_ADDR(I).NM_ADDR_OBJ_2); 
         ELSE
            IF trim(t_prop_addr(i).NM_ADDR_OBJ_3) IS NOT NULL THEN
               l_adr.SECONDADDRESABLEOBJECT := trim(t_prop_addr(i).NM_ADDR_OBJ_3);
            ELSE
               l_adr.SECONDADDRESABLEOBJECT := NULL;
            END IF;
         END IF;
      END IF;
      
    --  l_adr.PRIMARYADDRESSABLEOBJECT := trim(t_prop_addr(i).NM_ADDR_OBJ_1) || trim(t_prop_addr(i).NO_BLDG) || trim(t_prop_addr(i).NM_ORG_1) || trim(t_prop_addr(i).NM_ORG_2);
        /*
      IF trim(t_prop_addr(i).NM_ADDR_OBJ_1) IS NULL THEN
         IF TRIM(T_PROP_ADDR(I).NM_ORG_2) IS NOT NULL THEN
          l_adr.PRIMARYADDRESSABLEOBJECT := trim(t_prop_addr(i).NM_ORG_1) || ' ' || trim(t_prop_addr(i).NM_ORG_2);
         ELSE
            l_adr.PRIMARYADDRESSABLEOBJECT := trim(t_prop_addr(i).NM_ORG_1);
         END IF;
      ELSE
         IF TRIM(T_PROP_ADDR(I).NO_BLDG) IS NOT NULL THEN
            l_adr.PRIMARYADDRESSABLEOBJECT := trim(t_prop_addr(i).NO_BLDG) || ' ' || trim(t_prop_addr(i).NM_ADDR_OBJ_1);  
         ELSE
            l_adr.PRIMARYADDRESSABLEOBJECT := trim(t_prop_addr(i).NM_ADDR_OBJ_1);            
         END IF;
      END IF;  */
      
      L_ADR.PRIMARYADDRESSABLEOBJECT := TRIM(T_PROP_ADDR(I).NM_ADDR_OBJ_1); -- as per change in f+v doc
 
      IF t_prop_addr(i).CD_ADDR_TYPE = '005' THEN 
         l_adr.ADDRESSLINE01 := trim(t_prop_addr(i).TXT_FRGN_LOC_1); 
         l_adr.ADDRESSLINE02 := trim(t_prop_addr(i).TXT_FRGN_LOC_2);          
         l_adr.ADDRESSLINE03 := trim(t_prop_addr(i).TXT_FRGN_LOC_3); 
         L_ADR.ADDRESSLINE04 := TRIM(T_PROP_ADDR(I).TXT_FRGN_LOC_4); 
         --l_adr.ADDRESSLINE05 := NULL; 
         l_adr.ADDRESSLINE05 := TRIM(T_PROP_ADDR(I).TXT_FRGN_LOC_5);  -- as per change in f+v doc
         l_adr.POSTCODE := 'TBD';
      ELSE 

         --l_adr.ADDRESSLINE01 := trim(t_prop_addr(i).NM_STREET_1) || ' ' || trim(t_prop_addr(i).NM_STREET_TYPE_1); --this is not returning when l_adr.ADDRESSLINE01 is null
         --L_ADR.ADDRESSLINE01 := TRIM(T_PROP_ADDR(I).NM_STREET_1 || ' ' || T_PROP_ADDR(I).NM_STREET_TYPE_1);
         L_ADR.ADDRESSLINE01 := TRIM(trim(T_PROP_ADDR(I).NO_BLDG) || ' ' || T_PROP_ADDR(I).NM_STREET_1 || ' ' || T_PROP_ADDR(I).NM_STREET_TYPE_1); -- new as per change in F+v doc
         l_adr.ADDRESSLINE02 := trim(t_prop_addr(i).NM_STREET_2) || ' ' || trim(t_prop_addr(i).NM_STREET_TYPE_2);
         
--         IF trim(t_prop_addr(i).NM_STREET_TYPE_1) IS NOT NULL THEN       
 --           l_adr.ADDRESSLINE01 := trim(t_prop_addr(i).NM_STREET_1) || ' ' || trim(t_prop_addr(i).NM_STREET_TYPE_1);
 --        ELSE 
 --           IF trim(t_prop_addr(i).NM_STREET_1) IS NOT NULL THEN
 --              l_adr.ADDRESSLINE01 := trim(t_prop_addr(i).NM_STREET_1);
 --           ELSE
 --              l_adr.ADDRESSLINE01 := NULL;
 --           END IF;
 --        END IF;
 --           
 --        IF trim(t_prop_addr(i).NM_STREET_TYPE_2) IS NOT NULL THEN       
 --           l_adr.ADDRESSLINE02 := trim(t_prop_addr(i).NM_STREET_2) || ' ' || trim(t_prop_addr(i).NM_STREET_TYPE_2);
 --        ELSE 
 --           IF trim(t_prop_addr(i).NM_STREET_1) IS NOT NULL THEN
 --              l_adr.ADDRESSLINE02 := trim(t_prop_addr(i).NM_STREET_2);
 --           ELSE
 --              l_adr.ADDRESSLINE02 := NULL;
 --           END IF;
 --        END IF;

         l_adr.ADDRESSLINE03 := trim(t_prop_addr(i).NM_DEP_LOC_2);
         l_adr.ADDRESSLINE04 := trim(t_prop_addr(i).NM_DEP_LOC_1);
         l_adr.ADDRESSLINE05 := trim(t_prop_addr(i).NM_TOWN);
         l_adr.POSTCODE := t_prop_addr(i).ADDRESS_DET_PC;
         IF l_adr.POSTCODE IS NULL THEN 
            l_adr.POSTCODE := 'TBD';
         END IF;
         
      END IF;
      
      l_adr.PROPERTYNUMBERPROPERTY := NULL;
      l_adr.CUSTOMERNUMBERPROPERTY := null;
      L_ADR.LOCATIONFREETEXTDESCRIPTOR := NULL;
      /*
       L_PROGRESS := 'ADDRESSLINE01 After Concat';
      --check to transform when trim(NM_STREET_1 || ' ' || NM_STREET_TYPE_1) are null
      IF L_ADR.ADDRESSLINE01 IS NULL THEN
         P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('LINE01 Warning',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
         IF L_ADR.PRIMARYADDRESSABLEOBJECT IS NOT NULL THEN
          L_ADR.ADDRESSLINE01 := L_ADR.PRIMARYADDRESSABLEOBJECT;
           L_ADR.PRIMARYADDRESSABLEOBJECT := NULL; 
             ELSIF L_ADR.PRIMARYADDRESSABLEOBJECT IS NULL THEN 
          L_ADR.ADDRESSLINE01 := L_ADR.SECONDADDRESABLEOBJECT;
          L_ADR.SECONDADDRESABLEOBJECT := NULL;
         END IF;
      END IF;    */
         --  DBMS_OUTPUT.PUT_LINE('output 2');        
        L_PROGRESS := 'ADDRESSLINE01 After Concat';
      IF TRIM(L_ADR.ADDRESSLINE01) IS NULL THEN
      -- P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
         --L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
         --L_REC_WAR := true;
          if TRIM(L_ADR.ADDRESSLINE02) is not null then
          L_ADR.ADDRESSLINE01 := L_ADR.ADDRESSLINE02;
           L_ADR.ADDRESSLINE02 := NULL; 
           P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
           L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
           L_REC_WAR := TRUE;
           --ELSIF L_ADR.ADDRESSLINE02 IS NULL THEN 
           ELSIF TRIM(L_ADR.ADDRESSLINE03) IS NOT NULL THEN 
          L_ADR.ADDRESSLINE01 := L_ADR.ADDRESSLINE03;
          L_ADR.ADDRESSLINE03 := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR := TRUE;
           --ELSIF L_ADR.ADDRESSLINE03 IS NULL THEN 
           ELSIF TRIM(L_ADR.ADDRESSLINE04) IS NOT NULL  THEN 
          L_ADR.ADDRESSLINE01 := L_ADR.ADDRESSLINE04;
          L_ADR.ADDRESSLINE04 := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR := TRUE;
           --ELSIF L_ADR.ADDRESSLINE04 IS NULL  THEN 
          ELSE
          L_ADR.ADDRESSLINE01 := L_ADR.ADDRESSLINE05;
          L_ADR.ADDRESSLINE05 := NULL;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('ADDRESSLINE01 Warning',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
          L_REC_WAR := true;
          
    END IF; 
   END IF; 
        --DBMS_OUTPUT.PUT_LINE('output 3');        
        
        L_PROGRESS := 'Invalid PRIMARYADDRESSABLEOBJECT';
        --Drop data that requires cleansing
        IF L_ADR.PRIMARYADDRESSABLEOBJECT IN ('-','#') THEN 
        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid PRIMARYADDRESSABLEOBJECT',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            END IF;
        
      
      --POSTCODE VALIDATION
       --IF NOT FN_VALIDATE_POSTCODE(UPPER(L_ADR.POSTCODE)) THEN
         --    L_REC_EXC := TRUE;
         --   END IF;
         L_PROGRESS := 'Dropped Invalid Postcode';
         L_POSTCODE := FN_VALIDATE_POSTCODE(UPPER(L_ADR.POSTCODE));
         IF L_POSTCODE = 'INVALID' THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid Postcode',1,100),  L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
             L_REC_EXC := TRUE;
            END IF;
            
      -- Write address record upon change of address
      IF l_prev_cd_address <> t_prop_addr(i).CD_ADDRESS THEN
         l_progress := 'INSERT INTO MO_ADDRESS'; 
         L_NO_ROW_READ_CD_ADR := L_NO_ROW_READ_CD_ADR + 1;
          
          IF L_REC_EXC = TRUE THEN  --using this if statement to drop invalid post codes and they would be sent into the exception table
            IF (   L_NO_ROW_EXP > L_JOB.EXP_TOLERANCE
                   OR L_NO_ROW_ERR > L_JOB.ERR_TOLERANCE
                 OR L_NO_ROW_WAR > L_JOB.WAR_TOLERANCE) 
             -- IF (   L_NO_ROW_EXP > 9999
                 --   OR L_NO_ROW_ERR > 9999
                  --  OR L_NO_ROW_WAR > 9999)  
                THEN     
                    CLOSE cur_prop_addr; 
                    L_JOB.IND_STATUS := 'ERR';
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                    commit;
                    return_code := -1;
                    return;   
                end if;
                
                L_NO_ROW_DROPPED_CD_ADR := L_NO_ROW_DROPPED_CD_ADR + 1;
            ELSE
            --ELSIF (L_REC_WAR = TRUE AND L_NO_ROW_WAR > L_JOB.WAR_TOLERANCE) THEN   
            
             IF (L_REC_WAR = TRUE AND L_NO_ROW_WAR > L_JOB.WAR_TOLERANCE) THEN       
          --IF (L_REC_WAR = TRUE AND L_NO_ROW_WAR > 9999) THEN     
                    CLOSE cur_prop_addr;
                    L_JOB.IND_STATUS := 'ERR';
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Warning tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                    COMMIT;
                    return_code := -1;
                    RETURN;  
                END IF; 
                L_REC_WRITTEN := true;
        
        BEGIN 
          INSERT INTO MO_ADDRESS
          (ADDRESS_PK, UPRN, PAFADDRESSKEY, PROPERTYNUMBERPROPERTY, CUSTOMERNUMBERPROPERTY, 
           UPRNREASONCODE, SECONDADDRESABLEOBJECT, PRIMARYADDRESSABLEOBJECT, ADDRESSLINE01, 
           ADDRESSLINE02, ADDRESSLINE03, ADDRESSLINE04, ADDRESSLINE05, POSTCODE,
           COUNTRY, LOCATIONFREETEXTDESCRIPTOR)
          VALUES
          (L_ADR.ADDRESS_PK, L_ADR.UPRN, L_ADR.PAFADDRESSKEY, L_ADR.PROPERTYNUMBERPROPERTY, L_ADR.CUSTOMERNUMBERPROPERTY, 
           TRIM(L_ADR.UPRNREASONCODE), TRIM(L_ADR.SECONDADDRESABLEOBJECT), TRIM(L_ADR.PRIMARYADDRESSABLEOBJECT), TRIM(L_ADR.ADDRESSLINE01), 
           TRIM(L_ADR.ADDRESSLINE02), TRIM(L_ADR.ADDRESSLINE03), TRIM(L_ADR.ADDRESSLINE04), TRIM(L_ADR.ADDRESSLINE05), TRIM(l_postcode),
           trim(l_adr.COUNTRY), trim(l_adr.LOCATIONFREETEXTDESCRIPTOR));
        EXCEPTION 
        WHEN OTHERS THEN 
             l_no_row_dropped_cd_adr := l_no_row_dropped_cd_adr + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             L_ERROR_MESSAGE := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              l_rec_exc := true; --trap an error from here
        END;
        
        -- keep count of records written
        IF l_rec_written THEN
           l_no_row_insert_cd_adr := l_no_row_insert_cd_adr + 1;
        ELSE 
           -- if tolearance limit has been exceeded, set error message and exit out
           IF (   l_no_row_exp > l_job.EXP_TOLERANCE
               OR l_no_row_err > l_job.ERR_TOLERANCE
               OR l_no_row_war > l_job.WAR_TOLERANCE)   
           THEN
               P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_cd_adr,    'Distinct addresses read');
               P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_cd_adr,    'Distinct addresses inserted');
               P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_cd_adr,    'Distinct addresses dropped');       
               CLOSE cur_prop_addr; 
               l_job.IND_STATUS := 'ERR';
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
               P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
               COMMIT;
               return_code := -1;
               RETURN; 
           END IF;
        END IF;  
end if; -- to close invalid postcodes dropped
        l_prev_cd_address :=  t_prop_addr(i).CD_ADDRESS;
      
      END IF;
         
         if L_REC_EXC = false then  --drop an error from here and prevent it from going forward
              
      -- Write PROPERTY address record upon change of property
  
      IF l_prev_prop <> t_prop_addr(i).no_property THEN
         l_progress := 'INSERT INTO MO_PROPERTY_ADDRESS';     
         l_no_row_read_prop := l_no_row_read_prop + 1;
         l_rec_written := TRUE;

         BEGIN 
            INSERT INTO MO_PROPERTY_ADDRESS 
            (ADDRESSPROPERTY_PK, ADDRESS_PK, STWPROPERTYNUMBER_PK, ADDRESSUSAGEPROPERTY, EFFECTIVEFROMDATE, EFFECTIVETODATE)
            VALUES
            (AddressProperty_PK_SEQ.NEXTVAL, t_prop_addr(i).CD_ADDRESS, t_prop_addr(i).no_property, 'LocatedAt',  sysdate, null);
         EXCEPTION 
         WHEN OTHERS THEN 
             l_no_row_dropped_prop := l_no_row_dropped_prop + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_exp := l_no_row_exp + 1;
         END;

         -- keep count of records written
         IF l_rec_written THEN
            l_no_row_insert_prop := l_no_row_insert_prop + 1;
         ELSE 
           -- if tolearance limit has een exceeded, set error message and exit out
           IF (   l_no_row_exp > l_job.EXP_TOLERANCE
               OR l_no_row_err > l_job.ERR_TOLERANCE
               OR l_no_row_war > l_job.WAR_TOLERANCE)   
           THEN
               P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970,l_no_row_read_prop,    'Distinct properties read');
               P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990,l_no_row_insert_prop,    'Distinct properties inserted');
               P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980,l_no_row_dropped_prop,    'Distinct properties dropped');
               CLOSE cur_prop_addr; 
               l_job.IND_STATUS := 'ERR';
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
               P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
               COMMIT;
               return_code := -1;
               RETURN; 
           END IF;
         END IF;  
    
         -- Write meter address record 
    
         FOR rec_meter IN cur_prop_meter(t_prop_addr(i).no_property)
         LOOP
        
          l_no_row_read_meter := l_no_row_read_meter + 1;
          l_progress := 'INSERT INTO MO_METER_ADDRESS';     
          l_rec_written := TRUE;
          
          BEGIN 
            INSERT INTO MO_METER_ADDRESS
            (ADDRESSPROPERTY_PK, METERSERIALNUMBER_PK, ADDRESS_PK, ADDRESSUSAGEPROPERTY, EFFECTIVEFROMDATE, EFFECTIVETODATE,MANUFACTURER_PK
            , INSTALLEDPROPERTYNUMBER, MANUFCODE)   -- v1.10
            VALUES
            (AddressProperty_PK_SEQ.NEXTVAL, rec_meter.NO_UTL_EQUIP, t_prop_addr(i).CD_ADDRESS, 'SitedAt', sysdate, null,rec_meter.MANUFACTURER_PK
            , t_prop_addr(i).no_property, rec_meter.MANUFCODE);   --v1.10
          EXCEPTION 
          WHEN OTHERS THEN 
             l_no_row_dropped_meter := l_no_row_dropped_meter + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_exp := l_no_row_exp + 1;
          END;
          
          -- keep count of records written
          IF l_rec_written THEN
             l_no_row_insert_meter := l_no_row_insert_meter + 1;
          ELSE 
             -- if tolearance limit has een exceeded, set error message and exit out
             IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)   
             THEN
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_meter,    'Distinct meters read');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_meter,    'Distinct meters inserted');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_meter,    'Distinct meters dropped');
                 CLOSE cur_prop_addr; 
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN; 
             END IF;
          END IF;  
          
         END LOOP;

         -- Write customer address record 
    
         FOR rec_cus IN cur_cus_addr(t_prop_addr(i).no_property)
         LOOP
        
          l_no_row_read_cust := l_no_row_read_cust + 1;
          l_progress := 'INSERT INTO MO_CUST_ADDRESS';     
          L_REC_WRITTEN := TRUE;
          --added STWPROPERTYNUMBER_PK to identify a specific address for a customer at a property
          --to ensure that only one address
          BEGIN 
             INSERT INTO MO_CUST_ADDRESS
             
             (ADDRESSPROPERTY_PK, ADDRESS_PK, CUSTOMERNUMBER_PK, ADDRESSUSAGEPROPERTY, EFFECTIVEFROMDATE, EFFECTIVETODATE,STWPROPERTYNUMBER_PK)
             VALUES
             (AddressProperty_PK_SEQ.NEXTVAL, t_prop_addr(i).CD_ADDRESS, rec_cus.CUSTOMERNUMBER_PK, 'BilledAt', sysdate, null,rec_cus.STWPROPERTYNUMBER_PK);
          EXCEPTION 
          WHEN OTHERS THEN 
               l_no_row_dropped_cust := l_no_row_dropped_cust + 1;
               l_rec_written := FALSE;
               l_error_number := SQLCODE;
               l_error_message := SQLERRM;
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_exp := l_no_row_exp + 1;
          END;
              
          -- keep count of records written
          IF l_rec_written THEN
             l_no_row_insert_cust := l_no_row_insert_cust + 1;
          ELSE 
             -- if tolearance limit has een exceeded, set error message and exit out
             IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)   
             THEN
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_cust,    'Distinct customers read');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_cust,    'Distinct customers inserted');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_cust,    'Distinct customers dropped');
                 CLOSE cur_prop_addr; 
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN; 
             END IF;
          END IF;  
        END LOOP;
end if; --closing the if statement [drop an error from here and prevent it from going forward]
        l_prev_prop :=  t_prop_addr(i).no_property;

      END IF;

    END LOOP;
      
    IF t_prop_addr.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  
  CLOSE cur_prop_addr; 


  l_progress := 'Writing Counts';
  --  the recon key numbers used will be specific to each procedure
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970,l_no_row_read_prop,    'Distinct properties read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990,l_no_row_insert_prop,    'Distinct properties inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980,l_no_row_dropped_prop,    'Distinct properties dropped');

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_cd_adr,    'Distinct addresses read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_cd_adr,    'Distinct addresses inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_cd_adr,    'Distinct addresses dropped');

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_cust,    'Distinct customers read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_cust,    'Distinct customers inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_cust,    'Distinct customers dropped');

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 970, l_no_row_read_meter,    'Distinct meters read');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 990, l_no_row_insert_meter,    'Distinct meters inserted');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP28', 980, l_no_row_dropped_meter,    'Distinct meters dropped');

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);  
     
  l_progress := 'End';  
  
  COMMIT;  
   
EXCEPTION
WHEN OTHERS THEN    
     l_error_number := SQLCODE;
     L_ERROR_MESSAGE := SQLERRM;
       P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     --P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
       P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));    
     --P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));    
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     RETURN_CODE := -1;
END P_MOU_TRAN_ADDRESS ;
/
show error;

--CREATE OR REPLACE PUBLIC SYNONYM P_MOU_TRAN_ADDRESS  FOR P_MOU_TRAN_ADDRESS ;

exit;