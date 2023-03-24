create or replace
PROCEDURE           P_MOU_TRAN_METER_NETWORK (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO_METER_NETWORK
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_NETWORK.sql
--
-- Subversion $Revision: 5978 $
--
-- CREATED        : 21/04/2016
--
-- DESCRIPTION    : Procedure to transform and populate the MO_METER_NETWORK table
--                 Will read from key gen meter records, perform transformations
--                 and write to MO_METER_NETWORK
--
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     CP/DEF    Description
-- ---------   ----------  -------    ------    ---------------------------------------------
-- V 4.10      26/10/2016  K.Burton             Drop non-market main meter for level two
-- V 4.09      19/09/2016  D.Cheung             I-361 - Change from MOSL - exclude if ANY main spid is null, including level two, non-market scenario
-- V 4.08      05/09/2016  D.Cheung             I-351 - Not evaluating SPIDs for NONMARKET meters correctly
-- V 4.07      03/08/2016  D.Cheung             I-328 - Duplicate main - sub mappings caused by multiple sub-spids in cursor
-- V 4.06      21/07/2016  D.Cheung             I-299 - Changes to AGG_NET in preparation for TARGET changes
-- V 4.05      13/07/2016  D.Cheung             I-293 - Need checks for Invalid NULL SPIDS on marketable non-master MAIN and SUB meters
-- V 4.04      08/07/2016  D.Cheung             I-281 - Fixed bug on check for duplicate spids in network
-- V 4.03      07/07/2016  D.Cheung   CR_021    Changes to Transform rules logic
-- V 4.02      06/07/2016  D.Cheung   CR_021    Fix Duplicate SPIDs check issues
-- V 4.01      01/07/2016  D.Cheung   CR_021    REBUILD using new AGGREGATE PROPERTIES KEYGEN changes
-- V 3.01      26/05/2016  D.Cheung   D_51      MOSL TEST1 Defect m1 - Remove spaces from ManufacturerSerialNum
-- V 2.02      23/05/2016  D.Cheung   CR_014    Add MANUFCODE
--                                              Add STWPROPERTYNUMBER
-- V 2.01      20/05/2016  D.Cheung             Change extract criteria to join main and sub on logical property
-- V 1.01      10/05/2016  D.Cheung             Link back to MO_METER (on meterref)
-- V 0.02      05/05/2016  K.Burton             REOPENED Issue I-118 - removed link to BT_METER_SPID table from main cursor
--                                              SPIDs now retrieved from LU_SPID_RANGE directly for W service category
-- V 0.01      21/04/2016  D.Cheung             Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_NETWORK';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    BT_TVP163.NO_PROPERTY%TYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mo                          MO_METER_NETWORK%ROWTYPE;           --***TODO - ADD OUTPUT TABLE TYPE
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  
  l_more_levels                 BOOLEAN;
  l_level_count                 NUMBER;
  l_before_record_count         NUMBER;
  l_after_record_count          NUMBER;
  
  l_dup_spid_count              NUMBER;
  l_prev_master                 BT_METER_NETWORK.MASTER_PROPERTY%TYPE;
  l_master_dropped              BOOLEAN;
  l_master_written              BOOLEAN;

CURSOR cur_met (p_no_equipment_start   BT_TVP163.NO_EQUIPMENT%TYPE,
                 p_no_equipment_end     BT_TVP163.NO_EQUIPMENT%TYPE)
    IS
--***TODO - VERIFY MAIN QUERY
         SELECT /*+ PARALLEL(BT_MN,12) PARALLEL(MM_M,12) PARALLEL(MM_S,12) */
                DISTINCT 
                BT_MN.MAIN_STWMETERREF          MAIN_METERREF
                ,BT_MN.MAIN_STWPROPERTYNUMBER   MAIN_STWPROPERTYNUMBER_PK
                ,MM_M.MANUFACTURER_PK           MAIN_MANUFACTURER_PK             
                ,MM_M.MANUFACTURERSERIALNUM_PK  MAIN_MANSERIALNUM_PK    
                , CASE WHEN TRIM(BT_MN.FG_NMM) = 'Y' THEN NULL ELSE TRIM(LSRM.SPID_PK) END AS MAIN_SPID_PK   --v4.08
--                ,TRIM(LSRM.SPID_PK)             MAIN_SPID_PK
                ,MM_M.MANUFCODE                 MAIN_MANUFCODE
                ,MM_M.METERTREATMENT            MAIN_METERTREATMENT
                ,BT_MN.SUB_STWMETERREF          SUB_METERREF
                ,BT_MN.SUB_STWPROPERTYNUMBER    SUB_STWPROPERTYNUMBER_PK
                ,MM_S.MANUFACTURER_PK           SUB_MANUFACTURER_PK 
                ,MM_S.MANUFACTURERSERIALNUM_PK  SUB_MANSERIALNUM_PK
                , CASE WHEN MM_S.NONMARKETMETERFLAG = 1 THEN NULL ELSE TRIM(LSRS.SPID_PK) END AS SUB_SPID_PK   --v4.08
--                ,TRIM(LSRS.SPID_PK)             SUB_SPID_PK
                ,MM_S.MANUFCODE                 SUB_MANUFCODE
                ,MM_S.METERTREATMENT            SUB_METERTREATMENT
                ,BT_MN.FG_NMM                   MAIN_NMM
                ,BT_MN.NET_LEVEL
                ,BT_MN.FG_ADD_SUBTRACT
                ,BT_MN.MASTER_PROPERTY
                ,0 AGG_NET_FLAG
                ,MM_S.NONMARKETMETERFLAG        SUB_NMM
            FROM BT_METER_NETWORK BT_MN
                LEFT JOIN LU_SPID_RANGE LSRM ON (LSRM.CORESPID_PK = BT_MN.CORESPID
                    AND LSRM.SERVICECATEGORY = 'W')
                LEFT JOIN MO_METER MM_M ON (MM_M.METERREF = BT_MN.MAIN_STWMETERREF)
                LEFT JOIN MO_METER MM_S ON (MM_S.METERREF = BT_MN.SUB_STWMETERREF)
                LEFT JOIN BT_TVP163 BT163 ON (BT163.NO_EQUIPMENT = BT_MN.SUB_STWMETERREF 
                    AND BT163.NO_PROPERTY_INST = BT163.NO_PROPERTY 
                    AND BT163.NO_PROPERTY_MASTER <> BT163.NO_PROPERTY
                    AND BT163.FG_ADD_SUBTRACT = '+' 
                    AND BT163.DT_END_054 IS NULL)
                LEFT JOIN LU_SPID_RANGE LSRS ON (LSRS.CORESPID_PK = BT163.CORESPID
                    AND LSRS.SERVICECATEGORY = 'W')
            WHERE  BT_MN.SUB_STWMETERREF BETWEEN p_no_equipment_start AND p_no_equipment_end
            --WHERE  BT_MN.SUB_STWMETERREF BETWEEN 1 AND 999999999
                AND BT_MN.FG_ADD_SUBTRACT = '-'
--                AND BT_MN.SUB_STWPROPERTYNUMBER IN 
--                    (SELECT BT_S.SUB_STWPROPERTYNUMBER FROM BT_METER_NETWORK BT_S 
--                    WHERE BT_MN.MASTER_PROPERTY = BT_S.MASTER_PROPERTY AND BT_S.FG_ADD_SUBTRACT = '+')
                --AND BT_MN.MAIN_STWPROPERTYNUMBER IN (53002025,150002012,581002168,291002029,570002065,32002038) --TESTING ONLY
            ORDER BY BT_MN.MASTER_PROPERTY, BT_MN.NET_LEVEL, BT_MN.MAIN_STWMETERREF, BT_MN.SUB_STWMETERREF
            ;

TYPE tab_meter IS TABLE OF cur_met%ROWTYPE INDEX BY PLS_INTEGER;
t_met  tab_meter;

BEGIN

   -- initial variables

   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_prev_met := 0;
   
   l_more_levels := TRUE;
   l_level_count := 1;
   
   l_prev_master := 0;
   l_master_dropped := false;
   l_master_written := false;
   l_mo := NULL;
   l_job.IND_STATUS := 'RUN';

   -- get job no and start job

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

   l_progress := 'processing ';

   -- any errors set return code and exit out

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;
   
-- BUILD BT_METER_NETWORK TABLE
     --*** LEVEL 1
    BEGIN
        L_PROGRESS := 'TRUNCATE BT_METER_NETWORK TABLE';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_METER_NETWORK';

        L_PROGRESS := 'GET TOP LEVEL NETWORK MASTER HEADS';
        INSERT /*+ append */
            INTO BT_METER_NETWORK        
        SELECT DISTINCT
            TV163.NO_PROPERTY AS MAIN_STWPROPERTYNUMBER
            , TV163M.NO_EQUIPMENT AS MAIN_STWMETERREF
            , TV163.NO_PROPERTY_INST AS SUB_STWPROPERTYNUMBER
            , TV163.NO_EQUIPMENT AS SUB_STWMETERREF
            , TV163.FG_ADD_SUBTRACT 
            , TV163.CORESPID
            , TV163.FG_NMM
            , TV163.NO_PROPERTY_MASTER AS MASTER_PROPERTY
            , l_level_count NET_LEVEL
        FROM BT_TVP163 TV163
            LEFT JOIN BT_TVP163 TV163M ON TV163M.NO_PROPERTY_INST = TV163.NO_PROPERTY
--        WHERE TV163.AGG_NET = 'Y'
        WHERE TV163.NO_PROPERTY_MASTER IS NOT NULL
            AND TV163.DT_END_054 IS NULL
            AND TV163.CD_SERVICE_PROV = 'W'
            AND TV163.NO_PROPERTY IN (SELECT DISTINCT NO_PROPERTY_MASTER FROM BT_TVP163 WHERE NO_PROPERTY_MASTER IS NOT NULL)
        --ORDER BY TV163.NO_PROPERTY, TV163.NO_PROPERTY_INST;
        ;

        COMMIT;  
    END;

    --*** TREE WALK DOWN HIEARCHY UNTIL NO MORE LEVELS
    L_PROGRESS := 'LOOP THROUGH HIERARCHY LEVELS';
    WHILE l_more_levels = TRUE
    LOOP
            
        L_PROGRESS := 'GET CURRENT RECORDS COUNT';
        BEGIN
            SELECT COUNT(*)
                INTO   l_before_record_count
            FROM   BT_METER_NETWORK;
        END;    
      
        L_PROGRESS := 'INSERT SUB RECORDS';      
        INSERT /*+ append */
            INTO BT_METER_NETWORK
        SELECT DISTINCT
            TV163.NO_PROPERTY AS MAIN_STWPROPERTYNUMBER
            , TV163M.NO_EQUIPMENT AS MAIN_STWMETERREF
            , TV163.NO_PROPERTY_INST AS SUB_STWPROPERTYNUMBER
            , TV163.NO_EQUIPMENT AS SUB_STWMETERREF
            , TV163.FG_ADD_SUBTRACT 
            , TV163.CORESPID
            , TV163.FG_NMM
            , TV163.NO_PROPERTY_MASTER AS MASTER_PROPERTY
            , l_level_count + 1 AS NET_LEVEL
        FROM BT_TVP163 TV163
            LEFT JOIN BT_TVP163 TV163M ON TV163M.NO_PROPERTY_INST = TV163.NO_PROPERTY
--        WHERE TV163.AGG_NET = 'Y'
        WHERE TV163.NO_PROPERTY_MASTER IS NOT NULL
            AND TV163.DT_END_054 IS NULL
            AND TV163.CD_SERVICE_PROV = 'W'
            AND TV163.NO_PROPERTY IN (SELECT DISTINCT SUB_STWPROPERTYNUMBER FROM BT_METER_NETWORK WHERE NET_LEVEL = l_level_count AND MAIN_STWPROPERTYNUMBER <> SUB_STWPROPERTYNUMBER)
        ;
        COMMIT;
          
        L_PROGRESS := 'GET AFTER RECORDS COUNT';
        BEGIN
            SELECT COUNT(*)
                INTO   l_after_record_count
            FROM   BT_METER_NETWORK;
        END;    
          
        IF l_before_record_count = l_after_record_count THEN
            l_more_levels := FALSE;
        ELSE
            l_level_count := l_level_count + 1;
        END IF;
              
    END LOOP;
      
    l_progress := 'END BT_METER_NETWORK PROCESSING';

    COMMIT;
    

  -- start processing all records for range supplied

  OPEN cur_met (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing ';

  LOOP

    FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_met.COUNT
    LOOP

      l_err.TXT_KEY := t_met(i).SUB_STWPROPERTYNUMBER_PK;
      l_rec_exc := FALSE;
      l_rec_written := TRUE;    -- set default record status to write
      t_met(i).MAIN_METERTREATMENT := 'POTABLE'; -- we only have water meters
      t_met(i).SUB_METERTREATMENT := 'POTABLE'; -- we only have water meters

      -- keep count of distinct sub meters
      l_no_row_read := l_no_row_read + 1;

      L_PROGRESS := 'CHECK IF MASTER_PROPERTY CHANGED';
      IF (t_met(i).MASTER_PROPERTY <> l_prev_master) THEN
          l_prev_master := t_met(i).MASTER_PROPERTY;
          l_master_dropped := false;
          l_master_written := false;
          l_mo := null;
      END IF;
      
      L_PROGRESS := 'CHECK METER IS IN MO_METER';
      IF (t_met(i).MAIN_MANUFACTURER_PK IS NULL) THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('MAIN METER NOT IN MO_METER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      ELSIF (t_met(i).SUB_MANUFACTURER_PK IS NULL) THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('SUB METER NOT IN MO_METER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      END IF;

    
      IF (t_met(i).NET_LEVEL = 1) THEN
          L_PROGRESS := 'TRANSFORM PSEUDO AGG NETS-GET FIRST AVAILABLE MAIN';
          IF (t_met(i).MAIN_METERREF IS NULL) THEN
--***** TOO - ADD TRANSFORM RULES HERE FOR AGGREGATED LOGICAL ONLY MAIN PROPERTY - e.g. NO_PROPERTY = 848002505 *****
              IF (t_met(i).MAIN_MANUFACTURER_PK IS NULL) THEN
                  BEGIN 
                      SELECT METERREF, MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, MANUFCODE, SPID_PK, 1
                          INTO   t_met(i).MAIN_METERREF, t_met(i).MAIN_MANUFACTURER_PK, t_met(i).MAIN_MANSERIALNUM_PK, t_met(i).MAIN_MANUFCODE, t_met(i).MAIN_SPID_PK, l_mo.AGG_NET_FLAG
                      FROM   MO_METER
                      WHERE  METERREF = (
                          SELECT DISTINCT SUB_STWMETERREF FROM (
                              SELECT DISTINCT BT.SUB_STWMETERREF, BT.SUB_STWPROPERTYNUMBER 
                              FROM BT_METER_NETWORK BT
                                  JOIN MO_METER MO ON MO.METERREF = BT.SUB_STWMETERREF
                              WHERE BT.MAIN_STWPROPERTYNUMBER = t_met(i).MAIN_STWPROPERTYNUMBER_PK
                                  AND BT.CORESPID IS NOT NULL
                                  AND BT.FG_NMM = 'N'
                                  AND BT.FG_ADD_SUBTRACT = '+'
                              ORDER BY BT.SUB_STWPROPERTYNUMBER
                          )
                          WHERE ROWNUM = 1
                      )
                      ;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO MAIN METER AVAILABLE FOR AGGREGATED PROPERTY',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                          L_REC_EXC := true;
                          l_master_dropped := true;
                  END;
              ELSE
                  l_mo.AGG_NET_FLAG := t_met(i).AGG_NET_FLAG;
              END IF;
          ELSE
              l_mo.AGG_NET_FLAG := t_met(i).AGG_NET_FLAG;
          END IF;  

--NON-NETWORK RULE CHECKS   

          L_PROGRESS := 'CHECK FOR NULL MAIN SPID - ON MASTER PROPERTY';
          IF (t_met(i).MAIN_SPID_PK IS NULL) THEN
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NON-NETWORK-NULL MASTER SPID',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
              l_master_dropped := true;
          ELSIF (t_met(i).MAIN_NMM = 'Y') THEN
              L_PROGRESS := 'CHECK FOR NON_MARKET - ON MASTER PROPERTY';
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NOT-NETWORK-HEAD MAIN CANNOT BE NON-MARKET',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
              l_master_dropped := true;
          ELSIF (t_met(i).SUB_NMM = 0 AND t_met(i).SUB_SPID_PK IS NULL) THEN
              L_PROGRESS := 'CHECK FOR INVALID NULL SPID - ON LEVEL 1 SUB PROPERTY';
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NULL SPID on Marketable SUB Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
              l_master_dropped := true;
          ELSE
              L_PROGRESS := 'CHECK FOR DUPLICATE SPIDS IN NETWORK';
              BEGIN
                  SELECT MAX(SPID_COUNT)
                      INTO l_dup_spid_count
                  FROM (
                      SELECT BT_MN.CORESPID, COUNT(BT_MN.CORESPID) AS SPID_COUNT
                      FROM BT_METER_NETWORK BT_MN
                      WHERE BT_MN.MASTER_PROPERTY = t_met(i).MASTER_PROPERTY
                          AND (BT_MN.FG_ADD_SUBTRACT = '+') 
                          AND BT_MN.CORESPID IS NOT NULL
                          AND (
                              (BT_MN.SUB_STWPROPERTYNUMBER IN (SELECT BT_S.SUB_STWPROPERTYNUMBER FROM BT_METER_NETWORK BT_S WHERE BT_MN.MASTER_PROPERTY = BT_S.MASTER_PROPERTY AND BT_S.FG_ADD_SUBTRACT = '-'))
                              OR
                              (BT_MN.MASTER_PROPERTY = BT_MN.SUB_STWPROPERTYNUMBER)
                          )
                      GROUP BY BT_MN.CORESPID
                  );
              EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                      l_dup_spid_count := 0;
              END;
              IF (l_dup_spid_count > 1) THEN
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NON-NETWORK-DUPLICATE SPID AGAINST TWO OR MORE PROPERTIES',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                  L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                  L_REC_EXC := true;
                  l_master_dropped := true;
              END IF;
          END IF;
      ELSE
          L_PROGRESS := 'CHECK IF NEED TO DROP SUBS';           
          IF (l_master_dropped = true OR l_master_written = false) THEN
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NON-NETWORK-DROP SUB-LEVELS FOR DROPPED MASTER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
--V4.09 - change from MOSL drop if sub-level main is nonmarket          
          ELSIF (t_met(i).MAIN_NMM = 'Y') THEN
              L_PROGRESS := 'CHECK FOR NON-MARKET METER - ON MAIN PROPERTY';
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('SUB-LEVEL MAIN CANNOT BE NON_MARKET',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
              l_master_dropped := true;
--v4.09   
          ELSIF (t_met(i).MAIN_SPID_PK IS NULL) THEN
              L_PROGRESS := 'CHECK FOR INVALID NULL SPID - ON MAIN PROPERTY';
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NULL SPID on Marketable MAIN Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
              l_master_dropped := true;
          ELSIF (t_met(i).SUB_NMM = 0 AND t_met(i).SUB_SPID_PK IS NULL) THEN
              L_PROGRESS := 'CHECK FOR INVALID NULL SPID - ON SUB PROPERTY';
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NULL SPID on Marketable SUB Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := true;
              l_master_dropped := true;
          END IF;
      END IF;
      
      --OTHER VALIDATION CHECKS

--      L_PROGRESS := 'VALIDATE MAIN AGAINST SUB METERTREATMENT';
--      IF (t_met(i).MAIN_METERTREATMENT <>  t_met(i).SUB_METERTREATMENT) THEN
--          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('MAIN and SUB SPID types mismatch',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--          L_REC_EXC := true;
--      ELSIF (t_met(i).MAIN_METERTREATMENT = 'SEWERAGE' AND t_met(i).MAIN_SPID_PK <> t_met(i).SUB_SPID_PK) THEN
--          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Sewerage SPID mismatch',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--          L_REC_EXC := true;
--      END IF;

      IF l_rec_exc THEN  --using this if statement to pick up data that passes the biz rules into the table otherwise they will be dropped and would have be sent into the exception table
          IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)
          THEN
              CLOSE cur_met;
              L_JOB.IND_STATUS := 'ERR';
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
              P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
              commit;
              return_code := -1;
              RETURN;
          END IF;
          l_rec_written := FALSE;
      END IF;

      IF l_rec_written THEN
          BEGIN
              l_progress := 'loop processing ABOUT TO INSERT INTO TABLE';
              -- IF ALL CONDITIONS MET - WRITE RECORD TO MO_METER_NETWORK TABLE
--TODO ADD INSERT CODE
              INSERT INTO MO_METER_NETWORK
                (MAIN_METERREF, MAIN_STWPROPERTYNUMBER_PK, MAIN_MANUFACTURER_PK, MAIN_MANSERIALNUM_PK, MAIN_SPID, MAIN_MANUFCODE
                , SUB_METERREF, SUB_STWPROPERTYNUMBER_PK, SUB_MANUFACTURER_PK, SUB_MANSERIALNUM_PK, SUB_SPID, SUB_MANUFCODE
                , MASTER_PROPERTY, AGG_NET_FLAG)
              VALUES
                (t_met(i).MAIN_METERREF, t_met(i).MAIN_STWPROPERTYNUMBER_PK, t_met(i).MAIN_MANUFACTURER_PK, t_met(i).MAIN_MANSERIALNUM_PK, t_met(i).MAIN_SPID_PK, t_met(i).MAIN_MANUFCODE
                , t_met(i).SUB_METERREF, t_met(i).SUB_STWPROPERTYNUMBER_PK, t_met(i).SUB_MANUFACTURER_PK, t_met(i).SUB_MANSERIALNUM_PK, t_met(i).SUB_SPID_PK, t_met(i).SUB_MANUFCODE
                , t_met(i).MASTER_PROPERTY, l_mo.AGG_NET_FLAG)
                ;
          EXCEPTION
          WHEN OTHERS THEN
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;

               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_exp := l_no_row_exp + 1;

               -- if tolearance limit has een exceeded, set error message and exit out
               IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                   OR l_no_row_err > l_job.ERR_TOLERANCE
                   OR l_no_row_war > l_job.WAR_TOLERANCE)
               THEN
                   CLOSE cur_met;
                   l_job.IND_STATUS := 'ERR';
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                   commit;
                   return_code := -1;
                   RETURN;
                END IF;
          END;
      END IF;

        -- keep count of records written
      IF l_rec_written THEN
          l_no_row_insert := l_no_row_insert + 1;
          IF (t_met(i).NET_LEVEL = 1) THEN
              l_master_written := true;
          END IF;
      ELSE
          l_no_row_dropped := l_no_row_dropped + 1;
      END IF;

      -- SET PREVIOUS VALUES FOR NEXT LOOP
      l_prev_met := t_met(i).SUB_STWPROPERTYNUMBER_PK;

    END LOOP;

    IF t_met.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       commit;
    END IF;

  END LOOP;

  CLOSE cur_met;

  -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP49', 2540, l_no_row_read,    'Distinct Eligible meters  reads during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP49', 2550, l_no_row_dropped, 'Eligible property meters dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP49', 2560, l_no_row_insert,  'Eligible Property meters written to MO_METER_NETWORK during Transform');

  --  check counts match (rows read should equal SUM of rows inserted and rows dropped)
  IF l_no_row_read <> l_no_row_insert + l_no_row_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Reconciliation counts do not match',  l_no_row_read || ',' || l_no_row_insert || ',' || l_no_row_dropped, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     commit;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

  l_progress := 'End';

  commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     commit;
     return_code := -1;
END P_MOU_TRAN_METER_NETWORK;
/
/
show errors;
exit;