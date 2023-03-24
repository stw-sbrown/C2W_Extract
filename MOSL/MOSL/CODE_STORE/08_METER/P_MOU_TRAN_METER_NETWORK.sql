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
-- Subversion $Revision: 4039 $
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
-- Version     Date        Author   CP/DEF    Description
-- ---------   ----------  -------  ------    ---------------------------------------------
-- V 2.02      23/05/2016  D.Cheung CR_014    Add MANUFCODE
--                                            Add STWPROPERTYNUMBER
-- V 2.01      20/05/2016  D.Cheung           Change extract criteria to join main and sub on logical property
-- V 1.01      10/05/2016  D.Cheung           Link back to MO_METER (on meterref)
-- V 0.02      05/05/2016  K.Burton           REOPENED Issue I-118 - removed link to BT_METER_SPID table from main cursor
--                                            SPIDs now retrieved from LU_SPID_RANGE directly for W service category
-- V 0.01      21/04/2016  D.Cheung           Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_NETWORK';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    BT_TVP163.NO_EQUIPMENT%TYPE;
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
  l_main_metertreatment         MO_METER.METERTREATMENT%TYPE;
  l_sub_metertreatment          MO_METER.METERTREATMENT%TYPE;

CURSOR cur_met (p_no_equipment_start   BT_TVP163.NO_EQUIPMENT%TYPE,
                 p_no_equipment_end     BT_TVP163.NO_EQUIPMENT%TYPE)
    IS
--***TODO - VERIFY MAIN QUERY
--      SELECT * FROM
--      (
          SELECT /*+ PARALLEL(T063M,12) PARALLEL(T036M,12) PARALLEL(T063S,12) PARALLEL(T036S,12) */
          DISTINCT TV163M.NO_EQUIPMENT MAIN_METERREF
              ,TV163M.NO_PROPERTY MAIN_STWPROPERTYNUMBER_PK
              ,TRIM(T036M.NM_PREFERRED) MAIN_MANUFACTURER_PK
              ,TRIM(T063M.NO_UTL_EQUIP) MAIN_MANSERIALNUM_PK
              ,TRIM(LSRM.SPID_PK) MAIN_SPID_PK -- V 0.02
--              ,BTM.SUPPLY_POINT_CODE MAIN_SUPPLY_POINT_CODE -- V 0.02
              ,MMM.MANUFCODE AS MAIN_MANUFCODE    --v2.01
              ,TV163S.NO_EQUIPMENT SUB_METERREF
              ,TV163S.NO_PROPERTY SUB_STWPROPERTYNUMBER_PK
              ,TRIM(T036S.NM_PREFERRED) SUB_MANUFACTURER_PK
              ,TRIM(T063S.NO_UTL_EQUIP) SUB_MANSERIALNUM_PK
              ,TRIM(LSRS.SPID_PK) SUB_SPID_PK -- V 0.02
--              ,BTS.SUPPLY_POINT_CODE SUB_SUPPLY_POINT_CODE -- V 0.02
              ,MMS.MANUFCODE AS SUB_MANUFCODE    --v2.01
          FROM BT_TVP163 TV163M
          JOIN CIS.TVP063EQUIPMENT T063M ON (T063M.NO_EQUIPMENT = TV163M.NO_EQUIPMENT
              AND T063M.CD_COMPANY_SYSTEM = TV163M.CD_COMPANY_SYSTEM)
          JOIN CIS.TVP036LEGALENTITY T036M ON T036M.NO_LEGAL_ENTITY = T063M.NO_BUSINESS
-- ****** V 0.02 - REOPENED Issue I-118 removed link to BT_METER_SPID and replaced with LU_SPID_RANGE for water meters only        
--          JOIN BT_METER_SPID BTM ON (BTM.CORESPID = TV163M.CORESPID
--              AND BTM.NO_PROPERTY = TV163M.NO_PROPERTY)
          JOIN LU_SPID_RANGE LSRM ON (LSRM.CORESPID_PK = TV163M.CORESPID
              AND LSRM.SERVICECATEGORY = 'W')
          JOIN MO_METER MMM ON (MMM.METERREF = TV163M.NO_EQUIPMENT AND MMM.NONMARKETMETERFLAG = 0)  --*** V1.01 - linked back to MO_METER - to prevent FK failures
          JOIN BT_TVP163 TV163S ON (
              TV163S.NO_PROPERTY = TV163M.NO_PROPERTY  --**** v2.01
              --TV163S.NO_PROPERTY_INST = TV163M.NO_PROPERTY  --**** v2.01
              --AND TV163S.NO_EQUIPMENT = TV163M.NO_EQUIPMENT   --**** v2.01
              AND TV163S.FG_ADD_SUBTRACT = '-')
          JOIN CIS.TVP063EQUIPMENT T063S ON (T063S.NO_EQUIPMENT = TV163S.NO_EQUIPMENT
              AND T063S.CD_COMPANY_SYSTEM = TV163S.CD_COMPANY_SYSTEM)
          JOIN CIS.TVP036LEGALENTITY T036S ON T036S.NO_LEGAL_ENTITY = T063S.NO_BUSINESS
-- ****** V 0.02 - REOPENED Issue I-118 removed link to BT_METER_SPID and replaced with LU_SPID_RANGE for water meters only        
--          JOIN BT_METER_SPID BTS ON (BTS.CORESPID = TV163S.CORESPID
--              AND BTS.NO_PROPERTY = TV163S.NO_PROPERTY)
          JOIN LU_SPID_RANGE LSRS ON (LSRS.CORESPID_PK = TV163S.CORESPID
              AND LSRS.SERVICECATEGORY = 'W')
          JOIN MO_METER MMS ON (MMS.METERREF = TV163S.NO_EQUIPMENT AND MMS.NONMARKETMETERFLAG = 0) --*** V1.01 - linked back to MO_METER - to prevent FK failures
          WHERE  TV163M.NO_EQUIPMENT BETWEEN p_no_equipment_start AND p_no_equipment_end
          --WHERE  TV163M.NO_EQUIPMENT BETWEEN 1 AND 999999999
              AND TV163M.IND_MARKET_PROP_INST = 'Y'
              AND TV163M.FG_ADD_SUBTRACT = '+'  --**** v2.01              
              --AND TV163S.NO_PROPERTY = 202002246
-- ****** V 0.02 - REOPENED Issue I-118 removed link to BT_METER_SPID and replaced with LU_SPID_RANGE for water meters only        
--      )
--      PIVOT (
--          MAX(MAIN_SPID_PK)
--              FOR MAIN_SUPPLY_POINT_CODE
--              IN ('W' MAIN_SPID_W, 'S' MAIN_SPID_S)
--      )
--      PIVOT (
--          MAX(SUB_SPID_PK)
--              FOR SUB_SUPPLY_POINT_CODE
--              IN ('W' SUB_SPID_W, 'S' SUB_SPID_S));

      ORDER BY MAIN_STWPROPERTYNUMBER_PK, SUB_STWPROPERTYNUMBER_PK;
      --ORDER BY MAIN_METERREF, SUB_METERREF;

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

  -- start processing all records for range supplied

  OPEN cur_met (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing ';

  LOOP

    FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_met.COUNT
    LOOP

      l_err.TXT_KEY := t_met(i).SUB_STWPROPERTYNUMBER_PK;
      l_mo := NULL;
      l_rec_exc := FALSE;
      l_rec_written := TRUE;    -- set default record status to write
--      l_main_metertreatment := NULL;
--      l_sub_metertreatment := NULL;
      l_main_metertreatment := 'POTABLE'; -- we only have water meters
      l_sub_metertreatment := 'POTABLE'; -- we only have water meters

      -- keep count of distinct meter readings
      l_no_row_read := l_no_row_read + 1;

--***TODO - ADD transform rules
--      L_PROGRESS := 'CHECKING MAIN SPID';
      --GET CORRECT SPID FOR MAIN METER
--      IF (t_met(i).MAIN_SPID_W IS NULL AND t_met(i).MAIN_SPID_S IS NOT NULL) THEN
--          -- ONLY Sewage meter
--          l_mo.MAIN_SPID := t_met(i).MAIN_SPID_S;
--          l_main_metertreatment := 'SEWERAGE';
--      ELSIF (t_met(i).MAIN_SPID_W IS NOT NULL) THEN
--          -- Water meter
--          l_mo.MAIN_SPID := t_met(i).MAIN_SPID_W;
--          l_main_metertreatment := 'POTABLE';
--      ELSE
--          --both water and sewage MAIN SPIDs null - raise exception
--          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO Supply Point found for MAIN Meter',1,100),  t_met(i).MAIN_STWPROPERTYNUMBER_PK, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--          L_REC_EXC := true;
--      END IF;

--      L_PROGRESS := 'CHECKING SUB SPID';
      --GET CORRECT SPID FOR SUB METER
--      IF (t_met(i).SUB_SPID_W IS NULL AND t_met(i).SUB_SPID_S IS NOT NULL) THEN
--          -- ONLY Sewage meter
--          l_mo.SUB_SPID := t_met(i).SUB_SPID_S;
--          l_sub_metertreatment := 'SEWERAGE';
--      ELSIF (t_met(i).MAIN_SPID_W IS NOT NULL) THEN
--          -- Water meter
--          l_mo.SUB_SPID := t_met(i).SUB_SPID_W;
--          l_sub_metertreatment := 'POTABLE';
--      ELSE
--          --both water and sewage SUB SPIDs null - raise exception
--          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO Supply Point found for SUB Meter',1,100),  t_met(i).SUB_STWPROPERTYNUMBER_PK, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--          L_REC_EXC := true;
--      END IF;

      L_PROGRESS := 'VALIDATE MAIN AGAINST SUB METERTREATMENT';
      IF (l_main_metertreatment <>  l_sub_metertreatment) THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('MAIN and SUB SPID types mismatch',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      ELSIF (l_main_metertreatment = 'SEWERAGE' AND L_MO.MAIN_SPID <> L_MO.SUB_SPID) THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Sewerage SPID mismatch',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      END IF;

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
                (MAIN_METERREF, MAIN_STWPROPERTYNUMBER_PK, MAIN_MANUFACTURER_PK, MAIN_MANSERIALNUM_PK, MAIN_SPID
                , MAIN_MANUFCODE  --v2.01
                , SUB_METERREF, SUB_STWPROPERTYNUMBER_PK, SUB_MANUFACTURER_PK, SUB_MANSERIALNUM_PK, SUB_SPID
                , SUB_MANUFCODE)   --v2.01
              VALUES
                (T_MET(I).MAIN_METERREF, T_MET(I).MAIN_STWPROPERTYNUMBER_PK, T_MET(I).MAIN_MANUFACTURER_PK, T_MET(I).MAIN_MANSERIALNUM_PK, t_met(i).MAIN_SPID_PK
                , t_met(i).MAIN_MANUFCODE   --v2.01
                , t_met(i).SUB_METERREF, t_met(i).SUB_STWPROPERTYNUMBER_PK, t_met(i).SUB_MANUFACTURER_PK, t_met(i).SUB_MANSERIALNUM_PK, t_met(i).SUB_SPID_PK
                , t_met(i).SUB_MANUFCODE);
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