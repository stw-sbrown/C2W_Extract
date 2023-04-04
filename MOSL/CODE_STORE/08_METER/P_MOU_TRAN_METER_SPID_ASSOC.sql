create or replace
PROCEDURE           P_MOU_TRAN_METER_SPID_ASSOC (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO_METER_SPID_ASSOC
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_SPID_ASSOC.sql
--
-- Subversion $Revision: 6067 $
--
-- CREATED        : 21/04/2016
--
-- DESCRIPTION    : Procedure to populate the MO_METER_SPID_ASSOC table
--                 Will read from key gen meter records and output all associated SPIDs for each logical meter
--
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     CR/DEF    Description
-- ---------   ----------  -------    ------    ---------------------------------------------
-- v 3.09      01/11/2016  D.Cheung             I-352 - For Aggregates - set property as master
-- v 3.08      19/08/2016  D.Cheung             I-346 - Getting 2 SPIDs for 18 meters - MOSL Test 3 rejections
-- v 3.07      16/08/2016  D.Cheung             I-341 - Duplicate constraint error in DEL_METER_SUPPLY_POINT - Meter associated to multiple SPIDs
-- v 3.06      15/08/2016  D.Cheung             I-333 - Remove spid join to MO_METER
-- v 3.05      11/08/2016  S.Badhan             I-333 - Join meter spid to sub meter spid to get sub meters
-- v 3.04      05/08/2016  D.Cheung             I-299 - change logic to get correct corespids for network subs-meters
-- V 3.03      19/07/2016  D.Cheung             I-302 - Add TE Meter Associations
-- V 3.02      12/07/2016  D.Cheung   CR_021    Change to muse MASTER property SPID for Aggregate properties
-- V 3.01      26/05/2016  D.Cheung   D_51      MOSL TEST1 Defect m1 - Remove spaces from ManufacturerSerialNum
-- V 2.02      25/05/2016  D.Cheung             Change for TE - only get POTABLE meters
-- V 2.01      23/05/2016  D.Cheung   CR_014    Add MANUFCODE field
-- V 1.01      10/05/2016  D.Cheung             Link back to MO_METER (on meterref)
-- V 0.02      05/05/2016  K.Burton             REOPENED Issue I-118 - removed link to BT_METER_SPID table from main cursor
--                                              SPIDs now retrieved from LU_SPID_RANGE directly for W service category
-- V 0.01      21/04/2016  D.Cheung   CR_03     Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_SPID_ASSOC';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    MO_METER.METERREF%TYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mo                          MO_METER_SPID_ASSOC%ROWTYPE;          --****TODO - ADD OUTPUT TABLE TYPE
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;

CURSOR cur_met (p_no_equipment_start   BT_TVP163.NO_EQUIPMENT%TYPE,
                 p_no_equipment_end     BT_TVP163.NO_EQUIPMENT%TYPE)
    IS
--****TODO - ADD MAIN CURSOR QUERY
    SELECT DISTINCT  
      NO_EQUIPMENT
      ,NO_PROPERTY
      ,NM_PREFERRED
      ,NO_UTL_EQUIP
      ,SPID_PK
      ,MANUFCODE
FROM (
        SELECT /*+ FULL(TV163) FULL(TV054_N) FULL(TV054) PARALLEL(TV163,12) PARALLEL(TV054_N,12)  PARALLEL(LSR,12) PARALLEL(MM,12) */
--        DISTINCT
              TV163.NO_EQUIPMENT
              ,CASE WHEN TRIM(TV163.AGG_NET) IN ('N','M') THEN TV163.NO_PROPERTY ELSE NVL(TV163.NO_PROPERTY_MASTER,TV163.NO_PROPERTY) END NO_PROPERTY
              ,MM.MANUFACTURER_PK   NM_PREFERRED     --V3.01
              ,MM.MANUFACTURERSERIALNUM_PK  NO_UTL_EQUIP     --V3.01
--              ,LSR.SPID_PK -- V0.02
              ,CASE WHEN TRIM(TV163.AGG_NET) IN ('N','M') THEN LSR_N.SPID_PK ELSE LSR.SPID_PK END SPID_PK -- V 3.04
              ,MM.MANUFCODE --v2.01
        FROM   BT_TVP163 TV163
-- GET METER DETAILS IF IT IS A NETWORK OR MIXED SUB METER      
        LEFT JOIN BT_TVP054 TV054_N ON (TV163.NO_PROPERTY = TV054_N.NO_PROPERTY) --CR_021
        LEFT JOIN BT_TVP054 TV054 ON (NVL(TV163.NO_PROPERTY_MASTER,TV163.NO_PROPERTY) = TV054.NO_PROPERTY) --CR_021
        LEFT JOIN LU_SPID_RANGE LSR ON (LSR.CORESPID_PK = TV054.CORESPID
          AND LSR.SERVICECATEGORY = 'W')
        LEFT JOIN LU_SPID_RANGE LSR_N ON (LSR_N.CORESPID_PK = TV054_N.CORESPID
          AND LSR_N.SERVICECATEGORY = 'W')
        JOIN MO_METER MM ON (MM.METERREF = TV163.NO_EQUIPMENT 
          AND MM.NONMARKETMETERFLAG = 0
          AND ((TRIM(TV163.AGG_NET) IN ('N','M') AND MM.SPID_PK = LSR_N.SPID_PK) OR (TRIM(TV163.AGG_NET) NOT IN ('N','M') AND MM.SPID_PK = LSR.SPID_PK))  --v3.06 - v3.08
          AND MM.METERTREATMENT = 'POTABLE')  --v2.02
      WHERE  TV163.NO_EQUIPMENT BETWEEN p_no_equipment_start AND p_no_equipment_end
--      WHERE  TV163.NO_EQUIPMENT BETWEEN 1 AND 999999999
          AND TV163.FG_ADD_SUBTRACT = '+'     --v3.07
          AND TV163.CD_SERVICE_PROV = 'W'     --v3.07
          AND TV163.DT_END_054 IS NULL        --v3.07
      UNION
      SELECT /*+ FULL(MM) */
          MM_TE.METERREF NO_EQUIPMENT
          ,MM_TE.INSTALLEDPROPERTYNUMBER NO_PROPERTY
          ,TRIM(MM_TE.MANUFACTURER_PK)   NM_PREFERRED     
          ,TRIM(MM_TE.MANUFACTURERSERIALNUM_PK)  NO_UTL_EQUIP    
          ,TRIM(MM_TE.SPID_PK) SPID_PK
          ,TRIM(MM_TE.MANUFCODE) MANUFCODE
      FROM MO_METER MM_TE
      WHERE MM_TE.NONMARKETMETERFLAG = 0
          AND MM_TE.METERTREATMENT IN ('PRIVATETE','PRIVATEWATER')
    )
    WHERE SPID_PK IS NOT NULL
    ORDER BY NO_EQUIPMENT;

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

      l_err.TXT_KEY := t_met(i).NO_EQUIPMENT;
      l_mo := NULL;
      l_rec_exc := FALSE;
      l_rec_written := TRUE;    -- set default record status to write

      -- keep count of distinct meters
      l_no_row_read := l_no_row_read + 1;

--****TODO - ADD TRANSFORM RULES
--      l_progress := 'GET CORRECT SPID';
--      IF (t_met(i).WATERSPID IS NULL AND t_met(i).SEWAGESPID IS NOT NULL) THEN
--          -- ONLY Sewage meter
--          l_mo.SPID := t_met(i).SEWAGESPID;
--      ELSIF (t_met(i).WATERSPID IS NOT NULL) THEN
--          -- Water meter
--          l_mo.SPID := t_met(i).WATERSPID;
--      ELSE
--          --both water and sewage SPIDs null - raise exception
--          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO Supply Point found',1,100),  t_met(i).NO_PROPERTY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--          L_REC_EXC := true;
--      END IF;

      -- ***** EXCEPTION HANDLING
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
              -- IF ALL CONDITIONS MET - WRITE ROLL-OVER RECORD TO MO_METER_SPID_ASSOC TABLE
--****TODO - ADD INSERT
              INSERT INTO MO_METER_SPID_ASSOC
                (METERREF, STWPROPERTYNUMBER_PK, MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, SPID
                , MANUFCODE)  --v2.01
              VALUES
                (TRIM(t_met(i).NO_EQUIPMENT), TRIM(t_met(i).NO_PROPERTY), TRIM(t_met(i).NM_PREFERRED), TRIM(t_met(i).NO_UTL_EQUIP), TRIM(t_met(i).SPID_PK)
                , TRIM(t_met(i).MANUFCODE));  --v2.01
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
      l_prev_met := t_met(i).NO_EQUIPMENT;

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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP48', 2510, l_no_row_read,    'Distinct Eligible meters reads during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP48', 2520, l_no_row_dropped, 'Eligible property meters dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP48', 2530, l_no_row_insert,  'Eligible Property meters written to MO_METER_SPID_ASSOC during Transform');

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
END P_MOU_TRAN_METER_SPID_ASSOC;
/
/
show errors;
exit;