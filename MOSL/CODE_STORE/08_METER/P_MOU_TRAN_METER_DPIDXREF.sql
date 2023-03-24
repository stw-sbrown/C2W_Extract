create or replace
PROCEDURE           P_MOU_TRAN_METER_DPIDXREF (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO_METER_DPIDXREF
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_DPIDXREF.sql
--
-- Subversion $Revision: 6324 $
--
-- CREATED        : 25/05/2016
--
-- DESCRIPTION    : Procedure to populate the MO_METER_DPIDXREF table
--                 Will read from key gen meter records and output all associated DPIDs for each TE meter
--
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     CR/DEF    Description
-- ---------   ----------  -------    ------    ---------------------------------------------
-- V 0.00      24/05/2016  D.Cheung             Initial Draft
-- V 0.01      19/08/2016  D.Cheung   CR_035    Add Associations for Potable Water Meters
-- V 0.02      30/08/2016  K.Burton    198      SAP Defect 198 - effective date for MDVOL value is
--                                              not passed to SAP. Uncommented previously commented fields to insert
--                                              effective dates from MO_DISCHARGE_POINT
-- V 0.03      05/09/2016  D.Cheung             I-352 - Amend Join on second cursor part to join via meter_spid_assoc (logical property)
-- V 0.04      08/09/2016  D.Cheung             Change MDVOL calculation to use TE field
-- V 0.05      03/11/2016  D.Cheung             Add LU_OTHER_METER_DPID to cursor to force in OTHER METERS
-- V 0.06      08/11/2016  D.Cheung             Set default MDVOL value to 0 - will be updated later by RTS_MDVOL proc where applicable
-- V 0.07      16/11/2016  D.Cheung             Add LU_TE_METER_DPID_EXCLUSION to exclude water meters NOT used for TE calc
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_DPIDXREF';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    MO_METER_DPIDXREF.METERDPIDXREF_PK%TYPE;    --*****TODO - ADD MAIN CURSOR KEY
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mo                          MO_METER_DPIDXREF%ROWTYPE;          --****TODO - ADD OUTPUT TABLE TYPE
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_rec_war                     BOOLEAN;

CURSOR cur_met (p_no_equipment_start   MO_METER.METERREF%TYPE,
                 p_no_equipment_end     MO_METER.METERREF%TYPE)
    IS
--****TODO - ADD MAIN CURSOR QUERY
        SELECT DISTINCT
            NO_EQUIPMENT
            , NM_PREFERRED
            , NO_UTL_EQUIP
            , DPID_PK
            , SPID_PK
            , MDVOL
            , NO_PROPERTY
            , MANUFCODE
            , DPEFFECTFROMDATE
            , EFFECTIVEFROMDATE
            , EFFECTIVETODATE
        FROM (
            SELECT /*+ PARALLEL(BTW,12) PARALLEL(MD,12) PARALLEL(MM,12) */
--            DISTINCT
                  TRIM(BTW.NO_IWCS || BTW.MET_REF) AS NO_EQUIPMENT     
              , TRIM(MM.MANUFACTURER_PK) AS NM_PREFERRED
              , TRIM(MM.MANUFACTURERSERIALNUM_PK) AS NO_UTL_EQUIP
              , TRIM(MD.DPID_PK) AS DPID_PK
              , TRIM(MD.SPID_PK) AS SPID_PK
--              , NVL(BTW.MDVOL_FOR_TE_METER_PERC,0)*100 AS MDVOL   --v0.04
--              , NVL(ABS(BTW.TE),0)*100 AS MDVOL     --v0.04
              , 0 AS MDVOL  --v0.06
              , TRIM(MD.STWPROPERTYNUMBER_PK) AS NO_PROPERTY
              , TRIM(MM.MANUFCODE) AS MANUFCODE
              , MD.DPEFFECTFROMDATE AS DPEFFECTFROMDATE -- V 0.02 
              , MD.EFFECTFROMDATE AS EFFECTIVEFROMDATE -- V 0.02 
              , MD.EFFECTTODATE AS EFFECTIVETODATE -- V 0.02 
            FROM BT_TE_WORKING BTW
            JOIN MO_DISCHARGE_POINT MD ON BTW.NO_IWCS = MD.NO_IWCS
                AND BTW.NO_ACCOUNT = MD.NO_ACCOUNT
            JOIN (SELECT NO_IWCS, MAX(PERIOD) MAXPERIOD FROM BT_TE_WORKING GROUP BY NO_IWCS ) BTWM 
                ON BTW.NO_IWCS = BTWM.NO_IWCS AND BTW.PERIOD = BTWM.MAXPERIOD
            JOIN MO_METER MM ON BTW.NO_IWCS || BTW.MET_REF = MM.METERREF
                AND MM.METERTREATMENT IN ('PRIVATETE', 'PRIVATEWATER')
            WHERE 1=1   -- BTW.NO_IWCS || BTW.MET_REF BETWEEN p_no_equipment_start AND p_no_equipment_end
                AND BTW.TE_CATEGORY IN ('Private TE Meter', 'Private Water Meter')
    --           ORDER BY BTW.NO_IWCS || BTW.MET_REF;
            UNION
            SELECT 
                TRIM(TO_CHAR(NO_EQUIPMENT)) || Row_Nr AS NO_EQUIPMENT
                , NM_PREFERRED
                , NO_UTL_EQUIP
                , DPID_PK
                , SPID_PK
                , MDVOL
                , NO_PROPERTY
                , MANUFCODE
                , DPEFFECTFROMDATE
                , EFFECTIVEFROMDATE
                , EFFECTIVETODATE 
            FROM (
                SELECT  /*+ PARALLEL(MSP,12) PARALLEL(MD,12) PARALLEL(MM,12) PARALLEL(MSC,12) */
--                DISTINCT
                      MM.METERREF AS NO_EQUIPMENT
                      , TRIM(MM.MANUFACTURER_PK) AS NM_PREFERRED
                      , TRIM(MM.MANUFACTURERSERIALNUM_PK) AS NO_UTL_EQUIP
                      , TRIM(MD.DPID_PK) AS DPID_PK
                      , TRIM(MD.SPID_PK) AS SPID_PK
                      , 0 AS MDVOL
                      , TRIM(MD.STWPROPERTYNUMBER_PK) AS NO_PROPERTY
                      , TRIM(MM.MANUFCODE) AS MANUFCODE
                      , MD.DPEFFECTFROMDATE AS DPEFFECTFROMDATE
                      , MD.EFFECTFROMDATE AS EFFECTIVEFROMDATE
                      , MD.EFFECTTODATE AS EFFECTIVETODATE
                      , ROW_NUMBER() OVER (PARTITION BY MM.METERREF ORDER BY TRIM(MD.DPID_PK)) AS Row_Nr
                FROM MO_METER MM
                JOIN MO_METER_SPID_ASSOC MSA ON MSA.METERREF = MM.METERREF              --v0.03
                JOIN MO_SUPPLY_POINT MSP ON SUBSTR(MSA.SPID,1,10) = MSP.CORESPID_PK     --v0.03
                JOIN MO_DISCHARGE_POINT MD ON MD.SPID_PK = MSP.SPID_PK
                WHERE MM.METERREF BETWEEN p_no_equipment_start AND p_no_equipment_end
--                WHERE MM.METERREF BETWEEN 1 AND 999999999
                AND MM.METERTREATMENT IN ('POTABLE')
                AND MSP.SERVICECATEGORY = 'S'
                ORDER BY TRIM(MD.DPID_PK)
                ) x
            UNION
            SELECT 
--            DISTINCT
                TO_CHAR(L1.METERREF) AS NO_EQUIPMENT
                , TRIM(L1.MANUFACTURER_PK) AS NM_PREFERRED
                , TRIM(L1.MANUFACTURERSERIALNUM_PK) AS NO_UTL_EQUIP
                , TRIM(MD.DPID_PK) AS DPID_PK
                , TRIM(MD.SPID_PK) AS SPID_PK
--                , MAX(NVL(ABS(BTW.TE),0)*100) AS MDVOL 
                , 0 AS MDVOL  --v0.06
                , TRIM(MD.STWPROPERTYNUMBER_PK) AS NO_PROPERTY
                , TRIM(MM.MANUFCODE) AS MANUFCODE
                , MD.DPEFFECTFROMDATE AS DPEFFECTFROMDATE
                , MD.EFFECTFROMDATE AS EFFECTIVEFROMDATE
                , MD.EFFECTTODATE AS EFFECTIVETODATE
            FROM LU_OTHER_METER_DPID L1
            LEFT JOIN BT_TE_WORKING BTW ON L1.DPID_PK = BTW.NO_IWCS AND L1.STWACCOUNTNUMBER = BTW.NO_ACCOUNT AND BTW.PERIOD = 16
            JOIN MO_DISCHARGE_POINT MD ON BTW.NO_IWCS = MD.NO_IWCS AND BTW.NO_ACCOUNT = MD.NO_ACCOUNT
            JOIN MO_METER MM ON L1.METERREF = MM.METERREF
--            GROUP BY L1.METERREF, TRIM(L1.MANUFACTURER_PK), TRIM(L1.MANUFACTURERSERIALNUM_PK), TRIM(MD.DPID_PK), TRIM(MD.SPID_PK), TRIM(MD.STWPROPERTYNUMBER_PK), TRIM(MM.MANUFCODE), MD.DPEFFECTFROMDATE, MD.EFFECTFROMDATE, MD.EFFECTTODATE
        )
        WHERE SPID_PK IS NOT NULL 
            AND DPID_PK IS NOT NULL
--            AND NO_EQUIPMENT LIKE '887002001%'  --(TESTING ONLY)
            AND (NM_PREFERRED, NO_UTL_EQUIP) NOT IN (SELECT NVL(MANUFACTURER_PK,'-'), NVL(MANUFACTURERSERIALNUM_PK,'-') FROM LU_TE_METER_DPID_EXCLUSION) --v0.07
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
      --l_rec_war := FALSE;
      l_rec_written := TRUE;    -- set default record status to write

      -- keep count of distinct meters
      l_no_row_read := l_no_row_read + 1;

--****TODO - ADD TRANSFORM RULES
      l_progress := 'Process Transformations';
      l_mo.INITIALMETERREADDATE := NULL;

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
              -- IF ALL CONDITIONS MET - WRITE ROLL-OVER RECORD TO MO_METER_DPIDXREF TABLE
--****TODO - ADD INSERT
              INSERT INTO MO_METER_DPIDXREF
--                INSERT INTO TEMP_METER_DPIDXREF
                (METERDPIDXREF_PK, MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, DPID_PK, SPID
                , DPEFFECTFROMDATE, EFFECTIVEFROMDATE, EFFECTIVETODATE -- V 0.02 
                , PERCENTAGEDISCHARGE
                , MANUFCODE, INSTALLEDPROPERTYNUMBER)
              VALUES
                (t_met(i).NO_EQUIPMENT, t_met(i).NM_PREFERRED, t_met(i).NO_UTL_EQUIP, t_met(i).DPID_PK, t_met(i).SPID_PK
                , t_met(i).DPEFFECTFROMDATE, t_met(i).EFFECTIVEFROMDATE, t_met(i).EFFECTIVETODATE -- V 0.02 
                , t_met(i).MDVOL
                , t_met(i).MANUFCODE, t_met(i).NO_PROPERTY);
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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2630, l_no_row_read,    'Distinct Eligible TE meters reads during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2640, l_no_row_dropped, 'Eligible TE meters dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2650, l_no_row_insert,  'Eligible TE meters written to MO_METER_DPID_ASSOC during Transform');

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
END P_MOU_TRAN_METER_DPIDXREF;
/
/
show errors;
exit;