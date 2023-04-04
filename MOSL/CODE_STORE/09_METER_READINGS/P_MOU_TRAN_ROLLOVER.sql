
  CREATE OR REPLACE PROCEDURE P_MOU_TRAN_ROLLOVER (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform BT_CLOCKOVER
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_ROLLOVER.sql
--
-- Subversion $Revision: 6051 $
--
-- CREATED        : 04/04/2016
--
-- DESCRIPTION    : Procedure to create the BT_CLOCKOVER lookup table
--                 Will read from key gen meter records
--                 For meter readings in last 2.5 years, check if reading is LESS than previous reading (i.e. a clockover has occurred)
--                 and write to BT_CLOCKOVER table if all clockover rules met.
-- NOTES  :
-- This procedure must be run once BEFORE the P_MOU_TRAN_METER_READINGS procedure runs.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     CR/DEF     Description
-- ---------   ----------  -------    ------     ---------------------------------------------------
-- V 2.03      01/11/2016  S.Badhan              Performance changes.
-- V 2.02      26/09/2016  D.Cheung   CR_037     exclude Non-billable reads
-- V 2.01      12/05/2016  D.Cheung   D40        Added ESTIMATE Read Sources to Extract Criteria   
-- V 1.01      10/05/2016  D.Cheung              Issue I-173 - Remove VOID Status Rule to align with METER_READING
-- V 0.01      04/04/2016  D.Cheung              Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_ROLLOVER';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    BT_TVP163.NO_EQUIPMENT%TYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  --l_clockover                   BT_CLOCKOVER%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_prev_am_reading             CIS.TVP195READING.AM_READING%TYPE;

CURSOR cur_met (p_no_equipment_start   BT_TVP163.NO_EQUIPMENT%TYPE,
                 p_no_equipment_end     BT_TVP163.NO_EQUIPMENT%TYPE)
    IS
      SELECT /*+ PARALLEL(TV163,60) PARALLEL(T195,60) PARALLEL(TELG,60) */
      DISTINCT
            TV163.NO_EQUIPMENT
            ,T195.TS_CAPTURED
            ,T195.AM_READING
            ,T195.TP_READING
            ,T195.CD_MTR_RD_SRCE_98
            ,T195.ST_READING_168
            ,TELG.VOID_STATUS
      FROM   BT_TVP163 TV163
      JOIN CIS.TVP195READING T195 ON
          T195.NO_EQUIPMENT = TV163.NO_EQUIPMENT
          AND  T195.CD_COMPANY_SYSTEM = TV163.CD_COMPANY_SYSTEM
          AND T195.TP_READING = 'M'
          AND T195.CD_MTR_RD_SRCE_98 IN ('N','W','L','P','C','U','H','O','F','G','S','X')
          AND T195.ST_READING_168 IN ('B')  --V2.02
          AND MONTHS_BETWEEN(SYSDATE, T195.TS_CAPTURED) <= 30
      JOIN CIS.ELIGIBILITY_CONTROL_TABLE TELG ON
          TELG.CD_COMPANY_SYSTEM = TV163.CD_COMPANY_SYSTEM
          AND    TELG.NO_PROPERTY = TV163.NO_PROPERTY
      WHERE  TV163.NO_EQUIPMENT BETWEEN p_no_equipment_start AND p_no_equipment_end
      --AND TRIM(TV163.ST_EQUIP_INST)     = 'A' --Available
      --AND T195.AM_READING > 0 --V2.02
      ORDER BY TV163.NO_EQUIPMENT ASC, T195.TS_CAPTURED ASC;

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
   l_prev_am_reading := 0;    -- INITIALIZE LAST READING VALUE

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

   -- CLEAR EXISTING TABLE RECORDS
   l_progress := 'truncating table';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_CLOCKOVER';

  -- start processing all records for range supplied


  OPEN cur_met (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing ';

  LOOP

    FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_met.COUNT
    LOOP

      l_err.TXT_KEY := t_met(i).NO_EQUIPMENT;
      --l_clockover := NULL;
      --l_rec_exc := FALSE;
      l_rec_written := TRUE;    -- set default record status to write

      -- keep count of distinct meter readings
      l_no_row_read := l_no_row_read + 1;

      l_progress := 'DROP row if equipment changed from previous';
      IF (l_prev_met <> t_met(i).NO_EQUIPMENT) THEN
          l_rec_written := FALSE;   -- IF NO_EQUIPMENT CHANGED DON'T WRITE
      END IF;

      -- **** NA - METER KEYGEN ONLY HAS ACTIVE METERS
      --l_progress := 'DROP row if Meter Status NOT A (Actual)';
      --IF (t_met(i).ST_EQUIP_INST <> 'A') THEN
          --l_rec_written := FALSE;   -- IF METER STATUS NOT 'A' (AVAILABLE) - DON'T WRITE
      --END IF;

--*** V1.01 - I-173
      --l_progress := 'DROP row if Acct Status is not VOID for Actual Non-Billable Meter Reading';
      --IF (t_met(i).ST_READING_168 = 'N' AND t_met(i).VOID_STATUS <> 'B - FULL VOID') THEN
      --    l_rec_written := FALSE;   -- IF ACCT STATUS IS NOT VOID FOR READ STATUS N (NOT BILLABLE) - DON'T WRITE
      --END IF;
--*** V1.01

      l_progress := 'DROP row if current reading GREATER than previous reading';
      IF (t_met(i).AM_READING >= l_prev_am_reading) THEN
          l_rec_written := FALSE;   -- IF LATEST READING GREATER THEN PREVIOUS READING THEN DON'T WRITE
      END IF;

      -- ***** EXCEPTION HANDLING CURRENTLY NOT USED IN THIS PROCEDURE
--      IF l_rec_exc THEN  --using this if statement to pick up data that passes the biz rules into the table otherwise they will be dropped and would have be sent into the exception table
--          IF (   l_no_row_exp > l_job.EXP_TOLERANCE
--                 OR l_no_row_err > l_job.ERR_TOLERANCE
--                 OR l_no_row_war > l_job.WAR_TOLERANCE)
--          THEN
--              CLOSE cur_met;
--              L_JOB.IND_STATUS := 'ERR';
--              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
--              P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
--              commit;
--              return_code := -1;
--              RETURN;
--          END IF;
--          l_rec_written := FALSE;
--      END IF;

      IF l_rec_written THEN
          BEGIN
              l_progress := 'loop processing ABOUT TO INSERT INTO TABLE';
              -- IF ALL CONDITIONS MET - WRITE ROLL-OVER RECORD TO BT_CLOCKOVER TABLE
              INSERT INTO BT_CLOCKOVER
                (STWMETERREF_PK, METERREADDATE, METERREAD, ROLLOVERINDICATOR)
              VALUES
                (t_met(i).NO_EQUIPMENT, t_met(i).TS_CAPTURED, t_met(i).AM_READING, 1);
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
      l_prev_am_reading := t_met(i).AM_READING;

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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP46', 2450, l_no_row_read,    'Distinct Eligible meter reading reads during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP46', 2460, l_no_row_dropped, 'Eligible property meters dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP46', 2470, l_no_row_insert,  'Eligible Property meters written to BT_CLOCKOVER during Transform');

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
END P_MOU_TRAN_ROLLOVER;
/
/
show errors;
exit;