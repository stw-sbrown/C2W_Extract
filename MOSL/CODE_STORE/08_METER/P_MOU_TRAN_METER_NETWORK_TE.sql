create or replace
PROCEDURE           P_MOU_TRAN_METER_NETWORK_TE (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter TE Transform MO_METER_NETWORK
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_NETWORK_TE.sql
--
-- Subversion $Revision: 6062 $
--
-- CREATED        : 01/11/2016
--
-- DESCRIPTION    : Procedure to transform and populate the MO_METER_NETWORK table for TE Networks
--                 Will read from key gen meter records, perform transformations
--                 and write to MO_METER_NETWORK
--
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     CP/DEF    Description
-- ---------   ----------  -------    ------    ---------------------------------------------
-- V 0.01      01/11/2016  D.Cheung             Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_NETWORK_TE';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    BT_TVP163.NO_PROPERTY%TYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
--  l_mo                          MO_METER_NETWORK%ROWTYPE;           --***TODO - ADD OUTPUT TABLE TYPE
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
--***TODO - VERIFY MAIN QUERY
         SELECT 
                DISTINCT 
                MM_M.METERREF                     MAIN_METERREF
                ,MM_M.INSTALLEDPROPERTYNUMBER     MAIN_STWPROPERTYNUMBER_PK
                ,MM_M.MANUFACTURER_PK             MAIN_MANUFACTURER_PK             
                ,MM_M.MANUFACTURERSERIALNUM_PK    MAIN_MANSERIALNUM_PK    
                ,MAX(MSA_M.SPID)                  MAIN_SPID_PK
                ,MM_M.MANUFCODE                   MAIN_MANUFCODE
                ,MM_M.METERTREATMENT              MAIN_METERTREATMENT
                ,MM_S.METERREF                    SUB_METERREF
                ,MM_S.INSTALLEDPROPERTYNUMBER     SUB_STWPROPERTYNUMBER_PK
                ,MM_S.MANUFACTURER_PK             SUB_MANUFACTURER_PK 
                ,MM_S.MANUFACTURERSERIALNUM_PK    SUB_MANSERIALNUM_PK
                ,MAX(MSA_S.SPID)                  SUB_SPID_PK
                ,MM_S.MANUFCODE                   SUB_MANUFCODE
                ,MM_S.METERTREATMENT              SUB_METERTREATMENT
--                ,'N'                              MAIN_NMM
--                ,1                                NET_LEVEL
--                ,'+'                              FG_ADD_SUBTRACT
                ,MM_M.INSTALLEDPROPERTYNUMBER     MASTER_PROPERTY
--                ,0                                AGG_NET_FLAG
                ,MM_S.NONMARKETMETERFLAG          SUB_NMM
            FROM TE_SUB_METERS_TO_V SMV
                , MO_METER MM_M
                , MO_METER_SPID_ASSOC MSA_M
                , MO_METER MM_S
                , MO_METER_SPID_ASSOC MSA_S
            WHERE MM_M.MANUFACTURERSERIALNUM_PK = SMV.SUBMETERTO
            AND MM_S.METERREF = SMV.NO_IWCS || SMV.MET_REF
            AND MSA_M.METERREF = MM_M.METERREF
            AND MSA_S.METERREF(+) = MM_S.METERREF
            GROUP BY MM_M.METERREF, MM_M.INSTALLEDPROPERTYNUMBER, MM_M.MANUFACTURER_PK, MM_M.MANUFACTURERSERIALNUM_PK, MM_M.MANUFCODE, MM_M.METERTREATMENT
                , MM_S.METERREF, MM_S.INSTALLEDPROPERTYNUMBER, MM_S.MANUFACTURER_PK, MM_S.MANUFACTURERSERIALNUM_PK, MM_S.MANUFCODE, MM_S.METERTREATMENT, MM_M.INSTALLEDPROPERTYNUMBER, MM_S.NONMARKETMETERFLAG
            ORDER BY MM_M.METERREF, MM_S.METERREF
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
--   l_mo := NULL;
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
      l_rec_exc := FALSE;
      l_rec_written := TRUE;    -- set default record status to write

      -- keep count of distinct sub meters
      l_no_row_read := l_no_row_read + 1;
     
      L_PROGRESS := 'CHECK MAIN TREATMENT';     
      IF (t_met(i).MAIN_METERTREATMENT <> 'POTABLE') THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('MAIN METER MUST BE A POTABLE WATER METER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      END IF;
      
      L_PROGRESS := 'CHECK SUB TREATMENT';     
      IF (t_met(i).SUB_METERTREATMENT <> 'PRIVATETE' AND t_met(i).SUB_METERTREATMENT <> 'PRIVATEWATER') THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('SUB MUST BE A PRIVATEWATER OR PRIVATETE METER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      END IF;

      L_PROGRESS := 'CHECK MAIN AND SUB CORESPIDS';
      IF (SUBSTR(t_met(i).MAIN_SPID_PK,1,10) <> SUBSTR(t_met(i).SUB_SPID_PK,1,10)) THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('CORESPID OF WATER MAIN MUST MATCH SEWERAGE SPID ON SUB',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
          L_REC_EXC := true;
      END IF;
    
      --NON-NETWORK RULE CHECKS
      L_PROGRESS := 'CHECK FOR NULL MAIN SPID';
      IF (t_met(i).MAIN_SPID_PK IS NULL) THEN
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NON-NETWORK-NULL MAIN SPID',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
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
                (MAIN_METERREF, MAIN_STWPROPERTYNUMBER_PK, MAIN_MANUFACTURER_PK, MAIN_MANSERIALNUM_PK, MAIN_SPID, MAIN_MANUFCODE
                , SUB_METERREF, SUB_STWPROPERTYNUMBER_PK, SUB_MANUFACTURER_PK, SUB_MANSERIALNUM_PK, SUB_SPID, SUB_MANUFCODE
                , MASTER_PROPERTY, AGG_NET_FLAG)
              VALUES
                (t_met(i).MAIN_METERREF, t_met(i).MAIN_STWPROPERTYNUMBER_PK, t_met(i).MAIN_MANUFACTURER_PK, t_met(i).MAIN_MANSERIALNUM_PK, t_met(i).MAIN_SPID_PK, t_met(i).MAIN_MANUFCODE
                , t_met(i).SUB_METERREF, t_met(i).SUB_STWPROPERTYNUMBER_PK, t_met(i).SUB_MANUFACTURER_PK, t_met(i).SUB_MANSERIALNUM_PK, t_met(i).SUB_SPID_PK, t_met(i).SUB_MANUFCODE
                , t_met(i).MASTER_PROPERTY, 0)
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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP49', 2541, l_no_row_read,    'TE meter networks reads during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP49', 2551, l_no_row_dropped, 'TE meter networks dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP49', 2561, l_no_row_insert,  'TE meter networks written to MO_METER_NETWORK during Transform');

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
END P_MOU_TRAN_METER_NETWORK_TE;
/
/
show errors;
exit;