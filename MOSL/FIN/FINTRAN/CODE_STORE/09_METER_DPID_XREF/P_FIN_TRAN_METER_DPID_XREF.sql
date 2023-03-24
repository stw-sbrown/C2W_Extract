create or replace
PROCEDURE           P_FIN_TRAN_METER_DPID_XREF (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter XREF SAP Extract
--
-- AUTHOR         : Ola Badmus
--
-- FILENAME       : P_FIN_TRAN_METER_DPID_XREF.sql
--
-- Subversion $Revision: 5373 $
--
-- CREATED        : 01/08/2016
--
-- DESCRIPTION    : Procedure to populate MO_METER_READING table in FINTRAN
--                 Will read from reception.SAP_METER_READING and OWC_METER_READING tables
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date                Author         CR/DEF    Description
-- ---------   ---------------     -------        ----      ----------------------------------
-- V 0.01      28/07/2016          O.Badmus                 Initial Draft
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_FIN_TRAN_METER_DPID_XREF';  
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    varchar2(100);
  l_prev_met                    MO_METER_READING.METERREF%type; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mo                          MO_METER_DPIDXREF%ROWTYPE; --modify
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_rec_war                     BOOLEAN;
  l_no_meter_read               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_written            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_dropped            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_curr_meter_written          BOOLEAN;

CURSOR cur_met (P_METERREF_START   MO_METER_READING.METERREF%type,
                P_METERREF_END     MO_METER_READING.METERREF%type)
    is
     SELECT  DPEFFECTFROMDATE,
             DPID_PK,
             EFFECTIVEFROMDATE,
             EFFECTIVETODATE,
             INSTALLEDPROPERTYNUMBER  ,
             MANUFACTURERSERIALNUM_PK,
             MANUFACTURER_PK,
             MANUFCODE,
             INITIALMETERREADDATE,
             NO_EQUIPMENT || Row_Nr AS NO_EQUIPMENT,
             PERCENTAGEDISCHARGE, 
             SPID_PK
     FROM (SELECT  NULL DPEFFECTFROMDATE,
                   dp.DPID_PK,
                   null EFFECTIVEFROMDATE,
                   null EFFECTIVETODATE,
                   mt.INSTALLEDPROPERTYNUMBER  ,
                   mt.MANUFACTURERSERIALNUM_PK,
                   mt.MANUFACTURER_PK,
                   null MANUFCODE,
                   NULL INITIALMETERREADDATE,
                   mt.METERREF  as NO_EQUIPMENT  ,
                   dp.PERCENTAGEDISCHARGE,  
                   sp.SPID_PK,
                   ROW_NUMBER() OVER (PARTITION BY mt.METERREF ORDER BY dp.DPID_PK) AS Row_Nr
           FROM    RECEPTION.SAP_METER_DISCHARGE_POINT dp
                   JOIN MO_METER mt ON mt.MANUFACTURER_PK = dp.MANUFACTURER_PK AND mt.MANUFACTURERSERIALNUM_PK = dp.MANUFACTURERSERIALNUM_PK 
                   JOIN MO_SUPPLY_POINT sp ON SUBSTR(mt.SPID_PK,1,10) = sp.CORESPID_PK 
           WHERE   sp.SERVICECATEGORY = 'S') x
     ORDER BY NO_EQUIPMENT;


TYPE tab_meter IS TABLE OF cur_met%ROWTYPE INDEX BY PLS_INTEGER;
t_met tab_meter;

BEGIN

   l_progress := 'Start';
   l_err.TXT_DATA := C_MODULE_NAME;
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
   l_no_meter_read := 0;
   l_no_meter_written := 0;
   l_no_meter_dropped := 0;

   -- get job no
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
   COMMIT;
   l_progress := 'processing ';

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(NO_BATCH, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- process all records for range supplied
  OPEN cur_met (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX); -- modify

  l_progress := 'loop processing ';

  LOOP

      FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

      l_no_row_read := l_no_row_read + t_met.COUNT;

      FOR i IN 1..t_met.COUNT
      LOOP

          l_err.TXT_KEY := t_met(i).SPID_PK || ',' || t_met(i).NO_EQUIPMENT;          
          l_mo := null;
          L_REC_EXC := false;
          L_REC_WAR := false;
          
          l_mo.DPEFFECTFROMDATE  := t_met(i).DPEFFECTFROMDATE;
          l_mo.DPID_PK  := t_met(i).DPID_PK;
          l_mo.EFFECTIVEFROMDATE  := t_met(i).EFFECTIVEFROMDATE;
          l_mo.EFFECTIVETODATE  := t_met(i).EFFECTIVETODATE;
          l_mo.INITIALMETERREADDATE  := t_met(i).INITIALMETERREADDATE;
          l_mo.INSTALLEDPROPERTYNUMBER  := t_met(i).INSTALLEDPROPERTYNUMBER;
          l_mo.MANUFACTURERSERIALNUM_PK  := t_met(i).MANUFACTURERSERIALNUM_PK;
          l_mo.MANUFACTURER_PK  := t_met(i).MANUFACTURER_PK;
          l_mo.MANUFCODE  := t_met(i).MANUFCODE;
          l_mo.METERDPIDXREF_PK  := t_met(i).NO_EQUIPMENT;
          l_mo.PERCENTAGEDISCHARGE  := t_met(i).PERCENTAGEDISCHARGE;
          l_mo.SPID := t_met(i).SPID_PK;
          
          IF  L_REC_EXC = TRUE THEN  --using this if statement to pick up data that passes the biz rules into the table otherwise they will be dropped and would have be sent into the exception table
              IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                     OR l_no_row_err > l_job.ERR_TOLERANCE
                     OR l_no_row_war > l_job.WAR_TOLERANCE
                     )
              THEN
                  CLOSE cur_met;
                  l_job.IND_STATUS := 'ERR';
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                  commit;
                  return_code := -1;
                  RETURN;
              END IF;
              l_rec_written := FALSE;
          ELSE
              IF (L_REC_WAR = true AND l_no_row_war > l_job.WAR_TOLERANCE) THEN
                  CLOSE cur_met;
                  l_job.IND_STATUS := 'ERR';
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Warning tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                  commit;
                  return_code := -1;
                  return;
              END IF;
              l_rec_written := TRUE;
              l_progress := 'INSERT MO_METER_DPIDXREF';
              BEGIN
                  INSERT INTO MO_METER_DPIDXREF
                  (DPEFFECTFROMDATE,	DPID_PK,	EFFECTIVEFROMDATE,	EFFECTIVETODATE,	INITIALMETERREADDATE,	INSTALLEDPROPERTYNUMBER
                  ,	MANUFACTURERSERIALNUM_PK,	MANUFACTURER_PK,	MANUFCODE,	METERDPIDXREF_PK,	PERCENTAGEDISCHARGE,	SPID )
                  VALUES
                   ( t_met(i).DPEFFECTFROMDATE,	t_met(i).DPID_PK,	t_met(i).EFFECTIVEFROMDATE,	t_met(i).EFFECTIVETODATE,	t_met(i).INITIALMETERREADDATE, t_met(i).INSTALLEDPROPERTYNUMBER
                   ,t_met(i).MANUFACTURERSERIALNUM_PK, t_met(i).MANUFACTURER_PK, t_met(i).MANUFCODE, t_met(i).NO_EQUIPMENT, t_met(i).PERCENTAGEDISCHARGE,	t_met(i).SPID_PK  );

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
                     OR l_no_row_war > l_job.WAR_TOLERANCE
                     )
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
          END IF;  --close of if  L_REC_EXC statement

          IF l_rec_written THEN
              l_no_row_insert := l_no_row_insert + 1;
              l_curr_meter_written := TRUE;
              IF (l_prev_met <> t_met(i).NO_EQUIPMENT) THEN
                  l_no_meter_written := l_no_meter_written + 1;
              END IF;
          ELSE
              L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
              IF (l_prev_met <> t_met(i).NO_EQUIPMENT AND l_curr_meter_written = FALSE) THEN
                  l_no_meter_dropped := l_no_meter_dropped + 1;
              END IF;
          END IF;
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
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1150, L_NO_ROW_READ,    'Distinct Meter Readings read during Transform');
  --P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1151, l_no_meter_read,  'Distinct Meters read during Transform');  -- CR01 - add recon counts for METERS
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1160, L_NO_ROW_DROPPED, 'Meter Readings dropped during Transform');
  --P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1161, l_no_meter_dropped,  'Distinct Meters dropped during Transform');  -- CR01 - add recon counts for METERS
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1170, l_no_row_insert,  'Meter Readings written to MO_METER_READING during Transform');
  --P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1171, l_no_meter_written,  'Distinct Meters written during Transform');  -- CR01 - add recon counts for METERS

  --  check counts match
  IF l_no_row_read <> l_no_row_insert + L_NO_ROW_DROPPED THEN
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_no_row_read || ',' || l_no_row_insert, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
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
when OTHERS then
      L_ERROR_NUMBER := SQLCODE;
      L_ERROR_MESSAGE := SUBSTR(SQLERRM,1,512);
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      commit;
      RETURN_CODE := -1;
END P_FIN_TRAN_METER_DPID_XREF;
/
show error;

exit;