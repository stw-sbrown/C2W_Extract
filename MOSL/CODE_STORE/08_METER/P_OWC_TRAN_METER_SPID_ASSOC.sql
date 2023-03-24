create or replace
PROCEDURE P_OWC_TRAN_METER_SPID_ASSOC (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                       no_job       IN MIG_JOBREF.NO_JOB%TYPE,
                                       return_code  IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter XREF SAP Extract
--
-- AUTHOR         : Ola Badmus
--
-- FILENAME       : P_OWC_TRAN_METER_SPID_ASSOC.sql
--
-- Subversion $Revision: 5458 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_METER_READING table.
--                  Will read from OWC_METER_READING tables.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/09/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_METER_SPID_ASSOC'; 
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  L_PROGRESS                    varchar2(100);
  L_PREV_MET                    MO_METER_READING.METERREF%type; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  L_ERR                         MIG_ERRORLOG%ROWTYPE;
  L_MO                          MO_METER_SPID_ASSOC%ROWTYPE; 
  l_mt                          LU_METER_MANUFACTURER%ROWTYPE;  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  L_NO_ROW_INSERT               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  L_REC_WRITTEN                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_rec_war                     BOOLEAN;
  l_prev_am_reading             CIS.TVP195READING.AM_READING%TYPE;
  l_manufserialchk              NUMBER;
  l_no_meter_read               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_written            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_dropped            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_curr_meter_written          BOOLEAN;
  l_initialmeterreaddate        MO_METER_READING.INITIALMETERREADDATE%TYPE;
  l_prev_readdate               VARCHAR2(15);


CURSOR cur_met (P_METERREF_START   MO_METER_READING.METERREF%type,
                P_METERREF_END     MO_METER_READING.METERREF%TYPE)
    IS
    SELECT distinct
           t1.MANUFACTURER_PK,
           t1.MANUFACTURERSERIALNUM_PK,
           null manufcode,
           mt.METERREF as SAPEQUIPMENT,
           t1.SPID_PK,
           t2.SAPFLOCNUMBER,
           mt.INSTALLEDPROPERTYNUMBER
    FROM   RECEPTION.SAP_METER_SUPPLY_POINT t1
           JOIN RECEPTION.SAP_METER t2
           ON  t1.MANUFACTURER_PK = t2.MANUFACTURER_PK AND t1.MANUFACTURERSERIALNUM_PK = t2.MANUFACTURERSERIALNUM_PK
           JOIN MO_METER mt ON  mt.MANUFACTURERSERIALNUM_PK = t1.MANUFACTURERSERIALNUM_PK 
                                 AND mt.MANUFACTURER_PK          = t1.MANUFACTURER_PK
    ORDER BY METERREF;

TYPE tab_meter IS TABLE OF cur_met%ROWTYPE INDEX BY PLS_INTEGER;
t_met tab_meter;

BEGIN

   l_progress := 'Start';
   l_ERR.TXT_DATA := C_MODULE_NAME;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   L_NO_ROW_INSERT := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_prev_met := 0;
   l_prev_am_reading := 0;
   l_job.IND_STATUS := 'RUN';
   l_no_meter_read := 0;
   l_no_meter_written := 0;
   l_no_meter_dropped := 0;
   l_prev_readdate := NULL;

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
                         L_JOB.IND_STATUS);
COMMIT;
   l_progress := 'processing ';

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(NO_BATCH, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- process all records for range supplied
  OPEN cur_met (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX); 

  l_progress := 'loop processing ';

  LOOP

      FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

      l_no_row_read := l_no_row_read + t_met.COUNT;

      FOR i IN 1..t_met.COUNT
      LOOP

          l_err.TXT_KEY := t_met(i).SPID_PK || ',' || t_met(i).SAPEQUIPMENT;
          l_mo := null;
          L_REC_EXC := false;
          L_REC_WAR := false;
          
          l_mo.MANUFACTURER_PK  := t_met(i).MANUFACTURER_PK;
          l_mo.MANUFACTURERSERIALNUM_PK  := t_met(i).MANUFACTURERSERIALNUM_PK;
          l_mo.MANUFCODE  := t_met(i).MANUFCODE;
          l_mo.METERREF  := t_met(i).SAPEQUIPMENT;
          l_mo.SPID := t_met(i).SPID_PK;
          l_mo.STWPROPERTYNUMBER_PK  := t_met(i).INSTALLEDPROPERTYNUMBER;

          L_REC_WRITTEN := TRUE;
          l_progress := 'INSERT MO_METER_SPID_ASSOC';
          BEGIN
              INSERT INTO MO_METER_SPID_ASSOC 
              (MANUFACTURER_PK,	MANUFACTURERSERIALNUM_PK,	MANUFCODE,	METERREF,	SPID, STWPROPERTYNUMBER_PK)
              VALUES
              (t_met(i).MANUFACTURER_PK, t_met(i).MANUFACTURERSERIALNUM_PK, t_met(i).MANUFCODE,	 t_met(i).SAPEQUIPMENT,	 t_met(i).SPID_PK, t_met(i).INSTALLEDPROPERTYNUMBER);
          EXCEPTION
          WHEN OTHERS THEN
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;

             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_ERR.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_exp := l_no_row_exp + 1;

             -- if tolearance limit has een exceeded, set error message and exit out
             IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)
             THEN
                 CLOSE cur_met;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 commit;
                 return_code := -1;
                RETURN;
              END IF;
          END;

          IF l_rec_written THEN
              l_no_row_insert := l_no_row_insert + 1;
              l_curr_meter_written := TRUE;
              IF (l_prev_met <> t_met(i).SAPEQUIPMENT) THEN
                  l_no_meter_written := l_no_meter_written + 1;
              END IF;
--              l_prev_am_reading := t_met(i).AM_READING;
--              l_prev_readdate := TO_CHAR(t_met(i).TS_CAPTURED, 'DD-MON-YYYY');
          ELSE
              L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
              IF (l_prev_met <> t_met(i).SAPEQUIPMENT AND l_curr_meter_written = FALSE) THEN
                  l_no_meter_dropped := l_no_meter_dropped + 1;
              END IF;
          END IF;
          l_prev_met := t_met(i).SAPEQUIPMENT;

      END LOOP;

      IF t_met.COUNT < l_job.NO_COMMIT THEN
         EXIT;
      ELSE
         commit;
      END IF;

  END LOOP;

  CLOSE cur_met;
  
--  -- write counts
--  l_progress := 'Writing Counts';
--
--  --  the recon key numbers used will be specific to each procedure
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1150, L_NO_ROW_READ,    'Distinct Meter Readings read during Transform');
--  --P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1151, l_no_meter_read,  'Distinct Meters read during Transform');  -- CR01 - add recon counts for METERS
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1160, L_NO_ROW_DROPPED, 'Meter Readings dropped during Transform');
--  --P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1161, l_no_meter_dropped,  'Distinct Meters dropped during Transform');  -- CR01 - add recon counts for METERS
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1170, L_NO_ROW_INSERT,  'Meter Readings written to MO_METER_READING during Transform');
--  --P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP34', 1171, l_no_meter_written,  'Distinct Meters written during Transform');  -- CR01 - add recon counts for METERS

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  commit;

EXCEPTION
WHEN OTHERS THEN
      L_ERROR_NUMBER := SQLCODE;
      L_ERROR_MESSAGE := SUBSTR(SQLERRM,1,512);
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY,  SUBSTR(L_ERR.TXT_DATA || ',' || l_progress,1,100));
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      commit;
      RETURN_CODE := -1;
END P_OWC_TRAN_METER_SPID_ASSOC;
/
show error;

exit;