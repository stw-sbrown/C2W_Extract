create or replace
PROCEDURE  P_OWC_TRAN_METER_READING (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                     no_job       IN MIG_JOBREF.NO_JOB%TYPE,
                                     return_code  IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter Reading Transform SAP Extract
--
-- AUTHOR         : Ola Badmus
--
-- FILENAME       : P_OWC_TRAN_METER_READING.sql
--
-- Subversion $Revision: 5458 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_METER_READING table.
--                  Will read from reception.OWC_METER_READING tables
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/09/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_METER_READING'; 
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  L_PROGRESS                    varchar2(100);
  L_PREV_MET                    MO_METER_READING.METERREF%type;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  L_ERR                         MIG_ERRORLOG%ROWTYPE;
  --l_prev_cus                    METER_READING.METERREF%TYPE;
  L_MO                          MO_METER_READING%ROWTYPE; --modify
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
                P_METERREF_END     MO_METER_READING.METERREF%type)
    IS
    select  distinct  
            mrd.MANUFACTURERSERIALNUM_PK,
            mrd.MANUFACTURER_PK,
            mrd.METERREAD,
            mrd.METERREADMETHOD,
            mrd.METERREADDATE,
            mrd.ROLLOVERINDICATOR,
            mrd.ESTIMATEDREADREASONCODE,
            mrd.ESTIMATEDREADREMEDIALWORKIND,
            mrd.METERREADTYPE,
            mrd.previousmeterreading,
            mt.METERREF as SAPEQUIPMENT ,
            mrd.PREVMETERREF,
            mtr.INITIALMETERREADDATE,
            null MANUFCODE,
            null METERREADERASEDFLAG,
            null METERREADREASONTYPE,
            null METERREADSETTLEMENTFLAG,
            null METERREADSTATUS,
            null METRREADDATEFORREMOVAL,
            null PREVVALCDVCANDIDATEDAILYVOLUME,
            null RDAOUTCOME,
            0 REREADFLAG,
            mrd.ROLLOVERINDICATOR as ROLLOVERFLAG,
            mt.INSTALLEDPROPERTYNUMBER
    FROM   RECEPTION.SAP_METER_READING mrd 
     left join RECEPTION.SAP_METER mtr
    ON mrd.MANUFACTURERSERIALNUM_PK = mtr.MANUFACTURERSERIALNUM_PK 
    AND mrd.MANUFACTURER_PK = mtr.MANUFACTURER_PK
    JOIN MO_METER mt ON  mt.MANUFACTURERSERIALNUM_PK = mtr.MANUFACTURERSERIALNUM_PK 
                           AND mt.MANUFACTURER_PK = mtr.MANUFACTURER_PK
--    where mrd.SAPEQUIPMENT between P_METERREF_START and P_METERREF_END
    ORDER BY SAPEQUIPMENT;
    
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
      P_MIG_BATCH.FN_UPDATEJOB(NO_BATCH, L_JOB.NO_INSTANCE, L_JOB.IND_STATUS);
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

        L_ERR.TXT_KEY := t_met(I).SAPEQUIPMENT;
        L_MO := null;
        L_REC_EXC := false;
        L_REC_WAR := FALSE;
        l_rec_written := true;
          
        l_mo.ESTIMATEDREADREASONCODE := t_met(i).ESTIMATEDREADREASONCODE;
        l_mo.ESTIMATEDREADREMEDIALWORKIND := t_met(i).ESTIMATEDREADREMEDIALWORKIND;
        l_mo.INITIALMETERREADDATE := t_met(i).INITIALMETERREADDATE;
        l_mo.INSTALLEDPROPERTYNUMBER := t_met(i).INSTALLEDPROPERTYNUMBER;
        l_mo.MANUFACTURER_PK := t_met(i).MANUFACTURER_PK;
        l_mo.MANUFACTURERSERIALNUM_PK := t_met(i).MANUFACTURERSERIALNUM_PK;
        l_mo.MANUFCODE := null;
        l_mo.METERREAD := t_met(i).METERREAD;
        l_mo.METERREADDATE := t_met(i).METERREADDATE;
        l_mo.METERREADERASEDFLAG := t_met(i).METERREADERASEDFLAG;
        l_mo.METERREADMETHOD := t_met(i).METERREADMETHOD;
        l_mo.METERREADREASONTYPE := t_met(i).METERREADREASONTYPE;
        l_mo.METERREADSETTLEMENTFLAG := t_met(i).METERREADSETTLEMENTFLAG;
        l_mo.METERREADSTATUS := t_met(i).METERREADSTATUS;
        l_mo.METERREADTYPE := t_met(i).METERREADTYPE;
        l_mo.METERREF := t_met(i).SAPEQUIPMENT;
        l_mo.METRREADDATEFORREMOVAL := t_met(i).METRREADDATEFORREMOVAL;
        l_mo.PREVVALCDVCANDIDATEDAILYVOLUME := t_met(i).PREVVALCDVCANDIDATEDAILYVOLUME;
        l_mo.RDAOUTCOME := t_met(i).RDAOUTCOME;
        l_mo.REREADFLAG := t_met(i).REREADFLAG;
        l_mo.ROLLOVERFLAG := t_met(i).ROLLOVERFLAG;
        l_mo.ROLLOVERINDICATOR := t_met(i).ROLLOVERINDICATOR;

    --  l_mo.INSTALLEDPROPERTYNUMBER := t_met(i).STWPROPERTYNUMBER_PK;      --****  ????

        l_progress := 'INSERT MO_METER_READING';
        BEGIN
            INSERT INTO MO_METER_READING
            (ESTIMATEDREADREASONCODE,	ESTIMATEDREADREMEDIALWORKIND,	INITIALMETERREADDATE,	INSTALLEDPROPERTYNUMBER,	MANUFACTURER_PK,	MANUFACTURERSERIALNUM_PK
            ,	MANUFCODE,	METERREAD,	METERREADDATE,	METERREADERASEDFLAG,	METERREADMETHOD,	METERREADREASONTYPE,	METERREADSETTLEMENTFLAG,	METERREADSTATUS
            ,	METERREADTYPE,	METERREF,	METRREADDATEFORREMOVAL,	PREVVALCDVCANDIDATEDAILYVOLUME,	RDAOUTCOME,	REREADFLAG,	ROLLOVERFLAG,	ROLLOVERINDICATOR )
            VALUES
            (t_met(i).ESTIMATEDREADREASONCODE,	t_met(i).ESTIMATEDREADREMEDIALWORKIND,	t_met(i).INITIALMETERREADDATE,	t_met(i).INSTALLEDPROPERTYNUMBER,	t_met(i).MANUFACTURER_PK,	
            t_met(i).MANUFACTURERSERIALNUM_PK, t_met(i).MANUFCODE, t_met(i).METERREAD,	t_met(i).METERREADDATE,	t_met(i).METERREADERASEDFLAG,	t_met(i).METERREADMETHOD,
            t_met(i).METERREADREASONTYPE,	t_met(i).METERREADSETTLEMENTFLAG,	t_met(i).METERREADSTATUS, t_met(i).METERREADTYPE,	t_met(i).SAPEQUIPMENT,	t_met(i).METRREADDATEFORREMOVAL,
            t_met(i).PREVVALCDVCANDIDATEDAILYVOLUME,	t_met(i).RDAOUTCOME,	t_met(i).REREADFLAG,	t_met(i).ROLLOVERFLAG,	t_met(i).ROLLOVERINDICATOR );
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
                 OR l_no_row_war > l_job.WAR_TOLERANCE )
             THEN
                 CLOSE cur_met;
                 L_JOB.IND_STATUS := 'ERR';
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
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1150, L_NO_ROW_READ,    'Distinct Meter Readings read during Transform');
----  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1151, l_no_meter_read,  'Distinct Meters read during Transform');  -- CR01 - add recon counts for METERS
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1160, L_NO_ROW_DROPPED, 'Meter Readings dropped during Transform');
----  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1161, l_no_meter_dropped,  'Distinct Meters dropped during Transform');  -- CR01 - add recon counts for METERS
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1170, L_NO_ROW_INSERT,  'Meter Readings written to MO_METER_READING during Transform');
----  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1171, l_no_meter_written,  'Distinct Meters written during Transform');  -- CR01 - add recon counts for METERS

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  commit;

EXCEPTION
WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE(l_mo.METERREAD);
      L_ERROR_NUMBER := SQLCODE;
      L_ERROR_MESSAGE := SUBSTR(SQLERRM,1,512);
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY,  SUBSTR(L_ERR.TXT_DATA || ',' || l_progress,1,100));
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      commit;
      RETURN_CODE := -1;
END P_OWC_TRAN_METER_READING;
/
show error;

exit;