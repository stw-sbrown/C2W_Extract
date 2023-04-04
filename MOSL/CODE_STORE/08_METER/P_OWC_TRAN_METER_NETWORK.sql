create or replace
PROCEDURE P_OWC_TRAN_METER_NETWORK (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                    no_job       IN MIG_JOBREF.NO_JOB%TYPE,
                                    return_code  IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter Network Transform SAP Extract
--
-- AUTHOR         : Ola Badmus
--
-- FILENAME       : P_OWC_TRAN_METER_NETWORK.sql
--
-- Subversion $Revision: 5749 $
--
-- CREATED        : 09/09/2016
--
-- DESCRIPTION    : Procedure to populate MO_ METER_NETWORK table.
--                  Will read from reception OWC_METER_READING tables
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/09/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_METER_NETWORK';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  L_PROGRESS                    varchar2(100);
  L_PREV_MET                    MO_METER_READING.METERREF%type;  
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mt                          LU_METER_MANUFACTURER%ROWTYPE;
  --l_prev_cus                    METER_READING.METERREF%TYPE;
  l_mo                          MO_METER_NETWORK%ROWTYPE; --modify
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  L_NO_ROW_INSERT               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_rec_war                     BOOLEAN;
  l_prev_am_reading             CIS.TVP195READING.AM_READING%TYPE;
  l_manufserialchk              NUMBER;
  l_no_meter_read               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_written            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_dropped            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_curr_meter_written          BOOLEAN;
  l_initialmeterreaddate        MO_METER_READING.INITIALMETERREADDATE%TYPE;
  L_PREV_READDATE               varchar2(15);

   CURSOR cur_met (P_METERREF_START   MO_METER_READING.METERREF%TYPE,
                   P_METERREF_END     MO_METER_READING.METERREF%type)
    IS
    SELECT DISTINCT
             net.MAINNONMARKETFLAG,
             net.MAINMANUFACTURERSERIALNUM,
             net.MAINMANUFACTURER,
             NULL MAIN_MANUFCODE,
             mtm.METERREF AS MAIN_METERREF,
             mtm.SPID_PK,
             mtm.INSTALLEDPROPERTYNUMBER AS MAIN_STWPROPERTYNUMBER_PK,
             net.SUBMANUFACTURERSERIALNUM,
             net.SUBMANUFACTURER,
             NULL SUB_MANUFCODE,
             mts.METERREF AS SUB_METERREF,
             mts.SPID_PK AS SUB_SPID,
             mts.INSTALLEDPROPERTYNUMBER AS SUB_STWPROPERTYNUMBER_PK,
             NET.OWC
      FROM   RECEPTION.OWC_METER_NETWORK  NET
      LEFT JOIN MO_METER mtm
             ON net.MAINMANUFACTURERSERIALNUM = mtm.MANUFACTURERSERIALNUM_PK
                AND net.MAINMANUFACTURER = mtm.MANUFACTURER_PK
      LEFT JOIN MO_METER mts
             ON net.SUBMANUFACTURERSERIALNUM = mts.MANUFACTURERSERIALNUM_PK
                AND net.SUBMANUFACTURER = mts.MANUFACTURER_PK
      ORDER BY MAIN_METERREF;
    
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

  l_progress := 'loop processing';
  LOOP

    FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

    l_no_row_read := l_no_row_read + t_met.COUNT;

    FOR i IN 1..t_met.COUNT
    LOOP

        l_err.TXT_KEY := t_met(i).SPID_PK || ',' || t_met(i).MAIN_METERREF;
        l_mo := NULL;
        l_rec_exc := FALSE;
        l_rec_written := TRUE;
        l_rec_war := false;

      -- if SPID is NULL then reject the record - we can't have a meter with no SPID
      IF t_met(i).SPID_PK IS NULL THEN
        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('Cannot add a meter with no SPID',1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
        l_no_row_exp := l_no_row_exp + 1;
        l_rec_written := FALSE;
      ELSE      -- reject water SPIDs - we only load meter info related to sewerage SPIDs
        IF SUBSTR(t_met(i).SPID_PK,11,1) = 'W' THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR('Cannot add a water meter for OWC '  || t_MET(i).OWC,1,100),  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;    
        ELSE        
          l_mo.MAIN_METERREF :=  t_met(i).MAIN_METERREF;
          l_mo.MAIN_STWPROPERTYNUMBER_PK :=  t_met(i).MAIN_STWPROPERTYNUMBER_PK;
          l_mo.MASTER_PROPERTY  := t_met(i).MAIN_STWPROPERTYNUMBER_PK;
          l_mo.MAIN_MANUFACTURER_PK :=  t_met(i).MAINMANUFACTURER;
          l_mo.MAIN_MANSERIALNUM_PK :=  t_met(i).MAINMANUFACTURERSERIALNUM;
          l_mo.MAIN_SPID :=  t_met(i).SPID_PK;
          l_mo.SUB_METERREF :=  t_met(i).SUB_METERREF;
          l_mo.SUB_STWPROPERTYNUMBER_PK :=  t_met(i).SUB_STWPROPERTYNUMBER_PK;
          l_mo.SUB_MANUFACTURER_PK :=  t_met(i).SUBMANUFACTURER;
          l_mo.SUB_MANSERIALNUM_PK :=  t_met(i).SUBMANUFACTURERSERIALNUM;
          l_mo.SUB_SPID :=  t_met(i).SUB_SPID;
          l_mo.AGG_NET_FLAG :=  t_met(i).MAINNONMARKETFLAG;
          l_mo.MAIN_MANUFCODE := NULL;
          l_mo.SUB_MANUFCODE := null;
  
  
          l_progress := 'INSERT MO_METER_NETWORK';
          BEGIN
              INSERT INTO MO_METER_NETWORK
              (AGG_NET_FLAG,	MAIN_MANSERIALNUM_PK,	MAIN_MANUFACTURER_PK,	MAIN_MANUFCODE,	MAIN_METERREF,	MAIN_SPID,	MASTER_PROPERTY,
               SUB_MANSERIALNUM_PK,	SUB_MANUFACTURER_PK,	SUB_MANUFCODE,	SUB_METERREF,	SUB_SPID,	MAIN_STWPROPERTYNUMBER_PK,	SUB_STWPROPERTYNUMBER_PK )
              VALUES
              (l_mo.AGG_NET_FLAG,	l_mo.MAIN_MANSERIALNUM_PK,	l_mo.MAIN_MANUFACTURER_PK,	l_mo.MAIN_MANUFCODE,	l_mo.MAIN_METERREF,	l_mo.MAIN_SPID,	l_mo.MASTER_PROPERTY,
               l_mo.SUB_MANSERIALNUM_PK,	l_mo.SUB_MANUFACTURER_PK,	l_mo.SUB_MANUFCODE,	l_mo.SUB_METERREF,	l_mo.SUB_SPID,	l_mo.MAIN_STWPROPERTYNUMBER_PK,	l_mo.SUB_STWPROPERTYNUMBER_PK);
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
                L_JOB.IND_STATUS := 'ERR';
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                commit;
                return_code := -1;
                RETURN;
              END IF;
          END;
  
          IF l_rec_written THEN
              l_no_row_insert := l_no_row_insert + 1;
              l_curr_meter_written := TRUE;
              IF (l_prev_met <> t_met(i).MAIN_METERREF) THEN
                  l_no_meter_written := l_no_meter_written + 1;
              END IF;
  --              l_prev_am_reading := t_met(i).AM_READING;
  --              l_prev_readdate := TO_CHAR(t_met(i).TS_CAPTURED, 'DD-MON-YYYY');
          ELSE
              l_no_row_dropped := l_no_row_dropped + 1;
              IF (l_prev_met <> t_met(i).MAIN_METERREF AND l_curr_meter_written = FALSE) THEN
                  l_no_meter_dropped := l_no_meter_dropped + 1;
              END IF;
          END IF;
          l_prev_met := t_met(i).MAIN_METERREF;

         END IF; -- if SPID is not water SPID
        END IF; -- if SPID is NULL  
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
----  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1160, l_no_row_dropped, 'Meter Readings dropped during Transform');
----  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1161, l_no_meter_dropped,  'Distinct Meters dropped during Transform');  -- CR01 - add recon counts for METERS
--  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1170, l_no_row_insert,  'Meter Readings written to MO_METER_READING during Transform');
----  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1171, l_no_meter_written,  'Distinct Meters written during Transform');  -- CR01 - add recon counts for METERS

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  commit;

EXCEPTION
WHEN OTHERS THEN
      l_errOR_NUMBER := SQLCODE;
      l_errOR_MESSAGE := SUBSTR(SQLERRM,1,512);
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(l_errOR_MESSAGE,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      commit;
      RETURN_CODE := -1;
end P_OWC_TRAN_METER_NETWORK;
/
show error;

exit; 