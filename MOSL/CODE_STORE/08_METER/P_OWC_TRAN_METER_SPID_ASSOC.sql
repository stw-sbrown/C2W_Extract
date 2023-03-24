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
-- Subversion $Revision: 5802 $
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
-- V 0.02      11/10/2016  K.Burton   Changes to process DWRCYMRU-W data and add reconciliations
-----------------------------------------------------------------------------------------
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

  l_owc_measure                 LU_OWC_RECON_MEASURES%ROWTYPE;
   
  l_no_row_read_owc             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 
  l_no_row_dropped_owc          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 
  l_no_row_insert_owc           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE := 0; 

  owc_meter_spid_exception EXCEPTION;
  owc_over_tolerance_exception EXCEPTION;      

  CURSOR cur_met (p_owc VARCHAR2) IS
    SELECT MSP.MANUFACTURER_PK,
           MSP.MANUFACTURERSERIALNUM_PK,
           MSP.SPID_PK,
           MM.METERREF,
           MM.INSTALLEDPROPERTYNUMBER,
           MSP.OWC
    FROM RECEPTION.OWC_METER_SUPPLY_POINT MSP,
         MO_METER MM
    WHERE MM.MANUFACTURER_PK(+) = MSP.MANUFACTURER_PK
    AND MM.MANUFACTURERSERIALNUM_PK(+) = MSP.MANUFACTURERSERIALNUM_PK
    AND MSP.OWC = p_owc;

  CURSOR owc_cur IS
    SELECT DISTINCT OWC FROM RECEPTION.OWC_METER;  
    
  FUNCTION GET_OWC_MEASURES (p_owc VARCHAR2, p_table VARCHAR2) RETURN LU_OWC_RECON_MEASURES%ROWTYPE IS
    l_owc_measure LU_OWC_RECON_MEASURES%ROWTYPE;
  BEGIN
    SELECT * INTO l_owc_measure
    FROM LU_OWC_RECON_MEASURES 
    WHERE OWC = p_owc
    AND MO_TABLE = p_table;
    
    RETURN l_owc_measure;
  END GET_OWC_MEASURES;     

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
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(NO_BATCH, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  FOR owc IN owc_cur
  LOOP

    l_no_row_read_owc := 0; 
    l_no_row_dropped_owc := 0;
    l_no_row_insert_owc := 0;
  
    FOR mtr IN cur_met(owc.OWC)
    LOOP
      l_no_row_read := l_no_row_read + 1;
      l_no_row_read_owc := l_no_row_read_owc + 1;

      l_err.TXT_KEY := mtr.MANUFACTURERSERIALNUM_PK;
        
      BEGIN
        IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
          RAISE owc_over_tolerance_exception;
        END IF;        
        -- reject water SPIDs - we only load meter info related to sewerage SPIDs
        IF SUBSTR(mtr.SPID_PK,11,1) = 'W' THEN
          l_error_message := 'Cannot add a water meter for OWC '  || mtr.OWC;
          RAISE owc_meter_spid_exception;
        ELSE  -- if we get here we should only have sewerage meter to add - should only happen for DWRCYMRU
          l_progress := 'INSERT MO_METER_SPID_ASSOC';
          BEGIN
            INSERT INTO MO_METER_SPID_ASSOC 
            (MANUFACTURER_PK,MANUFACTURERSERIALNUM_PK,MANUFCODE,METERREF,SPID,STWPROPERTYNUMBER_PK, OWC)
            VALUES
            (mtr.MANUFACTURER_PK, mtr.MANUFACTURERSERIALNUM_PK, NULL,	mtr.METERREF, mtr.SPID_PK, mtr.INSTALLEDPROPERTYNUMBER, owc.OWC);
              
            l_no_row_insert := l_no_row_insert + 1;
            l_no_row_insert_owc := l_no_row_insert_owc + 1;
          EXCEPTION
            WHEN OTHERS THEN
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              RAISE owc_meter_spid_exception;
          END;
        END IF; -- if SPID is not water SPID

      EXCEPTION
        WHEN owc_meter_spid_exception THEN
          l_no_row_exp := l_no_row_exp + 1;
          l_no_row_dropped := l_no_row_dropped + 1;
          l_no_row_dropped_owc := l_no_row_dropped_owc + 1;
          P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'X', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
        WHEN owc_over_tolerance_exception THEN
          l_job.IND_STATUS := 'ERR';
          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded - Dropping bad data',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
          P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
          COMMIT;
          return_code := -1;
          RETURN; -- quit proc          
      END;
    END LOOP; -- cur_met
  
    -- write OWC specific counts 
    l_progress := 'Writing OWC counts ' || owc.OWC;  
    
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_METER_SPID_ASSOC');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_owc, owc.OWC || ' Meter to SPID associations read during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_owc, owc.OWC || ' Meter to SPID associations dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_owc, owc.OWC || ' Meter to SPID associations written to MO_METER_SPID_ASSOC during Transform'); 

  END LOOP; -- owc_cur
  
  -- write counts
  l_progress := 'Writing Total Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP48', 2510, l_no_row_read,    'Meter to SPID associations read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP48', 2520, l_no_row_dropped, 'Meter to SPID associations dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.NO_INSTANCE, 'CP48', 2530, l_no_row_insert,  'Meter to SPID associations  written to MO_METER_SPID_ASSOC during Transform');

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  COMMIT;

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