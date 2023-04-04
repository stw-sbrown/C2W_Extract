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
-- Subversion $Revision: 5969 $
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
-- V 0.02      11/10/2016  K.Burton   Changes to process DWRCYMRU-W data and add reconciliations
-- V 0.03      26/10/2016  D.Cheung   Fix Reconciliation Count (meter insert)
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_METER_READING'; 
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mt                          LU_METER_MANUFACTURER%ROWTYPE;  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_manufserialchk              NUMBER;
  l_no_row_read_owc             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_owc          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_owc           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;

  l_no_row_read_mtr             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_mtr          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_mtr           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  
  l_curr_meter_written          BOOLEAN;
  l_initialmeterreaddate        MO_METER_READING.INITIALMETERREADDATE%TYPE;
  
  l_owc_measure                 LU_OWC_RECON_MEASURES%ROWTYPE;
  
  owc_meter_reading_exception EXCEPTION;
  owc_over_tolerance_exception EXCEPTION;    

  CURSOR cur_met (p_owc VARCHAR2, p_metref VARCHAR2) IS
    SELECT DISTINCT mrd.MANUFACTURERSERIALNUM_PK,
        mrd.MANUFACTURER_PK,
        mrd.METERREAD,
        mrd.METERREADMETHOD,
        mrd.METERREADDATE,
        mrd.ROLLOVERINDICATOR,
        mrd.ESTIMATEDREADREASONCODE,
        CASE WHEN METERREADMETHOD <> 'ESTIMATED' 
          THEN NULL
          ELSE mrd.ESTIMATEDREADREMEDIALWORKIND
        END AS ESTIMATEDREADREMEDIALWORKIND,
        mrd.METERREADTYPE,
        mrd.previousmeterreading,
        mt.METERREF as SAPEQUIPMENT ,
        mrd.PREVMETERREF,
        mtr.INITIALMETERREADDATE,
        NULL MANUFCODE,
        NULL METERREADERASEDFLAG,
        NULL METERREADREASONTYPE,
        NULL METERREADSETTLEMENTFLAG,
        NULL METERREADSTATUS,
        NULL METRREADDATEFORREMOVAL,
        NULL PREVVALCDVCANDIDATEDAILYVOLUME,
        NULL RDAOUTCOME,
        0 REREADFLAG,
        mrd.ROLLOVERINDICATOR as ROLLOVERFLAG,
        mt.INSTALLEDPROPERTYNUMBER,
        mtr.SPID,
        mrd.OWC
    FROM RECEPTION.OWC_METER_READING mrd,
         RECEPTION.OWC_METER mtr,
         MO_METER mt
    WHERE mrd.OWC = p_owc
    AND mt.METERREF = p_metref
    AND mrd.MANUFACTURERSERIALNUM_PK = mtr.MANUFACTURERSERIALNUM_PK 
    AND mrd.MANUFACTURER_PK = mtr.MANUFACTURER_PK
    AND mt.MANUFACTURERSERIALNUM_PK = mtr.MANUFACTURERSERIALNUM_PK 
    AND mt.MANUFACTURER_PK = mtr.MANUFACTURER_PK
    ORDER BY  mrd.MANUFACTURERSERIALNUM_PK,mrd.MANUFACTURER_PK;
    
  CURSOR owc_cur IS
    SELECT DISTINCT OWC FROM RECEPTION.OWC_METER_READING;   
    
  CURSOR met_cur (p_owc VARCHAR2) IS
    SELECT METERREF 
    FROM MO_METER 
    WHERE OWC = p_owc;
  
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
   l_job.IND_STATUS := 'RUN';

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
      P_MIG_BATCH.FN_UPDATEJOB(NO_BATCH, L_JOB.NO_INSTANCE, L_JOB.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  l_no_row_read_mtr := 0;
  l_no_row_dropped_mtr := 0;
  l_no_row_insert_mtr := 0;  
  
  FOR owc in owc_cur
  LOOP

    l_no_row_read_owc := 0;
    l_no_row_dropped_owc := 0;
    l_no_row_insert_owc := 0;

    FOR met IN met_cur(owc.OWC)
    LOOP
      -- individual meter count 
      l_no_row_read_mtr := l_no_row_read_mtr + 1;
      l_curr_meter_written := FALSE;
      
      -- process all records for range supplied
      FOR mtr IN cur_met(owc.OWC,met.METERREF)
      LOOP
        l_no_row_read := l_no_row_read + 1;  -- total rows read count
        l_no_row_read_owc := l_no_row_read_owc + 1;  -- rows (readings) per OWC
        
        l_err.TXT_KEY := mtr.MANUFACTURERSERIALNUM_PK;
            
        BEGIN  
          IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
            RAISE owc_over_tolerance_exception;
          END IF;          
          -- if SPID is NULL then reject the record - we can't have a meter with no SPID
          IF mtr.SPID IS NULL THEN
            l_error_message := 'Cannot add a meter with no SPID';
            RAISE owc_meter_reading_exception;
          ELSE      -- reject water SPIDs - we only load meter info related to sewerage SPIDs
            IF SUBSTR(mtr.SPID,11,1) = 'W' THEN
              l_error_message := 'Cannot add a water meter for OWC '  || mtr.OWC;
              RAISE owc_meter_reading_exception;              
            ELSE  -- if we get here we should only have sewerage meter to add - should only happen for DWRCYMRU
              l_progress := 'INSERT MO_METER_READING';
              BEGIN
                INSERT INTO MO_METER_READING
                (ESTIMATEDREADREASONCODE,	ESTIMATEDREADREMEDIALWORKIND,	INITIALMETERREADDATE,	INSTALLEDPROPERTYNUMBER,	MANUFACTURER_PK,	MANUFACTURERSERIALNUM_PK
                ,	MANUFCODE,	METERREAD,	METERREADDATE,	METERREADERASEDFLAG,	METERREADMETHOD,	METERREADREASONTYPE,	METERREADSETTLEMENTFLAG,	METERREADSTATUS
                ,	METERREADTYPE,	METERREF,	METRREADDATEFORREMOVAL,	PREVVALCDVCANDIDATEDAILYVOLUME,	RDAOUTCOME,	REREADFLAG,	ROLLOVERFLAG,	ROLLOVERINDICATOR,OWC )
                VALUES
                (mtr.ESTIMATEDREADREASONCODE,	mtr.ESTIMATEDREADREMEDIALWORKIND,	mtr.INITIALMETERREADDATE,	mtr.INSTALLEDPROPERTYNUMBER,	mtr.MANUFACTURER_PK,	
                mtr.MANUFACTURERSERIALNUM_PK, mtr.MANUFCODE, mtr.METERREAD,	mtr.METERREADDATE,	mtr.METERREADERASEDFLAG,	mtr.METERREADMETHOD,
                mtr.METERREADREASONTYPE,	mtr.METERREADSETTLEMENTFLAG,	mtr.METERREADSTATUS, mtr.METERREADTYPE,	mtr.SAPEQUIPMENT,	mtr.METRREADDATEFORREMOVAL,
                mtr.PREVVALCDVCANDIDATEDAILYVOLUME,	mtr.RDAOUTCOME,	mtr.REREADFLAG,	mtr.ROLLOVERFLAG,	mtr.ROLLOVERINDICATOR, owc.OWC );
                
                l_no_row_insert := l_no_row_insert + 1;
                l_no_row_insert_owc := l_no_row_insert_owc + 1;
                l_curr_meter_written := TRUE;
              EXCEPTION
                WHEN OTHERS THEN
                   l_error_number := SQLCODE;
                   l_error_message := SQLERRM;
                   RAISE owc_meter_reading_exception;
              END;
            END IF; -- if SPID is not water SPID
          END IF; -- if SPID is NULL   

        EXCEPTION
          WHEN owc_meter_reading_exception THEN
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
      
      -- at least one reading written for the meter so count the meter as having been inserted  --v0.03
      IF l_curr_meter_written THEN
          l_no_row_insert_mtr := l_no_row_insert_mtr + 1;
      END IF;
    
      COMMIT;
    END LOOP; -- met_cur
    
    -- write OWC specific counts 
    l_progress := 'Writing OWC counts ' || owc.OWC;  
    
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_METER_READING');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_owc, owc.OWC || ' Meter readings read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_owc, owc.OWC || ' Meter readings dropped during Transform');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_owc, owc.OWC || ' Meter readings written to MO_METER_READING during Transform'); 
    
  END LOOP; -- owc_cur
  
  -- write counts
  l_progress := 'Writing Total Counts';

  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1150, l_no_row_read,    'OWC Distinct Meter Readings read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1151, l_no_row_read_mtr,  'OWC Distinct Meters read during Transform'); 
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1160, l_no_row_dropped, 'OWC Meter Readings dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1161, l_no_row_dropped_mtr,  'OWC Distinct Meters dropped during Transform'); 
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1170, l_no_row_insert,  'OWC Meter Readings written to MO_METER_READING during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1171, l_no_row_insert_mtr,  'OWC Distinct Meters written during Transform'); 

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  COMMIT;

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