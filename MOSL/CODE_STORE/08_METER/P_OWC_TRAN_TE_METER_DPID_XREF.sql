create or replace
PROCEDURE P_OWC_TRAN_TE_METER_DPID_XREF (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                         no_job       IN MIG_JOBREF.NO_JOB%TYPE,
                                         return_code  IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: TE Meter DPID XREF import
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_OWC_TRAN_TE_METER_NETWORK.sql
--
-- Subversion $Revision: 6260 $
--
-- CREATED        : 03/10/2016
--
-- DESCRIPTION    : Procedure to populate MO_METER_DPIDXREF table with OWC TE meters
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      03/10/2016  K.Burton   Initial Draft
-- V 0.02      05/10/2016  K.Burton   Added batch control code and recon measures
-- V 0.03      06/10/2016  K.Burton   Bug fixes following initial testing
-- V 0.04      14/11/2016  K.Burton   1. Added warning message for no meter match by serial number
--                                    2. Updated error for no match on counts to show counts
--                                    3. Change validation for DWRCYMRU-W to check sewerage meter supply point file  
-- V 0.05      15/11/2016  S.Badhan   Make error message more generic and add data to error data field.
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_OWC_TRAN_TE_METER_DPID_XREF';  
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    MO_METER_READING.METERREF%type; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mo                          MO_METER_DPIDXREF%ROWTYPE; --modify
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_owc             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert_owc           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_owc          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
--  l_rec_written                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_rec_war                     BOOLEAN;
  l_no_meter_read               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_written            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_meter_dropped            MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_curr_meter_written          BOOLEAN;
  
--  l_owc_exception_msg  VARCHAR2(2000);
  l_meter_manufacturer VARCHAR2(32);
  l_meter_serial VARCHAR2(32);
  l_te_count NUMBER;
  l_te_match BOOLEAN;
  l_owc_measure LU_OWC_RECON_MEASURES%ROWTYPE;

  
  CURSOR te_met_cur (p_owc VARCHAR2, p_iwcs VARCHAR2) IS
    SELECT
      NVL((SELECT MAX(METERDPIDXREF_PK) FROM MO_METER_DPIDXREF),0) + ROWNUM METERDPIDXREF_PK,
      DPID,
      STW_PROPERTYNUMBER,
      OWC_SPID,
      OWC_METERSERIAL,
      OWC_METERMANUFACTURER,
      OWC_PROPERTYNUMBER,
--      QUIS,
      ACCOUNT_NUMBER,
      NO_METERS,
      OWC      
    FROM (SELECT DISTINCT BT.NO_IWCS DPID,
              LU.STW_PROPERTYNUMBER,
              LU.OWC_SPID,
              LU.OWC_METERSERIAL,
              LU.OWC_METERMANUFACTURER,
              LU.OWC_PROPERTYNUMBER,
--              LU.QUIS,
              BT.ACCOUNT_NUMBER,
              COUNT(DISTINCT LU.OWC_METERSERIAL) OVER (PARTITION BY BT.NO_IWCS) NO_METERS,
              LU.OWC
          FROM LU_OWC_TE_METERS LU, 
               BT_OWC_TE_DPID_REF BT
          WHERE LU.OWC = p_owc
          AND BT.NO_IWCS = p_iwcs
          AND LU.OWC = BT.OWC
--          AND LU.QUIS = BT.QUIS
          AND LU.ACCOUNT_NUMBER = BT.ACCOUNT_NUMBER
          AND LU.STW_PROPERTYNUMBER = BT.STW_PROPERTYNUMBER
          ORDER BY LU.OWC,BT.NO_IWCS);
        
  CURSOR owc_cur IS
    SELECT DISTINCT OWC FROM LU_OWC_TE_METERS;

  CURSOR dpid_cur (p_owc VARCHAR2) IS
    SELECT DISTINCT NO_IWCS DPID FROM BT_OWC_TE_DPID_REF WHERE OWC = p_owc;
    
  owc_meter_validation_exception EXCEPTION;
  owc_meter_match_exception EXCEPTION;
  owc_over_tolerance_exception EXCEPTION;

  FUNCTION GET_OWC_MEASURES (p_owc VARCHAR2, p_table VARCHAR2) RETURN LU_OWC_RECON_MEASURES%ROWTYPE IS
    l_owc_measure LU_OWC_RECON_MEASURES%ROWTYPE;
  BEGIN
--    DBMS_OUTPUT.PUT_LINE(p_owc || ',' || p_table);
    SELECT * INTO l_owc_measure
    FROM LU_OWC_RECON_MEASURES 
    WHERE OWC = p_owc
    AND MO_TABLE = p_table;
    
    RETURN l_owc_measure;
  END GET_OWC_MEASURES;
  
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
   l_progress := 'Processing ';

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(NO_BATCH, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  FOR owc IN owc_cur
  LOOP
  
    -- reset OWC specific counts
    l_no_row_read_owc := 0;
    l_no_row_dropped_owc := 0;
    l_no_row_insert_owc := 0;

    FOR dpid IN dpid_cur(owc.OWC)
    LOOP
      FOR tm IN te_met_cur(owc.OWC, dpid.DPID)
      LOOP
        l_err.TXT_KEY := owc.OWC || ',' || dpid.DPID;
        l_err.TXT_DATA := C_MODULE_NAME;
        l_te_match := FALSE;  -- assume no match for TE meter will be found
        l_no_row_read := l_no_row_read + 1;
        l_no_row_read_owc := l_no_row_read_owc + 1;
        
        BEGIN
          IF (l_no_row_exp > l_job.EXP_TOLERANCE OR l_no_row_err > l_job.ERR_TOLERANCE OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
            RAISE owc_over_tolerance_exception;
          END IF;
          
          l_progress := 'Validating lookup data';
  
          IF tm.OWC_SPID IS NOT NULL THEN
            -- if we have sewerage SPID in lookup - check meter supply point file 
            -- to see if meter can be found and make sure they match
            IF tm.NO_METERS = 0 THEN
              l_error_message := 'No meter details provided by ' || owc.OWC || ' for SPID ' ;
              l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' || tm.OWC_SPID,1,100);
              RAISE owc_meter_validation_exception;
            END IF;

            IF owc.OWC = 'DWRCYMRU-W' THEN
              -- For DWRCYMRU-W need to validate mete against sewerage meter supply point file
              SELECT COUNT(*)
              INTO l_te_count
              FROM RECEPTION.OWC_METER_SUPPLY_POINT MSP
              WHERE MSP.OWC = owc.OWC
              AND SUBSTR(MSP.SPID_PK,1,10) = SUBSTR(tm.OWC_SPID,1,10)
              AND MSP.MANUFACTURER_PK = tm.OWC_METERMANUFACTURER
              AND MSP.MANUFACTURERSERIALNUM_PK = tm.OWC_METERSERIAL;
            ELSE
              SELECT COUNT(*)
              INTO l_te_count
              FROM RECEPTION.OWC_METER_SUPPLY_POINT_W MSP
              WHERE MSP.OWC = owc.OWC
              AND SUBSTR(MSP.SPID_PK,1,10) = SUBSTR(tm.OWC_SPID,1,10)
              AND MSP.MANUFACTURER_PK = tm.OWC_METERMANUFACTURER
              AND MSP.MANUFACTURERSERIALNUM_PK = tm.OWC_METERSERIAL;
            END IF;
            
            IF l_te_count = 0 THEN
              -- report an error/warning here and move on
              l_error_message := 'No matching meter found in ' || owc.OWC || ' meter supply point file for SPID ';
              l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' || tm.OWC_SPID,1,100);                               
              RAISE owc_meter_validation_exception;
            END IF;

            IF l_te_count > 1 THEN
              -- report an error/warning here and move on
              l_error_message := 'Duplicate matching meters found in ' || owc.OWC || ' meter supply point: ' ;
              l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' || tm.OWC_SPID || ',' || tm.OWC_METERMANUFACTURER || ',' || tm.OWC_METERSERIAL,1,100);   
              RAISE owc_meter_validation_exception;
            END IF;
          ELSE
            -- report an error/warning here and move on
            l_error_message := 'No waste SPID provided by ' || owc.OWC || ' for STW property number ';
            l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' || tm.OWC_SPID || ',' || tm.STW_PROPERTYNUMBER,1,100);   
            RAISE owc_meter_validation_exception;     
          END IF;     
  
          l_progress := 'Matching against TE_WORKING_V';
          
          -- If we get this far the meter has passed initial validation - so now try to match to TE_WORKING_V
          -- First try to match meter against TE_WORKING_V for by DPID and SERIAL number
          SELECT COUNT(*) 
          INTO l_te_count
          FROM TE_WORKING_V
          WHERE HAS_CROSS_BORDER_WATER_YN = 'Y'
          AND UPPER(TE_CATEGORY) = 'WATER METER'
          AND NO_IWCS = tm.DPID
          AND NO_ACCOUNT = tm.ACCOUNT_NUMBER
          AND UPPER(SERIAL_NO) = UPPER(tm.OWC_METERSERIAL);
          
          IF l_te_count = 0 THEN
            l_error_message := 'No TE serial number match found for ' || owc.OWC || ' serial ';
            l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' || tm.OWC_METERSERIAL,1,100);
            -- report warning message but continue to next matching check
            l_no_row_war := l_no_row_war + 1;
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'W', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  l_err.TXT_DATA);
            l_err.TXT_DATA := C_MODULE_NAME;

            -- check by number of meters found
            SELECT COUNT(*) 
            INTO l_te_count
            FROM TE_WORKING_V
            WHERE HAS_CROSS_BORDER_WATER_YN = 'Y'
            AND UPPER(TE_CATEGORY) = 'WATER METER'
            AND NO_IWCS = tm.DPID
            AND NO_ACCOUNT = tm.ACCOUNT_NUMBER;
            
            IF l_te_count = 0 THEN
              l_error_message := 'No TE water meters found for DPID ' || tm.DPID;
              l_no_row_war := l_no_row_war + 1;
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'W', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
--              RAISE owc_meter_match_exception;   
            END IF;
            
            IF l_te_count > 0 AND  l_te_count <> tm.NO_METERS THEN
              l_error_message := 'Number of meters provided for DPID by ' || owc.OWC || ' does not match TE';
              l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' ||  tm.DPID  || ', (' || tm.NO_METERS || ') does not match TE (' || l_te_count || ')', 1,100);
              l_no_row_war := l_no_row_war + 1;
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'W', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
--            ELSE
--              RAISE owc_meter_match_exception;
            END IF;
          ELSIF l_te_count > 1 THEN
            l_error_message := 'More than one matching TE serial number found for ' || owc.OWC || ' serial ' ;
            l_err.TXT_DATA := substr(l_err.TXT_DATA || ',' || l_progress || ',' || tm.OWC_METERSERIAL,1,100);   
            RAISE owc_meter_validation_exception;
--          ELSE
--            l_te_match := TRUE; -- we found exact match
          END IF;
        l_progress := 'Inserting record to MO_METER_DPIDXREF';
          
          -- if we get this far we have found a match (either exact or numerical) so write the record to MO_METER_DPIDXREF
          BEGIN
            INSERT INTO MO_METER_DPIDXREF
            (
              METERDPIDXREF_PK, MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, DPID_PK,
              INITIALMETERREADDATE, SPID, DPEFFECTFROMDATE, EFFECTIVEFROMDATE, EFFECTIVETODATE,
              PERCENTAGEDISCHARGE, MANUFCODE, INSTALLEDPROPERTYNUMBER, OWC
            )
            VALUES
            (
              tm.METERDPIDXREF_PK, tm.OWC_METERMANUFACTURER, tm.OWC_METERSERIAL, tm.DPID,
              NULL, tm.OWC_SPID, NULL, NULL, NULL,
              0, tm.OWC_METERMANUFACTURER, tm.STW_PROPERTYNUMBER, owc.OWC
            );  
            
            l_no_row_insert := l_no_row_insert + 1;
            l_no_row_insert_owc := l_no_row_insert_owc + 1;
          EXCEPTION
            WHEN OTHERS THEN
              l_error_message := SQLERRM;
              l_err.TXT_DATA := SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100);
              RAISE owc_meter_match_exception;
          END;
        
        EXCEPTION
          WHEN owc_meter_validation_exception THEN
            l_no_row_exp := l_no_row_exp + 1;
            l_no_row_dropped := l_no_row_dropped + 1;
            l_no_row_dropped_owc := l_no_row_dropped_owc + 1;
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'X', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  l_err.TXT_DATA);
            NULL; -- move on to next record
          WHEN owc_meter_match_exception THEN
            l_no_row_exp := l_no_row_exp + 1;
            l_no_row_dropped := l_no_row_dropped + 1;
            l_no_row_dropped_owc := l_no_row_dropped_owc + 1;
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, l_job.NO_INSTANCE, 'X', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  l_err.TXT_DATA);
            EXIT; -- exit te_met_cur loop
          WHEN owc_over_tolerance_exception THEN
            l_job.IND_STATUS := 'ERR';
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded - Dropping bad data',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
            P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
            commit;
            return_code := -1;
            RETURN; -- quit proc       
        END;
      END LOOP; -- te_met_cur
    END LOOP; -- dpid_cur
    
    l_owc_measure := GET_OWC_MEASURES(owc.OWC,'MO_METER_DPIDXREF');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_READ_MEASURE, l_no_row_read_owc, owc.OWC || ' TE meters read during import');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_DROPPED_MEASURE, l_no_row_dropped_owc, owc.OWC || ' TE meters dropped during import');   
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.no_instance, l_owc_measure.CONTROL_POINT, l_owc_measure.OBJ_INSERTED_MEASURE, l_no_row_insert_owc, owc.OWC || ' TE meters written to MO_METER_DPIDXREF during import'); 
    
    
    COMMIT;
  END LOOP; -- owc_cur

  -- write counts
  l_progress := 'Writing Total Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2630, l_no_row_read,    'Total TE meters reads during import');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2640, l_no_row_dropped, 'Total TE meters dropped during import');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2650, l_no_row_insert,  'Total TE meters written to MO_METER_DPIDXREF during import');  

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
      l_error_number := SQLCODE;
      l_error_message := SUBSTR(SQLERRM,1,512);
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', SUBSTR(l_error_message,1,100),  l_err.TXT_KEY,  SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      COMMIT;
      return_code := -1;
END P_OWC_TRAN_TE_METER_DPID_XREF;
/
exit;