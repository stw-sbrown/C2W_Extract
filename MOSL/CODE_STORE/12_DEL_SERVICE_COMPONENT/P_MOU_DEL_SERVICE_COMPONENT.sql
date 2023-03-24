create or replace
PROCEDURE P_MOU_DEL_SERVICE_COMPONENT (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                               no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                               return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Service Component Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_MOU_DEL_SERVICE_COMPONENT.sql
--
-- Subversion $Revision: 5456 $
--
-- CREATED        : 11/04/2016
--
-- DESCRIPTION    : Procedure to create the Supply Point MOSL Upload file
--                  Queries Transform tables and populates table DEL_SERVICE_COMPONENT
--                  Writes to file SERVICE_COMPONENT_SEVERN-W_<date/timestamp>.dat

-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V0.01       07/04/2016  K.Burton   Initial Draft
-- V0.02       21/04/2016  K.Burton   Added PIPESIZE to UNMEASURED WATER / SEWERAGE ouputs
-- V0.03       26/04/2016  K.Burton   Changed call to ArchiveDeliveryTable procedure
--                                    now calls P_DEL_UTIL_ARCHIVE_TABLE stored procedure
-- V0.04       28/04/2016  K.Burton   Corrections to cursor for repeated columns for
--                                    Special Agreement Flag / Factor / Ref
-- V1.01       16/05/2016  K.Burton   Changes to file write logic to accomodate cross border
--                                    files
-- V1.02       11/07/2016  L.Smith    I-283. Count of MO service components flattened to table DEL_SERVICE_COMPONENT
-- V1.03       25/08/2016  S.Badhan   I-320. If user FINDEL use directory FINEXPORT.
-- V1.04       01/09/2016  K.Burton   Updates for splitting STW data into 3 batches
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_DEL_SERVICE_COMPONENT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  l_no_row_dropped_cb           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- count of cross border rows NOT output to any file
  l_no_row_written              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_count                       NUMBER;
  l_no_sct_count                NUMBER;
  l_filepath VARCHAR2(30) := 'DELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100) := 'DEL_SERVICE_COMPONENT';

  l_sql VARCHAR2(2000);
--  fHandle UTL_FILE.FILE_TYPE;

  -- Cross Border Control cursor
  CURSOR cb_cur IS
    SELECT WHOLESALER_ID, RUN_FLAG
    FROM BT_CROSSBORDER_CTRL;
--    WHERE RUN_FLAG = 1;
    
  CURSOR cur_service_component IS
    SELECT SP.SPID_PK,
           -- METERED POTABLE WATER
           MPW.TARIFFCODE_PK METEREDPWTARIFFCODE,
           MPW.SPECIALAGREEMENTFLAG MPWSPECIALAGREEMENTFLAG,
           MPW.SPECIALAGREEMENTFACTOR MPWSPECIALAGREEMENTFACTOR,
           MPW.SPECIALAGREEMENTREF MPWSPECIALAGREEMENTREF,
           MPW.METEREDPWMAXDAILYDEMAND,
           MPW.MPWMAXIMUMDEMANDTARIFF, -- NOT OUTPUT TO FILE
           MPW.DAILYRESERVEDCAPACITY,
           MPW.MPWDAILYSTANDBYUSAGEVOLCHARGE, -- NOT OUTPUT TO FILE
           -- METERED NON-POTABLE WATER
           MNPW.TARIFFCODE_PK METEREDNPWTARIFFCODE,
           MNPW.SPECIALAGREEMENTFLAG MNPWSPECIALAGREEMENTFLAG,
           MNPW.SPECIALAGREEMENTFACTOR MNPWSPECIALAGREEMENTFACTOR,
           MNPW.SPECIALAGREEMENTREF MNPWSPECIALAGREEMENTREF,
           MNPW.METEREDNPWMAXDAILYDEMAND,
           MNPW.MNPWMAXIMUMDEMANDTARIFF, -- NOT OUTPUT TO FILE
           MNPW.METEREDNPWDAILYRESVDCAPACITY,
           MNPW.MNPWDAILYSTANDBYUSAGEVOLCHARGE, -- NOT OUTPUT TO FILE
           -- ASSESSED WATER
           AW.TARIFFCODE_PK AWASSESSEDTARIFFCODE,
           AW.SPECIALAGREEMENTFLAG AWSPECIALAGREEMENTFLAG,
           AW.SPECIALAGREEMENTFACTOR AWSPECIALAGREEMENTFACTOR,
           AW.SPECIALAGREEMENTREF AWSPECIALAGREEMENTREF,
           AW.ASSESSEDCHARGEMETERSIZE AWASSESSEDCHARGEMETERSIZE,
           AW.ASSESSEDDVOLUMETRICRATE AWASSESSEDDVOLUMETRICRATE,
           AW.ASSESSEDTARIFBAND AWASSESSEDTARIFBAND,
           AW.AWFIXEDCHARGE, -- NOT OUTPUT TO FILE
           AW.AWVOLUMETRICCHARGE, -- NOT OUTPUT TO FILE
           AW.AWTARIFFBAND, -- NOT OUTPUT TO FILE
           -- UNMEASURED WATER
           UW.TARIFFCODE_PK UWUNMEASUREDTARIFFCODE,
           UW.SPECIALAGREEMENTFLAG UWSPECIALAGREEMENTFLAG,
           UW.SPECIALAGREEMENTFACTOR UWSPECIALAGREEMENTFACTOR,
           UW.SPECIALAGREEMENTREF UWSPECIALAGREEMENTREF,
           UW.UNMEASUREDTYPEACOUNT UWUNMEASUREDTYPEACOUNT,
           UW.UNMEASUREDTYPEADESCRIPTION UWUNMEASUREDTYPEADESCRIPTION,
           UW.UNMEASUREDTYPEBCOUNT UWUNMEASUREDTYPEBCOUNT,
           UW.UNMEASUREDTYPEBDESCRIPTION UWUNMEASUREDTYPEBDESCRIPTION,
           UW.UNMEASUREDTYPECCOUNT UWUNMEASUREDTYPECCOUNT,
           UW.UNMEASUREDTYPECDESCRIPTION UWUNMEASUREDTYPECDESCRIPTION,
           UW.UNMEASUREDTYPEDCOUNT UWUNMEASUREDTYPEDCOUNT,
           UW.UNMEASUREDTYPEDDESCRIPTION UWUNMEASUREDTYPEDDESCRIPTION,
           UW.UNMEASUREDTYPEECOUNT UWUNMEASUREDTYPEECOUNT,
           UW.UNMEASUREDTYPEEDESCRIPTION UWUNMEASUREDTYPEEDESCRIPTION,
           UW.UNMEASUREDTYPEFCOUNT UWUNMEASUREDTYPEFCOUNT,
           UW.UNMEASUREDTYPEFDESCRIPTION UWUNMEASUREDTYPEFDESCRIPTION,
           UW.UNMEASUREDTYPEGCOUNT UWUNMEASUREDTYPEGCOUNT,
           UW.UNMEASUREDTYPEGDESCRIPTION UWUNMEASUREDTYPEGDESCRIPTION,
           UW.UNMEASUREDTYPEHCOUNT UWUNMEASUREDTYPEHCOUNT,
           UW.UNMEASUREDTYPEHDESCRIPTION UWUNMEASUREDTYPEHDESCRIPTION,
           UW.PIPESIZE UWPIPESIZE,
           -- WATER CHARGE ADJUSTMENT
           WCA.ADJUSTMENTSCHARGEADJTARIFFCODE WADJCHARGEADJTARIFFCODE,
           -- METERED FOUL SEWERAGE
           MFS.TARIFFCODE_PK METEREDFSTARIFFCODE,
           MFS.SPECIALAGREEMENTFLAG MFSSPECIALAGREEMENTFLAG,
           MFS.SPECIALAGREEMENTFACTOR MFSSPECIALAGREEMENTFACTOR,
           MFS.SPECIALAGREEMENTREF MFSSPECIALAGREEMENTREF,
           -- ASSESSED SEWERAGE
           ASW.TARIFFCODE_PK ASASSESSEDTARIFFCODE,
           ASW.SPECIALAGREEMENTFLAG ASSPECIALAGREEMENTFLAG,
           ASW.SPECIALAGREEMENTFACTOR ASSPECIALAGREEMENTFACTOR, -- V0.04 copy and paste error corrected
           ASW.SPECIALAGREEMENTREF ASSPECIALAGREEMENTREF,       -- V0.04 copy and paste error corrected
           ASW.ASSESSEDCHARGEMETERSIZE ASASSESSEDCHARGEMETERSIZE,
           ASW.ASSESSEDDVOLUMETRICRATE ASASSESSEDDVOLUMETRICRATE,
           ASW.ASSESSEDTARIFBAND ASASSESSEDTARIFBAND,
           ASW.ASFIXEDCHARGE, -- NOT OUTPUT TO FILE
           ASW.ASVOLMETCHARGE, -- NOT OUTPUT TO FILE
           ASW.ASTARIFFBAND, -- NOT OUTPUT TO FILE
           -- UNMEASURED SEWERAGE
           US.TARIFFCODE_PK USUNMEASUREDTARIFFCODE,
           US.SPECIALAGREEMENTFLAG USSPECIALAGREEMENTFLAG,
           US.SPECIALAGREEMENTFACTOR USSPECIALAGREEMENTFACTOR,
           US.SPECIALAGREEMENTREF USSPECIALAGREEMENTREF,
           US.UNMEASUREDTYPEACOUNT USUNMEASUREDTYPEACOUNT,
           US.UNMEASUREDTYPEADESCRIPTION USUNMEASUREDTYPEADESCRIPTION,
           US.UNMEASUREDTYPEBCOUNT USUNMEASUREDTYPEBCOUNT,
           US.UNMEASUREDTYPEBDESCRIPTION USUNMEASUREDTYPEBDESCRIPTION,
           US.UNMEASUREDTYPECCOUNT USUNMEASUREDTYPECCOUNT,
           US.UNMEASUREDTYPECDESCRIPTION USUNMEASUREDTYPECDESCRIPTION,
           US.UNMEASUREDTYPEDCOUNT USUNMEASUREDTYPEDCOUNT,
           US.UNMEASUREDTYPEDDESCRIPTION USUNMEASUREDTYPEDDESCRIPTION,
           US.UNMEASUREDTYPEECOUNT USUNMEASUREDTYPEECOUNT,
           US.UNMEASUREDTYPEEDESCRIPTION USUNMEASUREDTYPEEDESCRIPTION,
           US.UNMEASUREDTYPEFCOUNT USUNMEASUREDTYPEFCOUNT,
           US.UNMEASUREDTYPEFDESCRIPTION USUNMEASUREDTYPEFDESCRIPTION,
           US.UNMEASUREDTYPEGCOUNT USUNMEASUREDTYPEGCOUNT,
           US.UNMEASUREDTYPEGDESCRIPTION USUNMEASUREDTYPEGDESCRIPTION,
           US.UNMEASUREDTYPEHCOUNT USUNMEASUREDTYPEHCOUNT,
           US.UNMEASUREDTYPEHDESCRIPTION USUNMEASUREDTYPEHDESCRIPTION,
           US.PIPESIZE USPIPESIZE,
           -- SEWERAGE CHARGE ADJUSTMENT
           SCA.ADJUSTMENTSCHARGEADJTARIFFCODE SADJCHARGEADJTARIFFCODE,
           -- SURFACE WATER DRAINAGE
           SW.TARIFFCODE_PK SRFCWATERTARRIFCODE,
           SW.SRFCWATERAREADRAINED,
           SW.SRFCWATERCOMMUNITYCONFLAG,
           SW.SPECIALAGREEMENTFLAG SWSPECIALAGREEMENTFLAG,
           SW.SPECIALAGREEMENTFACTOR SWSPECIALAGREEMENTFACTOR,
           SW.SPECIALAGREEMENTREF SWSPECIALAGREEMENTREF,
           -- HIGHWAY DRAINAGE
           HD.TARIFFCODE_PK HWAYDRAINAGETARIFFCODE,
           HD.HWAYSURFACEAREA,
           HD.HWAYCOMMUNITYCONFLAG,
           HD.SPECIALAGREEMENTFLAG HDSPECIALAGREEMENTFLAG,
           HD.SPECIALAGREEMENTFACTOR HDSPECIALAGREEMENTFACTOR,
           HD.SPECIALAGREEMENTREF HDSPECIALAGREEMENTREF
    FROM DEL_SERVICE_COMPONENT_MPW_V MPW, -- METERED POTABLE WATER
         DEL_SERVICE_COMPONENT_MNPW_V MNPW, -- METERED NON-POTABLE WATER
         DEL_SERVICE_COMPONENT_AW_V AW, -- ASSESSED WATER
         DEL_SERVICE_COMPONENT_UW_V UW, -- UNMEASURED WATER
         DEL_SERVICE_COMPONENT_WCA_V WCA, -- WATER CHARGE ADJUSTMENT
         DEL_SERVICE_COMPONENT_MS_V MFS, -- METERED FOUL SEWERAGE
         DEL_SERVICE_COMPONENT_AS_V ASW, -- ASSESSED SEWERAGE
         DEL_SERVICE_COMPONENT_US_V US, -- UNMEASURED SEWERAGE
         DEL_SERVICE_COMPONENT_SCA_V SCA, -- SEWERAGE CHARGE ADJUSTMENT
         DEL_SERVICE_COMPONENT_SW_V SW, -- SURFACE WATER DRAINAGE
         DEL_SERVICE_COMPONENT_HD_V HD, -- HIGHWAY DRAINAGE
         DEL_SUPPLY_POINT SP
    WHERE SP.SPID_PK = MPW.SPID_PK(+)
    AND SP.SPID_PK = MNPW.SPID_PK(+)
    AND SP.SPID_PK = AW.SPID_PK(+)
    AND SP.SPID_PK = UW.SPID_PK(+)
    AND SP.SPID_PK = WCA.SPID_PK(+)
    AND SP.SPID_PK = MFS.SPID_PK(+)
    AND SP.SPID_PK = ASW.SPID_PK(+)
    AND SP.SPID_PK = US.SPID_PK(+)
    AND SP.SPID_PK = SCA.SPID_PK(+)
    AND SP.SPID_PK = SW.SPID_PK(+)
    AND SP.SPID_PK = HD.SPID_PK(+)
--    AND EXISTS (SELECT 1 FROM MOUTRAN.MO_SERVICE_COMPONENT WHERE SPID_PK = SP.SPID_PK)
    ORDER BY SP.SPID_PK;

  TYPE tab_service_component IS TABLE OF cur_service_component%ROWTYPE INDEX BY PLS_INTEGER;
  t_service_component  tab_service_component;

BEGIN
   -- initial variables
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_dropped_cb := 0;
   l_no_row_written := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_no_sct_count := 0;
   l_job.IND_STATUS := 'RUN';

   IF USER = 'FINDEL' THEN
      l_filepath := 'FINEXPORT';
   END IF;

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

   COMMIT;

   l_progress := 'processing ';
   l_filename := 'SERVICE_COMPONENT_SEVERN-W_' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || '.dat';

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- start processing all records for range supplied
--  fhandle := UTL_FILE.FOPEN('DELEXPORT', l_filename, 'w');

  OPEN cur_service_component;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_service_component BULK COLLECT INTO t_service_component LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_service_component.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
      l_err.TXT_KEY := t_service_component(i).SPID_PK;
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into DEL_SERVICE_COMPONENT';
          INSERT INTO DEL_SERVICE_COMPONENT VALUES t_service_component(i);
          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
               l_no_row_dropped := l_no_row_dropped + 1;
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
                 CLOSE cur_service_component;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
               END IF;
        END;

        -- keep count of records written
        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
           
           IF t_service_component(i).METEREDPWTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).METEREDNPWTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).AWASSESSEDTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).UWUNMEASUREDTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).WADJCHARGEADJTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).METEREDFSTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).ASASSESSEDTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).USUNMEASUREDTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).SADJCHARGEADJTARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).SRFCWATERTARRIFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
           
           IF t_service_component(i).HWAYDRAINAGETARIFFCODE IS NOT NULL THEN
              l_no_sct_count := l_no_sct_count + 1;
           END IF;
        END IF;
    END LOOP;

-- NOW WRITE THE FILE
   FOR w IN cb_cur
   LOOP
    CASE w.WHOLESALER_ID 
      WHEN 'ANGLIAN-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_ANW_V';
      WHEN 'DWRCYMRU-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_WEL_V';
      WHEN 'SEVERN-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_STW_V';
      WHEN 'SEVERN-A' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_STWA_V';
      WHEN 'SEVERN-B' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_STWB_V';
      WHEN 'THAMES-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_THW_V';
      WHEN 'WESSEX-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_WEW_V';
      WHEN 'YORKSHIRE-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_YOW_V';
      WHEN 'UNITED-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_UUW_V';
      WHEN 'SOUTHSTAFF-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_SSW_V';
    END CASE;
    IF w.RUN_FLAG = 1 THEN
      l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
      P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);
      l_no_row_written := l_no_row_written + l_rows_written; -- add rows written to total
      
      IF w.WHOLESALER_ID NOT LIKE 'SEVERN%' THEN
        l_filename := 'OWC_SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        l_sql := 'SELECT  SC.*
                  FROM DEL_SUPPLY_POINT STW, DEL_SERVICE_COMPONENT_STW_V SC 
                  WHERE STW.OTHERWHOLESALERID = ''' || w.WHOLESALER_ID || ''' 
                  AND STW.SPID_PK = SC.SPID_PK';
        P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);      
      END IF;
    ELSE
      l_sql := 'SELECT COUNT(*) FROM DEL_SERVICE_COMPONENT SC, DEL_SUPPLY_POINT SP WHERE SC.SPID_PK = SP.SPID_PK AND SP.WHOLESALERID = :wholesaler';
      EXECUTE IMMEDIATE l_sql INTO l_count USING w.WHOLESALER_ID;
      l_no_row_dropped_cb := l_no_row_dropped_cb + l_count;
    END IF;
  END LOOP;
  
    IF t_service_component.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;

  CLOSE CUR_SERVICE_COMPONENT;

  -- archive the latest batch
  P_DEL_UTIL_ARCHIVE_TABLE(p_tablename => l_tablename,
                           p_batch_no => no_batch,
                           p_filename => l_filename);

  -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2080, l_no_row_read,    'Distinct Service Components read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2090, l_no_row_dropped, 'Service Components dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2095, l_no_row_dropped_cb, 'Cross Border Supply Points not written to any file');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2100, l_no_row_insert,  'Del Service Components written to ' || l_tablename || ' from extract');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2101, l_no_sct_count, 'MO Service Components flattened on ' || l_tablename || ' from extract');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2110, l_no_row_written, 'Service Components written to file(s) from extract');

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
END P_MOU_DEL_SERVICE_COMPONENT;
/
exit;