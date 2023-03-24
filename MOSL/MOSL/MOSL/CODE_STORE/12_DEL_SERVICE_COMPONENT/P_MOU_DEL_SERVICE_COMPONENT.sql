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
-- Subversion $Revision: 4023 $
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
    AND EXISTS (SELECT 1 FROM MOUTRAN.MO_SERVICE_COMPONENT WHERE SPID_PK = SP.SPID_PK)
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

--           -- now write the record to the ouput file
--           BEGIN
--            l_progress := 'write row export file ';
--
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SPID_PK) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).METEREDPWTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MPWSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MPWSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MPWSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).METEREDPWMAXDAILYDEMAND) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).DAILYRESERVEDCAPACITY) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).METEREDNPWTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MNPWSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MNPWSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MNPWSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).METEREDNPWMAXDAILYDEMAND) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).METEREDNPWDAILYRESVDCAPACITY) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWASSESSEDTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWASSESSEDCHARGEMETERSIZE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWASSESSEDDVOLUMETRICRATE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).AWASSESSEDTARIFBAND) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEACOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEADESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEBCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEBDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPECCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPECDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEDCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEDDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEECOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEEDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEFCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEFDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEGCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEGDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).UWUNMEASUREDTYPEHCOUNT) || '|');
--            UTL_FILE.PUT(FHANDLE,TRIM(t_service_component(i).UWUNMEASUREDTYPEHDESCRIPTION) || '|');
--            UTL_FILE.PUT(FHANDLE,TRIM(t_service_component(i).UWPIPESIZE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).WADJCHARGEADJTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).METEREDFSTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MFSSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MFSSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).MFSSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASASSESSEDTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASASSESSEDCHARGEMETERSIZE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASASSESSEDDVOLUMETRICRATE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).ASASSESSEDTARIFBAND) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEACOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEADESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEBCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEBDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPECCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPECDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEDCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEDDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEECOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEEDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEFCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEFDESCRIPTION) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEGCOUNT) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).USUNMEASUREDTYPEGDESCRIPTION) || '|');
--            UTL_FILE.PUT(FHANDLE,TRIM(T_SERVICE_COMPONENT(I).USUNMEASUREDTYPEHCOUNT) || '|');
--            UTL_FILE.PUT(FHANDLE,TRIM(T_SERVICE_COMPONENT(I).USUNMEASUREDTYPEHDESCRIPTION) || '|');
--            UTL_FILE.PUT(FHANDLE,TRIM(T_SERVICE_COMPONENT(I).USPIPESIZE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SADJCHARGEADJTARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SRFCWATERTARRIFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SRFCWATERAREADRAINED) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SRFCWATERCOMMUNITYCONFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SWSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SWSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).SWSPECIALAGREEMENTREF) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).HWAYDRAINAGETARIFFCODE) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).HWAYSURFACEAREA) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).HWAYCOMMUNITYCONFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).HDSPECIALAGREEMENTFLAG) || '|');
--            UTL_FILE.PUT(fHandle,TRIM(t_service_component(i).HDSPECIALAGREEMENTFACTOR) || '|');
--            UTL_FILE.PUT_LINE(fHandle,TRIM(t_service_component(i).HDSPECIALAGREEMENTREF));
--
--        EXCEPTION
--          WHEN OTHERS THEN
--               l_rec_written := FALSE;
--               l_error_number := SQLCODE;
--               l_error_message := SQLERRM;
--
--               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
--               l_no_row_exp := l_no_row_exp + 1;
--
--               -- if tolearance limit has een exceeded, set error message and exit out
--               IF (   l_no_row_exp > l_job.EXP_TOLERANCE
--                   OR l_no_row_err > l_job.ERR_TOLERANCE
--                   OR l_no_row_war > l_job.WAR_TOLERANCE)
--               THEN
--                 CLOSE cur_service_component;
--                 l_job.IND_STATUS := 'ERR';
--                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
--                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
--                 COMMIT;
--                 return_code := -1;
--                 RETURN;
--               END IF;
--            END;
--
--          IF l_rec_written THEN
--             l_no_row_written := l_no_row_written + 1;
--          END IF;
        END IF;
    END LOOP;

-- NOW WRITE THE FILE
   FOR w IN cb_cur
   LOOP
    CASE w.WHOLESALER_ID 
      WHEN 'ANGLIAN-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_ANW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_ANW_V;
        END IF;
      WHEN 'DWRCYMRU-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_WEL_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_WEL_V;
        END IF;
      WHEN 'SEVERN-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_STW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_STW_V;
        END IF;
      WHEN 'THAMES-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_THW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_THW_V;
        END IF;
      WHEN 'WESSEX-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_WEW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_WEW_V;
        END IF;
      WHEN 'YORKSHIRE-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_YOW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_YOW_V;
        END IF;
      WHEN 'UNITED-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_UUW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_UUW_V;
        END IF;
      WHEN 'SOUTHSTAFF-W' THEN
        l_sql := 'SELECT * FROM DEL_SERVICE_COMPONENT_SSW_V';
        l_filename := 'SERVICE_COMPONENT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        IF w.RUN_FLAG = 0 THEN
          SELECT COUNT(*) INTO l_count FROM DEL_SERVICE_COMPONENT_SSW_V;
        END IF;
    END CASE;
    IF w.RUN_FLAG = 1 THEN
      P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);
      l_no_row_written := l_no_row_written + l_rows_written; -- add rows written to total
    ELSE
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
--  UTL_FILE.FCLOSE(FHANDLE);

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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP37', 2100, l_no_row_insert,  'Service Components written to ' || l_tablename || ' from extract');
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