create or replace
PACKAGE BODY P_MIG_TARIFF AS


PROCEDURE P_MOU_TRAN_TARIFF_RUN  AS
------------------------------------------------------------------------------------
-- PACKAGE SPECIFICATION: Batch Migration
--
-- AUTHOR         : sreedhar p
--
-- FILENAME       : P_MIG_TARIFF.pks
--
-- Subversion $Revision: 4023 $
--
-- CREATED        : 19-Mar-2016
--
-- DESCRIPTION    :
--
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------        --------------------------------------
-- V 0.01      03/03/2016          S.Pallati      Initial Version
-- V 0.02      19/03/2016          S.Pallati      implemented comments by Mike M on 18-mar-2016.
--                                                created a procedure which will  p_start_batch and fn_updatebatch
--                                                and deleted p_tariff_run independent proc.
--                                                We need to execute the proc P_MOU_TRAN_TARIFF_RUN to run tariff migration
-- V 0.03      28/04/2016          S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                                for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
-- V 0.05      09/05/2016          K.Burton       Correction to P_MOU_TRAN_TARIFF_MPW to swap BLOCK and BAND_CHARGE table data
--                                                - BAND_CHARGE is now also called STANDBY_BLOCK since this matches the MOSL spec naming
--                                                convention
-- V 0.06      12/05/2016          K.Burton       Added update statement to replace all £ symbols in tariff names with GBP
----------------------------------------------------------------------------------------

l_error_number                  VARCHAR2(255);
l_error_message                 VARCHAR2(512);
l_key                           VARCHAR2(200);
return_code                     NUMBER;
l_batch_no                      NUMBER;
l_NO_INSTANCE                   NUMBER;
l_IND_STATUS                    VARCHAR2(3);
/*-------------------------------------------------------------------------------------------------------------
| Main processing.
|-------------------------------------------------------------------------------------------------------------*/
BEGIN
  --P_MIG_BATCH.P_STARTBATCH;
  return_code   := 0;
  l_NO_INSTANCE :=0;

  SELECT NVL(MAX(no_batch),0) INTO l_batch_no FROM MIG_BATCHSTATUS;

  l_batch_no := l_batch_no+1;

  SELECT err_tolerance INTO g_err_tol FROM MIG_JOBREF WHERE NO_JOB=17;

  P_MIG_TARIFF.P_MOU_TRAN_TARIFF_AS ( l_batch_no,10,return_code );

  IF (return_code =0) THEN
    p_mig_tariff.p_mou_tran_tariff_aw ( l_batch_no,11, return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = l_batch_no;
  END IF;

  IF (return_code =0) THEN
    p_mig_tariff.p_mou_tran_tariff_mpw ( l_batch_no, 12, return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = l_batch_no;
  END IF;

  IF (return_code =0) THEN
    p_mig_tariff.p_mou_tran_tariff_ms ( l_batch_no,13, return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = l_batch_no;
  END IF;

  IF (return_code =0) THEN
    p_mig_tariff.p_mou_tran_tariff_sw ( l_batch_no,14, return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = l_batch_no;
  END IF;

-- TEMP REMOVE TE TARIFFs
--  IF (return_code =0) THEN
--    p_mig_tariff.p_mou_tran_tariff_te ( l_batch_no,15, return_code );
--    --P_MIG_BATCH.FN_UPDATEBATCH;
--    UPDATE MIG_BATCHSTATUS
--    SET BATCH_STATUS = 'END',
--      ts_update      = CURRENT_TIMESTAMP
--    WHERE NO_BATCH   = l_batch_no;
--  END IF;

  IF (RETURN_CODE =0) THEN
    p_mig_tariff.p_mou_tran_tariff_us ( l_batch_no, 16,return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = l_batch_no;
  END IF;

  IF (return_code =0) THEN
    p_mig_tariff.p_mou_tran_tariff_uw ( l_batch_no, 17, return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = l_batch_no;
  END IF;

  IF (return_code =0) THEN
    p_mig_tariff.p_mou_tran_tariff_hd ( l_batch_no, 18, return_code );
    --P_MIG_BATCH.FN_UPDATEBATCH;
    UPDATE MIG_BATCHSTATUS
    SET BATCH_STATUS = 'END',
      ts_update      = CURRENT_TIMESTAMP
    WHERE NO_BATCH   = L_BATCH_NO;
  END IF;

  -- DB patch 15
  DELETE FROM MO_TARIFF_STANDING_DATA;

  INSERT
  INTO MO_TARIFF_STANDING_DATA
    (
      WHOLESALER,
      DEFAULTRETURNTOSEWER,
      VACANCYCHARGINGMETHODWATER,
      VACANCYCHARGINGMETHODSEWERAGE,
      TEMPDISCONCHARGINGMETHODWAT,
      TEMPDISCONCHARGINGMETHODSEW
    )
    VALUES
    (
      'STW',
      100,
      'VWA',
      'VSA',
      'TWA',
      'TSA'
    );

  DELETE FROM LU_TARIFF;

  INSERT
  INTO LU_TARIFF
    (
      TARIFFCODE_PK,
      TARIFFEFFECTIVEFROMDATE,
      TARIFF_EFFECTIVE_TO_DATE,
      TARIFFNAME,
      TARIFFSTATUS,
      TARIFFLEGACYEFFECTIVEFROMDATE,
      TARIFFAUTHCODE,
      SERVICECOMPONENTTYPE
    )
  SELECT TARIFFCODE_PK,
    TARIFFEFFECTIVEFROMDATE,
    NULL,
    TARIFFNAME,
    TARIFFSTATUS ,
    NULL,
    TARIFFAUTHCODE ,
    SERVICECOMPONENTTYPE
  FROM MO_TARIFF ;
  
  -- remove and £ symbols from tariff names as these cause MOSL display problems
  -- replace with GBP - V 0.06 
  UPDATE MO_TARIFF
  SET TARIFFNAME = REPLACE(TARIFFNAME,'£','GBP')
  WHERE TARIFFNAME LIKE '%£%';

EXCEPTION
WHEN OTHERS THEN
  l_IND_STATUS := 'ERR';
  P_MIG_BATCH.FN_ERRORLOG(l_batch_no, l_NO_INSTANCE, 'E', 'TARIFF RUN FAILED', 0, 'TARIFF RUN FAILED');
  P_MIG_BATCH.FN_UPDATEJOB(l_batch_no, l_NO_INSTANCE, l_IND_STATUS);
END P_MOU_TRAN_TARIFF_RUN;


PROCEDURE P_MOU_TRAN_TARIFF_AS(
    no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
    NO_JOB      IN MIG_JOBREF.NO_JOB%TYPE,
    return_code IN OUT NUMBER ) AS
  ----------------------------------------------------------------------------------------
  -- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for service component Assessed sewerage AS
  --
  -- AUTHOR         : Sreedhar Pallati
  --
  -- FILENAME       :
  --
  -- CREATED        : 25/02/2016
  --
  -- DESCRIPTION    : Procedure to create the Tariff MO Extract
  -- NOTES  :
  --
  ---------------------------- Modification History --------------------------------------
  --
  -- Version     Date                Author         Description
  -- ---------   ---------------     -------             ----------------------------------
  -- V 0.01      25/02/2016          S.Pallati        Initial Draft
  -- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
  -- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables
  --                                               for tariff code and tariff names
  -- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures
  -----------------------------------------------------------------------------------------
  l_tariff_ver NUMBER;
  l_serv_comp  VARCHAR2(5);
  xmlClob_line CLOB;
  xmlClob_full_file CLOB;
  xmlFile UTL_FILE.FILE_TYPE;
  x XMLType;
  l_err_rows               NUMBER := 0;
  l_tariff_code            VARCHAR2(100);
  l_tariff_count           NUMBER                :=0;
  l_tariff_dropped         NUMBER                :=0;
  l_db_count_before_insert NUMBER                :=0;
  l_db_count_after_insert  NUMBER                :=0;
  l_db_rows_inserted       NUMBER                :=0;
  c_module_name            CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_AS';
  c_company_cd             CONSTANT VARCHAR2(4)  := 'STW1';
  l_error_number           VARCHAR2(255);
  l_error_message          VARCHAR2(512);
  l_progress               VARCHAR2(100);
  l_job MIG_JOBSTATUS%ROWTYPE;
  l_err MIG_ERRORLOG%ROWTYPE;
  l_no_row_read MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written BOOLEAN;
BEGIN
  l_progress        := 'Start';
  l_err.TXT_DATA    := c_module_name;
  l_err.TXT_KEY     := 0;
  l_job.NO_INSTANCE := 0;
  l_no_row_read     := 0;
  l_no_row_insert   := 0;
  l_no_row_dropped  := 0;
  l_no_row_war      := 0;
  l_no_row_err      := 0;
  l_no_row_exp      := 0;
  l_job.IND_STATUS  := 'RUN';

  -- get job no and start job
  P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name, l_job.NO_INSTANCE, l_job.ERR_TOLERANCE, l_job.EXP_TOLERANCE, l_job.WAR_TOLERANCE, l_job.NO_COMMIT, l_job.NO_STREAM, l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX, l_job.IND_STATUS);
  L_PROGRESS         := 'processing AS xml file';
  
  IF l_job.IND_STATUS = 'ERR' THEN
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    return_code := -1;
    RETURN;
  END IF;

  SELECT COUNT(TARIFFCODE_PK)
  INTO l_db_count_before_insert
  FROM MO_TARIFF;

  l_serv_comp := 'AS';

  -- xml file reading and storing the data into xmlClob_full_file
  xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_AS_XML.xml', 'R');
  LOOP
    BEGIN
      UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
      xmlClob_full_file := xmlClob_full_file||xmlClob_line;
    EXCEPTION
    WHEN No_Data_Found THEN
      EXIT;
    END;
  END LOOP;

  UTL_FILE.FCLOSE(xmlFile);

  x     := XMLType.createXML(xmlClob_full_file);

  FOR r IN
  (SELECT ExtractValue(Value(p),'/Row/TariffCode/text()')                          AS TariffCode ,
    ExtractValue(Value(p),'/Row/TariffName/text()')                                AS TariffName ,
    ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()')                   AS TARIFFEFFECTIVEFROMDATE ,
    ExtractValue(Value(p),'/Row/TariffStatus/text()')                              AS TariffStatus ,
    ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()')             AS TARIFFLEGACYEFFECTIVEFROMDATE ,
    ExtractValue(Value(p),'/Row/ServiceComponent/text()')                          AS ServiceComponent ,
    ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()')                   AS Tariffauthorisationcode ,
    ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()')                 AS VACANCYCHARGIGMETHODWATER ,
    ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()')             AS VACANCYCHARGINGMETHODSEWERAGE ,
    ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()')               AS TEMPDISCONCHARGINGMETHODWAT ,
    ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()')               AS TEMPDISCONCHARGINGMETHODSEW ,
    ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()')    AS Default_Return_to_Sewer ,
    ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()')                 AS tar_ver_tariffcode ,
    ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') AS TARIFFVEREFFECTIVEFROMDATE ,
    ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()')               AS tarver_TariffStatus ,
    ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_AS/TariffCode/text()')              AS MO_TARIFF_TYPE_AS_tariff_code ,
    ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_AS/ASFixedCharge/text()')           AS MO_TARIFF_TYPE_ASFixedCharge ,
    ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_AS/ASVolumetricCharge/text()')      AS MO_TARIFF_TYPE_ASVolumetricCha ,
    ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()')                       AS MO_METER_TariffCode ,
    ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()')                   AS MO_METER_LowerMeterSize ,
    ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()')                   AS MO_METER_UpperMeterSize ,
    ExtractValue(Value(p),'/Row/MO_METER/Charge/text()')                           AS MO_METER_Charge ,
    ExtractValue(Value(p),'/Row/MO_BAND/TariffCode/text()')                        AS MO_BAND_TariffCode ,
    ExtractValue(Value(p),'/Row/MO_BAND/Band/text()')                              AS MO_BAND_Band ,
    ExtractValue(Value(p),'/Row/MO_BAND/Charge/text()')                            AS MO_BAND_Charge
  FROM TABLE(XMLSequence(Extract(x,'/TARIFF_AS/Row'))) p
  )
  LOOP
    l_progress    := 'processing AS xml parse';
    l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;
    BEGIN
      IF r.TariffCode  IS NOT NULL THEN
        l_progress     := 'processing MO_TARIFF';
        l_tariff_count := l_tariff_count+1;

        INSERT
        INTO MO_TARIFF
          (
            TARIFFCODE_PK,
            TARIFFEFFECTIVEFROMDATE,
            TARIFFSTATUS,
            TARIFFLEGACYEFFECTIVEFROMDATE,
            APPLICABLESERVICECOMPONENT,
            TARIFFAUTHCODE,
            --VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
            --VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
            --TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
            --TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
            SERVICECOMPONENTTYPE,
            TARIFFNAME
          )
          VALUES
          (
            trim(r.TariffCode),
            to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
            upper(r.TariffStatus), -- issue 28 fix
            to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
            l_serv_comp,
            r.Tariffauthorisationcode,
            --r.VACANCYCHARGIGMETHODWATER,
            --r.VACANCYCHARGINGMETHODSEWERAGE,
            --r.TEMPDISCONCHARGINGMETHODWAT,
            --r.TEMPDISCONCHARGINGMETHODSEW ,
            L_SERV_COMP,
            trim(r.TariffName)
          );
      END IF;

      IF r.tar_ver_tariffcode IS NOT NULL THEN
        l_progress            := 'processing MO_TARIFF_VERSION';

        SELECT NVL(MAX(TARIFFVERSION),0)
        INTO l_tariff_ver
        FROM MO_TARIFF_VERSION
        WHERE tariffcode_pk=r.tar_ver_tariffcode;

        INSERT
        INTO MO_TARIFF_VERSION
          (
            TARIFF_VERSION_PK,
            TARIFFCODE_PK,
            TARIFFVERSION,
            TARIFFVEREFFECTIVEFROMDATE,
            TARIFFSTATUS,
            APPLICABLESERVICECOMPONENT,
            --DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
            TARIFFCOMPONENTTYPE,
            SECTION154PAYMENTVALUE,
            STATE
            /* DB patch 15 fix*/
          )
          VALUES
          (
            TARIFF_VERSION_PK_SEQ.NEXTVAL,                     --TARIFF_VERSION_PK
            trim(r.tar_ver_tariffcode),                        --TARIFFCODE_PK
            l_tariff_ver+1,                                    --TARIFFVERSION
            to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
            upper(r.tarver_TariffStatus),                      --TARIFFSTATUS-- issue 28 fix
            L_SERV_COMP,                                       --APPLICABLESERVICECOMPONENT
            --nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
            l_serv_comp,--TARIFFCOMPONENTTYPE
            NULL,
            g_state
          );--SECTION154PAYMENTVALUE););
      END IF;
      --SPSP tariff type pk - use actual seq
      IF r.MO_TARIFF_TYPE_AS_tariff_code IS NOT NULL THEN
        l_progress                       := 'processing MO_TARIFF_TYPE_AS';

        INSERT
        INTO MO_TARIFF_TYPE_AS
          (
            TARIFF_TYPE_PK,
            TARIFF_VERSION_PK,
            ASFIXEDCHARGE,
            ASVOLMETCHARGE,
            ASEFFECTIVEDATE
          )
          VALUES
          (
            AS_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
            TARIFF_VERSION_PK_SEQ.currval,
            r.MO_TARIFF_TYPE_ASFixedCharge,
            r.MO_TARIFF_TYPE_ASVolumetricCha,
            NULL
          );
      END IF;

      IF r.MO_BAND_TariffCode IS NOT NULL THEN
        l_progress            := 'processing MO_AS_BAND_CHARGE';
        INSERT
        INTO MO_AS_BAND_CHARGE
          (
            TARIFF_BAND_CHARGE_PK,
            TARIFF_TYPE_PK,
            BAND,
            CHARGE
          )
          VALUES
          (
            AS_TARIFF_BAND_CHARGE_PK_SEQ.nextval,
            AS_TARIFF_TYPE_PK_SEQ.currval,
            r.MO_BAND_Band,
            r.MO_BAND_Charge
          );
      END IF;

      IF r.MO_METER_TariffCode IS NOT NULL THEN
        l_progress             := 'processing MO_AS_METER_ASMFC';
        INSERT
        INTO MO_AS_METER_ASMFC
          (
            TARIFF_ASMFC_PK,
            TARIFF_TYPE_PK,
            LOWERMETERSIZE,
            UPPERMETERSIZE,
            CHARGE
          )
          VALUES
          (
            AS_TARIFF_ASMFC_PK_SEQ.nextval,
            AS_TARIFF_TYPE_PK_SEQ.currval,
            r.MO_METER_LowerMeterSize,
            r.MO_METER_UpperMeterSize,
            r.MO_METER_Charge
          );
      END IF;
    EXCEPTION
    WHEN OTHERS THEN
      IF r.TariffCode    IS NOT NULL THEN
        l_tariff_dropped := l_tariff_dropped + 1;
        l_err.TXT_KEY    := r.TariffCode;
        g_err_rows       := g_err_rows+1;
      END IF;
      l_error_number  := SQLCODE;
      l_error_message := sqlerrm;

      IF(l_no_row_exp > l_job.exp_tolerance OR g_err_rows >= g_err_tol OR l_no_row_war > l_job.war_tolerance) THEN
        l_job.IND_STATUS := 'ERR';
        P_MIG_BATCH.FN_ERRORLOG( no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded', l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
        P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2395, l_tariff_count, 'Distinct AS tariffs read during Transform');
        P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2405, l_tariff_dropped, 'Distinct AS tariffs dropped during Transform');
        P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2415, l_db_rows_inserted, 'Distinct AS tariffs written to MO_TARIFFs during Transform');
        COMMIT;
        return_code := -1;
        RETURN;
      ELSE
        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', SUBSTR(l_error_message,1,100), l_err.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || l_progress,1,100));
      END IF;
    END;
  END LOOP;
  
  l_progress := 'Writing Counts';
  SELECT COUNT(TARIFFCODE_PK) INTO l_db_count_after_insert FROM MO_TARIFF;
  
  l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;

  --l_tariff_dropped :=   l_tariff_count - l_db_rows_inserted;
  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2395, l_tariff_count, 'Distinct AS tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2405, l_tariff_dropped, 'Distinct AS tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2415, l_db_rows_inserted, 'Distinct AS tariffs written to MO_TARIFFs during Transform');

  IF l_tariff_count  <> l_db_rows_inserted+l_tariff_dropped THEN
    l_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match', l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    COMMIT;
    return_code := -1;
  ELSE
    l_job.IND_STATUS      := 'END';
    IF (l_db_rows_inserted =0) THEN
      l_job.IND_STATUS    := 'ERR';
    END IF;
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;
  COMMIT;
EXCEPTION
WHEN OTHERS THEN
  l_error_number  := SQLCODE;
  l_error_message := SQLERRM;
  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', SUBSTR(l_error_message,1,100), l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
  l_job.IND_STATUS := 'ERR';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  return_code := -1;
  COMMIT;
END P_MOU_TRAN_TARIFF_AS;

  procedure P_MOU_TRAN_TARIFF_AW   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for assessed water AW
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
-----------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_AW';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing AW xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

 select count(TARIFFCODE_PK) into l_db_count_before_insert from MO_TARIFF;
l_serv_comp := 'AW';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_AW_XML.xml', 'R');
LOOP
BEGIN
UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
xmlClob_full_file := xmlClob_full_file||xmlClob_line;
EXCEPTION WHEN No_Data_Found THEN EXIT; END;
END LOOP;

UTL_FILE.FCLOSE(xmlFile);

x := XMLType.createXML(xmlClob_full_file);


  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus



          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_AW/TariffCode/text()') as MO_TARIFF_TYPE_AW_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_AW/AWFixedCharge/text()') as MO_TARIFF_TYPE_AW_AWFixCharge
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_AW/AWVolumetricCharge/text()') as MO_TARIFF_TYPE_AW_AWVolCharge


          ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as MO_METER_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_METER_LowerMeterSize
            ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_METER_UpperMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_METER_Charge


          ,ExtractValue(Value(p),'/Row/MO_BAND/TariffCode/text()') as MO_BAND_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_BAND/Band/text()') as MO_BAND_Band
            ,ExtractValue(Value(p),'/Row/MO_BAND/Charge/text()') as MO_BAND_Charge

    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_AW/Row'))) p
    ) LOOP
l_progress := 'processing AW xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN
if r.TariffCode is not null then
l_progress := 'processing MO_TARIFF';
l_tariff_count := l_tariff_count+1;
insert into MO_TARIFF (TARIFFCODE_PK,
TARIFFEFFECTIVEFROMDATE,
TARIFFSTATUS,
TARIFFLEGACYEFFECTIVEFROMDATE,
APPLICABLESERVICECOMPONENT,
TARIFFAUTHCODE,
--VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
--VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
SERVICECOMPONENTTYPE,
TARIFFNAME)
VALUES
(trim(r.TariffCode),
to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
upper(r.TariffStatus), -- issue 28 fix
to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
l_serv_comp,
r.Tariffauthorisationcode,
--r.VACANCYCHARGIGMETHODWATER,
--r.VACANCYCHARGINGMETHODSEWERAGE,
--r.TEMPDISCONCHARGINGMETHODWAT,
--r.TEMPDISCONCHARGINGMETHODSEW ,
L_SERV_COMP,
trim(r.TariffName));
 end if;


    if r.tar_ver_tariffcode is not null then
    l_progress := 'processing MO_TARIFF_VERSION';
select nvl(max(TARIFFVERSION),0) into l_tariff_ver from MO_TARIFF_VERSION where tariffcode_pk=r.tar_ver_tariffcode;

insert into MO_TARIFF_VERSION  (
TARIFF_VERSION_PK,
TARIFFCODE_PK,
TARIFFVERSION,
TARIFFVEREFFECTIVEFROMDATE,
TARIFFSTATUS,
APPLICABLESERVICECOMPONENT,
--DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
TARIFFCOMPONENTTYPE,
SECTION154PAYMENTVALUE,
STATE/* DB patch 15 fix*/)
values
(TARIFF_VERSION_PK_SEQ.NEXTVAL,--TARIFF_VERSION_PK
trim(r.tar_ver_tariffcode),--TARIFFCODE_PK
l_tariff_ver+1,--TARIFFVERSION
to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
upper(r.tarver_TariffStatus),--TARIFFSTATUS-- issue 28 fix
L_SERV_COMP,--APPLICABLESERVICECOMPONENT
--nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
l_serv_comp,--TARIFFCOMPONENTTYPE
NULL,
g_state);--SECTION154PAYMENTVALUE););

end if;


--SPSP tariff type pk - use actual seq
   if r.MO_TARIFF_TYPE_AW_TariffCode is not null then

l_progress := 'processing MO_TARIFF_TYPE_AW';
insert into MO_TARIFF_TYPE_AW (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
AWFIXEDCHARGE,
AWVOLUMETRICCHARGE
)
values
(
AW_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
TARIFF_VERSION_PK_SEQ.currval,
r.MO_TARIFF_TYPE_AW_AWFixCharge,
r.MO_TARIFF_TYPE_AW_AWVolCharge

);
end if;


 if r.MO_METER_TariffCode is not null then

l_progress := 'processing MO_AW_METER_AWMFC';
insert into MO_AW_METER_AWMFC (
TARIFF_AWMFC_PK,
TARIFF_TYPE_PK,
LOWERMETERSIZE,
UPPERMETERSIZE,
CHARGE
)
values
(
AW_TARIFF_AWMFC_PK_SEQ.nextval,
AW_TARIFF_TYPE_PK_SEQ.currval,
r.MO_METER_LowerMeterSize,
r.MO_METER_UpperMeterSize,
r.MO_METER_Charge
);
end if;



 if r.MO_BAND_TariffCode is not null then
 l_progress := 'processing MO_AW_BAND_CHARGE';
insert into MO_AW_BAND_CHARGE (
TARIFF_BAND_CHARGE_PK,
TARIFF_TYPE_PK,
BAND,
CHARGE
)
values
(
AW_TARIFF_BAND_CHARGE_PK_SEQ.nextval,
AW_TARIFF_TYPE_PK_SEQ.currval,
r.MO_BAND_Band,
r.MO_BAND_Charge
);
end if;
   EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
            -- g_err_rows := g_err_rows+1;
             l_error_number := sqlcode;
             l_error_message := SQLERRM;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2393, l_tariff_count,    'Distinct AW tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2403, l_tariff_dropped,    'Distinct AW tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2413, l_db_rows_inserted,  'Distinct AW tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;


l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2393, l_tariff_count,    'Distinct AW tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2403, l_tariff_dropped,    'Distinct AW tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2413, l_db_rows_inserted,  'Distinct AW tariffs written to MO_TARIFFs during Transform');

IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

Commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;

  END P_MOU_TRAN_TARIFF_AW;


PROCEDURE P_MOU_TRAN_TARIFF_MPW(
    no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
    NO_JOB      IN MIG_JOBREF.NO_JOB%TYPE,
    return_code IN OUT NUMBER ) AS
  ----------------------------------------------------------------------------------------
  -- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for MPW
  --
  -- AUTHOR         : Sreedhar Pallati
  --
  -- FILENAME       :
  --
  -- CREATED        : 25/02/2016
  --
  -- DESCRIPTION    : Procedure to create the Tariff MO Extract
  -- NOTES  :
  --
  ---------------------------- Modification History --------------------------------------
  --
  -- Version     Date                Author         Description
  -- ---------   ---------------     -------        ----------------------------------
  -- V 0.01      25/02/2016          S.Pallati       Initial Draft
  -- V 0.02      11/03/2016          S.Pallati       Issue 28 fixed -- upper case conversion of tariff status
  -- V 0.03      28-April-2016      S Pallati        Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables
  --                                                 for tariff code and tariff names
  -- v 0.04      29/04/2016          L. Smith        Control Point CP44 mapped to new measures
  -- V 0.05      09/05/2016          K.Burton        Correction to P_MOU_TRAN_TARIFF_MPW to swap BLOCK and BAND_CHARGE table data
  --                                                 - BAND_CHARGE is now also called STANDBY_BLOCK since this matches the MOSL spec naming
  --                                                 convention
  -----------------------------------------------------------------------------------------
  l_tariff_ver NUMBER;
  l_serv_comp  VARCHAR2(5);
  xmlClob_line CLOB;
  xmlClob_full_file CLOB;
  xmlFile UTL_FILE.FILE_TYPE;
  x XMLType;
  l_err_rows               NUMBER := 0;
  l_tariff_code            VARCHAR2(100);
  l_tariff_count           NUMBER                :=0;
  l_tariff_dropped         NUMBER                :=0;
  l_db_count_before_insert NUMBER                :=0;
  l_db_count_after_insert  NUMBER                :=0;
  l_db_rows_inserted       NUMBER                :=0;
  c_module_name            CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_MPW';
  c_company_cd             CONSTANT VARCHAR2(4)  := 'STW1';
  l_error_number           VARCHAR2(255);
  l_error_message          VARCHAR2(512);
  l_progress               VARCHAR2(100);
  l_job MIG_JOBSTATUS%ROWTYPE;
  l_err MIG_ERRORLOG%ROWTYPE;
  l_no_row_read MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written BOOLEAN;
BEGIN
  l_progress        := 'Start';
  l_err.TXT_DATA    := c_module_name;
  l_err.TXT_KEY     := 0;
  l_job.NO_INSTANCE := 0;
  l_no_row_read     := 0;
  l_no_row_insert   := 0;
  l_no_row_dropped  := 0;
  l_no_row_war      := 0;
  l_no_row_err      := 0;
  l_no_row_exp      := 0;
  l_job.IND_STATUS  := 'RUN';
  
  -- get job no and start job
  P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name, l_job.NO_INSTANCE, l_job.ERR_TOLERANCE, l_job.EXP_TOLERANCE, l_job.WAR_TOLERANCE, l_job.NO_COMMIT, l_job.NO_STREAM, l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX, l_job.IND_STATUS);
  
  l_progress         := 'processing MPW xml file';
  IF l_job.IND_STATUS = 'ERR' THEN
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    return_code := -1;
    RETURN;
  END IF;

  SELECT COUNT(TARIFFCODE_PK) INTO L_DB_COUNT_BEFORE_INSERT FROM MO_TARIFF;
  l_serv_comp := 'MPW';

  -- xml file reading and storing the data into xmlClob_full_file
  xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_MPW_XML.xml', 'R');
  LOOP
    BEGIN
      UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
      xmlClob_full_file := xmlClob_full_file||xmlClob_line;
    EXCEPTION
    WHEN No_Data_Found THEN
      EXIT;
    END;
  END LOOP;
  UTL_FILE.FCLOSE(xmlFile);

  x := XMLType.createXML(xmlClob_full_file);
  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus

          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MPW/TariffCode/text()') as MO_TARIFF_TYPE_MPW_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MPW/UWFIXEDCHARGE/text()') as MPWSUPPLYPOINTFIXEDCHARGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MPW/UWRVPOUNDAGE/text()') as MPWPREMIUMTOLFACTOR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MPW/UWRVTHRESHOLD/text()') as MPWDAILYSTANDBYUSAGEVOLCHARGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MPW/UWRVMAXIMUMCHARGE/text()') as MPWDAILYPREMIUMUSAGEVOLCHARGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MPW/UWRVMinimumCharge/text()') as MPWMAXIMUMDEMANDTARIFF

--          ,ExtractValue(Value(p),'/Row/MO_BLOCK/TariffCode/text()') as MO_BLOCK_TariffCode -- V 0.05
--          ,ExtractValue(Value(p),'/Row/MO_BLOCK/RESERVATIONVOLUME/text()') as MO_BLOCK_RESERVATIONVOLUME -- V 0.05
--          ,ExtractValue(Value(p),'/Row/MO_BLOCK/Price/text()') as MO_BLOCK_Price -- V 0.05

          ,ExtractValue(Value(p),'/Row/MO_STANDBY_BLOCK/TariffCode/text()') as MO_STANDBY_BLOCK_TariffCode -- V 0.05
          ,ExtractValue(Value(p),'/Row/MO_STANDBY_BLOCK/RESERVATIONVOLUME/text()') as MO_STANDBY_BLOCK_RESVOL -- V 0.05
          ,ExtractValue(Value(p),'/Row/MO_STANDBY_BLOCK/Price/text()') as MO_STANDBY_BLOCK_Price -- V 0.05

          ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as MO_METER_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_METER_LowerMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_METER_UpperMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_METER_Charge

--           ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/TariffCode/text()') as MO_BAND_CHARGE_TariffCode -- V 0.05
--          ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/UpperAnnualVol/text()') as MO_BAND_CHARGE_UAV -- V 0.05
--          ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Charge/text()') as MO_BAND_CHARGE_Charge -- V 0.05

           ,ExtractValue(Value(p),'/Row/MO_BLOCK/TariffCode/text()') as MO_BLOCK_TariffCode -- V 0.05
          ,ExtractValue(Value(p),'/Row/MO_BLOCK/UpperAnnualVol/text()') as MO_BLOCK_UAV -- V 0.05
          ,ExtractValue(Value(p),'/Row/MO_BLOCK/Charge/text()') as MO_BLOCK_Charge -- V 0.05
    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_MPW/Row'))) p
    ) LOOP

IF r.TariffStatus = 'Inactive' THEN
  r.TariffStatus := 'Active';
END IF;
l_progress    := 'processing TE xml parse';
l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN
  
  IF r.TariffCode  IS NOT NULL THEN
    l_progress     := 'processing MO_TARIFF';
    l_tariff_count := l_tariff_count+1;
    INSERT
    INTO MO_TARIFF
      (
        TARIFFCODE_PK,
        TARIFFEFFECTIVEFROMDATE,
        TARIFFSTATUS,
        TARIFFLEGACYEFFECTIVEFROMDATE,
        APPLICABLESERVICECOMPONENT,
        TARIFFAUTHCODE,
        --VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
        --VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
        --TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
        --TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
        SERVICECOMPONENTTYPE,
        TARIFFNAME
      )
      VALUES
      (
        trim(r.TariffCode),
        to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
        upper(r.TariffStatus), -- issue 28 fix
        to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
        l_serv_comp,
        r.Tariffauthorisationcode,
        --r.VACANCYCHARGIGMETHODWATER,
        --r.VACANCYCHARGINGMETHODSEWERAGE,
        --r.TEMPDISCONCHARGINGMETHODWAT,
        --r.TEMPDISCONCHARGINGMETHODSEW ,
        L_SERV_COMP,
        trim(r.TariffName)
      );
  END IF;


  IF r.tar_ver_tariffcode IS NOT NULL THEN
    l_progress            := 'processing MO_TARIFF_VERSION';
    SELECT NVL(MAX(TARIFFVERSION),0)
    INTO l_tariff_ver
    FROM MO_TARIFF_VERSION
    WHERE tariffcode_pk=r.tar_ver_tariffcode;
  
    INSERT
    INTO MO_TARIFF_VERSION
      (
        TARIFF_VERSION_PK,
        TARIFFCODE_PK,
        TARIFFVERSION,
        TARIFFVEREFFECTIVEFROMDATE,
        TARIFFSTATUS,
        APPLICABLESERVICECOMPONENT,
        --DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
        TARIFFCOMPONENTTYPE,
        SECTION154PAYMENTVALUE,
        STATE
        /* DB patch 15 fix*/
      )
      VALUES
      (
        TARIFF_VERSION_PK_SEQ.NEXTVAL,                     --TARIFF_VERSION_PK
        trim(r.tar_ver_tariffcode),                        --TARIFFCODE_PK
        l_tariff_ver+1,                                    --TARIFFVERSION
        to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
        upper(r.tarver_TariffStatus),                      --TARIFFSTATUS-- issue 28 fix
        L_SERV_COMP,                                       --APPLICABLESERVICECOMPONENT
        --nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
        l_serv_comp,--TARIFFCOMPONENTTYPE
        NULL,
        g_state
      );--SECTION154PAYMENTVALUE););
  END IF; 
  
  IF r.MO_TARIFF_TYPE_MPW_TariffCode IS NOT NULL THEN
    l_progress                       := 'processing MO_TARIFF_TYPE_MPW';
    INSERT
    INTO MO_TARIFF_TYPE_MPW
      (
        TARIFF_TYPE_PK,
        TARIFF_VERSION_PK,
        MPWSUPPLYPOINTFIXEDCHARGES,
        MPWPREMIUMTOLFACTOR,
        MPWDAILYSTANDBYUSAGEVOLCHARGE,
        MPWDAILYPREMIUMUSAGEVOLCHARGE,
        MPWMAXIMUMDEMANDTARIFF
      )
      VALUES
      (
        MPW_TARIFF_TYPE_PK_SEQ.nextval,
        TARIFF_VERSION_PK_SEQ.currval,
        r.MPWSUPPLYPOINTFIXEDCHARGE,
        r.MPWPREMIUMTOLFACTOR,
        r.MPWDAILYSTANDBYUSAGEVOLCHARGE,
        r.MPWDAILYPREMIUMUSAGEVOLCHARGE,
        r.MPWMAXIMUMDEMANDTARIFF
      );
  END IF;

  IF r.MO_BLOCK_TariffCode IS NOT NULL THEN
    l_progress             := 'processing MO_MPW_BLOCK_MWBT';
    INSERT
    INTO MO_MPW_BLOCK_MWBT
      (
        TARIFF_MWBT_PK,
        TARIFF_TYPE_PK,
        UPPERANNUALVOL,
        CHARGE
      )
      VALUES
      (
        MPW_TARIFF_MWBT_PK_SEQ.nextval,
        MPW_TARIFF_TYPE_PK_SEQ.CURRVAL,
--        r.MO_BLOCK_RESERVATIONVOLUME,  -- V 0.05
--        r.MO_BLOCK_Price  -- V 0.05
        r.MO_BLOCK_UAV, -- V 0.05
        R.MO_BLOCK_Charge -- V 0.05
      );
  END IF;

  IF r.MO_METER_TariffCode IS NOT NULL THEN
    l_progress             := 'processing MO_MPW_METER_MWMFC';
    INSERT
    INTO MO_MPW_METER_MWMFC
      (
        TARIFF_MWMFC_PK,
        TARIFF_TYPE_PK,
        LOWERMETERSIZE,
        UPPERMETERSIZE,
        CHARGE
      )
      VALUES
      (
        MPW_TARIFF_MWMFC_PK_SEQ.nextval,
        MPW_TARIFF_TYPE_PK_SEQ.currval,
        r.MO_METER_LowerMeterSize,
        r.MO_METER_UpperMeterSize,
        r.MO_METER_Charge
      );
  END IF;
 
  IF r.MO_STANDBY_BLOCK_TariffCode IS NOT NULL THEN -- V 0.05
    l_progress                   := 'processing MO_MPW_STANDBY_MWCAPCHG';
    INSERT
    INTO MO_MPW_STANDBY_MWCAPCHG
      (
        TARIFF_MWCAPCHG_PK,
        TARIFF_TYPE_PK,
        RESERVATIONVOLUME,
        CHARGE
      )
      VALUES
      (
        MPW_TARIFF_MWCAPCHG_PK_SEQ.nextval,
        MPW_TARIFF_TYPE_PK_SEQ.currval,
        r.MO_STANDBY_BLOCK_RESVOL, -- V 0.05
        r.MO_STANDBY_BLOCK_PRICE -- V 0.05
--        r.MO_BAND_CHARGE_UAV, -- V 0.05
--        r.MO_BAND_CHARGE_Charge -- V 0.05
      );
  END IF;

   EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
            -- g_err_rows := g_err_rows+1;
             l_error_number := SQLCODE;
             l_error_message := sqlerrm;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2397, l_tariff_count,    'Distinct MPW tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2407, l_tariff_dropped,    'Distinct MPW tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2417, l_db_rows_inserted,  'Distinct MPW tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;

  l_progress := 'Writing Counts';
  SELECT COUNT(TARIFFCODE_PK) INTO l_db_count_after_insert FROM MO_TARIFF;
  l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;

  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2397, l_tariff_count,    'Distinct MPW tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2407, l_tariff_dropped,    'Distinct MPW tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2417, l_db_rows_inserted,  'Distinct MPW tariffs written to MO_TARIFFs during Transform');

   IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
       l_job.IND_STATUS := 'ERR';
       P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
       P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
       COMMIT;
       return_code := -1;
    ELSE
       l_job.IND_STATUS := 'END';
        IF (l_db_rows_inserted =0) THEN
       l_job.IND_STATUS := 'ERR';
       END IF;
       P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    END IF;
    
  COMMIT;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_MPW;


procedure P_MOU_TRAN_TARIFF_MS  (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for MS
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
-----------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_MS';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing MS xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

 select count(TARIFFCODE_PK) into l_db_count_before_insert from MO_TARIFF;
l_serv_comp := 'MS';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_MS_XML.xml', 'R');
LOOP
BEGIN
UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
xmlClob_full_file := xmlClob_full_file||xmlClob_line;
EXCEPTION WHEN No_Data_Found THEN EXIT; END;
END LOOP;

UTL_FILE.FCLOSE(xmlFile);

x := XMLType.createXML(xmlClob_full_file);
  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus

          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MS/TariffCode/text()') as tar_type_ms_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_MS/MSSupplyPointFixedCharges/text()') as MSSupplyPointFixedCharges

          ,ExtractValue(Value(p),'/Row/MO_BLOCK/TariffCode/text()') as MO_BLOCK_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_BLOCK/UpperAnnualVol/text()') as MO_BLOCK_UpperAnnualVol
            ,ExtractValue(Value(p),'/Row/MO_BLOCK/Charge/text()') as MO_BLOCK_Charge


          ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as MO_METER_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_METER_LowerMeterSize
            ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_METER_UpperMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_METER_Charge


    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_MS/Row'))) p
    ) LOOP
l_progress := 'processing MS xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN

if r.TariffCode is not null then
l_progress := 'processing MO_TARIFF';
l_tariff_count := l_tariff_count+1;
insert into MO_TARIFF (TARIFFCODE_PK,
TARIFFEFFECTIVEFROMDATE,
TARIFFSTATUS,
TARIFFLEGACYEFFECTIVEFROMDATE,
APPLICABLESERVICECOMPONENT,
TARIFFAUTHCODE,
--VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
--VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
SERVICECOMPONENTTYPE,
TARIFFNAME)
VALUES
(trim(r.TariffCode),
to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
upper(r.TariffStatus), -- issue 28 fix
to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
l_serv_comp,
r.Tariffauthorisationcode,
--r.VACANCYCHARGIGMETHODWATER,
--r.VACANCYCHARGINGMETHODSEWERAGE,
--r.TEMPDISCONCHARGINGMETHODWAT,
--r.TEMPDISCONCHARGINGMETHODSEW ,
L_SERV_COMP,
trim(r.TariffName));
 end if;


    if r.tar_ver_tariffcode is not null then
    l_progress := 'processing MO_TARIFF_VERSION';
select nvl(max(TARIFFVERSION),0) into l_tariff_ver from MO_TARIFF_VERSION where tariffcode_pk=r.tar_ver_tariffcode;

insert into MO_TARIFF_VERSION  (
TARIFF_VERSION_PK,
TARIFFCODE_PK,
TARIFFVERSION,
TARIFFVEREFFECTIVEFROMDATE,
TARIFFSTATUS,
APPLICABLESERVICECOMPONENT,
--DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
TARIFFCOMPONENTTYPE,
SECTION154PAYMENTVALUE,
STATE/* DB patch 15 fix*/)
values
(TARIFF_VERSION_PK_SEQ.NEXTVAL,--TARIFF_VERSION_PK
trim(r.tar_ver_tariffcode),--TARIFFCODE_PK
l_tariff_ver+1,--TARIFFVERSION
to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
upper(r.tarver_TariffStatus),--TARIFFSTATUS-- issue 28 fix
L_SERV_COMP,--APPLICABLESERVICECOMPONENT
--nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
l_serv_comp,--TARIFFCOMPONENTTYPE
NULL,
g_state);--SECTION154PAYMENTVALUE););

end if;


 if r.tar_type_ms_tariff_code is not null then
 l_progress := 'processing MO_TARIFF_TYPE_MS';
insert into MO_TARIFF_TYPE_MS (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
MSSUPPLYPOINTFIXEDCHARGES,
MSEFFECTIVEFROMDATE
)
values
(
MS_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
TARIFF_VERSION_PK_SEQ.currval,--TARIFF_VERSION_PK
r.MSSupplyPointFixedCharges,--MSSUPPLYPOINTFIXEDCHARGES
null
);
end if;

 if r.MO_BLOCK_tariff_code is not null then
--select ADDRESS_PK_SEQ.nextval into l_tariff_type_pk from dual;
l_progress := 'processing MO_MS_BLOCK_MSBT';
insert into MO_MS_BLOCK_MSBT (
TARIFF_MWBT_PK,
TARIFF_TYPE_PK,
UPPERANNUALVOL,
CHARGE
)
values
(
MS_TARIFF_MWBT_PK_SEQ.nextval,
MS_TARIFF_TYPE_PK_SEQ.currval,
r.MO_BLOCK_UpperAnnualVol,
r.MO_BLOCK_Charge
);
end if;


 if r.MO_METER_TariffCode is not null then
 l_progress := 'processing MO_MS_METER_MSMFC';
insert into MO_MS_METER_MSMFC (
TARIFF_MSMFC_PK,
TARIFF_TYPE_PK,
LOWERMETERSIZE,
UPPERMETERSIZE,
CHARGE
)
values
(
MS_TARIFF_MSMFC_PK_SEQ.nextval,
MS_TARIFF_TYPE_PK_SEQ.currval,
r.MO_METER_LowerMeterSize,
r.MO_METER_UpperMeterSize,
r.MO_METER_Charge
);
end if;

    EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
             --g_err_rows := g_err_rows+1;
             l_error_number := SQLCODE;
             l_error_message := sqlerrm;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2394, l_tariff_count,    'Distinct MS tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2404, l_tariff_dropped,    'Distinct MS tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2414, l_db_rows_inserted,  'Distinct MS tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;


l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2394, l_tariff_count,    'Distinct MS tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2404, l_tariff_dropped,    'Distinct MS tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2414, l_db_rows_inserted,  'Distinct MS tariffs written to MO_TARIFFs during Transform');

IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

Commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_MS;




 procedure P_MOU_TRAN_TARIFF_SW (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for SW
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-----------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_SW';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing SW xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

 select count(TARIFFCODE_PK) into l_db_count_before_insert from MO_TARIFF;
l_serv_comp := 'SW';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_SW_XML.xml', 'R');
LOOP
BEGIN
UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
xmlClob_full_file := xmlClob_full_file||xmlClob_line;
EXCEPTION WHEN No_Data_Found THEN EXIT; END;
END LOOP;

UTL_FILE.FCLOSE(xmlFile);

x := XMLType.createXML(xmlClob_full_file);

  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus

          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/TariffCode/text()') as tar_type_swd_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/SWCOMBAND/text()') as SWCOMBAND
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/SWFIXEDCHARGE/text()') as SWFIXEDCHARGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/SWRVPOUNDAGE/text()') as SWRVPOUNDAGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/SWRVTHRESHOLD/text()') as SWRVTHRESHOLD
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/SWRVMAXCHARGE/text()') as SWRVMAXCHARGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_SWD/SWRVMINCHARGE/text()') as SWRVMINCHARGE

          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/TariffCode/text()') as mo_area_band_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/LowerArea/text()') as MO_Area_Band_LowerArea
          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/UpperArea/text()') as MO_Area_Band_UpperArea
          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/Band/text()') as MO_Area_Band

          ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/TariffCode/text()') as mo_band_charge_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Band/text()') as Band_charge_Band
          ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Charge/text()') as Band_charge_Charge

          ,ExtractValue(Value(p),'/Row/MO_BLOCK/TariffCode/text()') as mo_block_tariff_code
           ,ExtractValue(Value(p),'/Row/MO_BLOCK/UpperAnnualVol/text()') as MO_Block_UpperAnnualVol
          ,ExtractValue(Value(p),'/Row/MO_BLOCK/Charge/text()') as MO_Block_Charge

           ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as mo_meter_tariff_code
            ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_Meter_LowerMeterSize
           ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_Meter_UpperMeterSize
            ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_Meter_Charge

    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_SWD/Row'))) p
    ) LOOP

l_progress := 'processing TE xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN

if r.TariffCode is not null then
l_progress := 'processing MO_TARIFF';
l_tariff_count := l_tariff_count+1;
insert into MO_TARIFF (TARIFFCODE_PK,
TARIFFEFFECTIVEFROMDATE,
TARIFFSTATUS,
TARIFFLEGACYEFFECTIVEFROMDATE,
APPLICABLESERVICECOMPONENT,
TARIFFAUTHCODE,
--VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
--VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
SERVICECOMPONENTTYPE,
TARIFFNAME)
VALUES
(trim(r.TariffCode),
to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
upper(r.TariffStatus), -- issue 28 fix
to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
l_serv_comp,
r.Tariffauthorisationcode,
--r.VACANCYCHARGIGMETHODWATER,
--r.VACANCYCHARGINGMETHODSEWERAGE,
--r.TEMPDISCONCHARGINGMETHODWAT,
--r.TEMPDISCONCHARGINGMETHODSEW ,
L_SERV_COMP,
trim(r.TariffName));
 end if;


    if r.tar_ver_tariffcode is not null then
    l_progress := 'processing MO_TARIFF_VERSION';
select nvl(max(TARIFFVERSION),0) into l_tariff_ver from MO_TARIFF_VERSION where tariffcode_pk=r.tar_ver_tariffcode;

insert into MO_TARIFF_VERSION  (
TARIFF_VERSION_PK,
TARIFFCODE_PK,
TARIFFVERSION,
TARIFFVEREFFECTIVEFROMDATE,
TARIFFSTATUS,
APPLICABLESERVICECOMPONENT,
--DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
TARIFFCOMPONENTTYPE,
SECTION154PAYMENTVALUE,
STATE/* DB patch 15 fix*/)
values
(TARIFF_VERSION_PK_SEQ.NEXTVAL,--TARIFF_VERSION_PK
trim(r.tar_ver_tariffcode),--TARIFFCODE_PK
l_tariff_ver+1,--TARIFFVERSION
to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
upper(r.tarver_TariffStatus),--TARIFFSTATUS-- issue 28 fix
L_SERV_COMP,--APPLICABLESERVICECOMPONENT
--nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
l_serv_comp,--TARIFFCOMPONENTTYPE
NULL,
g_state);--SECTION154PAYMENTVALUE););

end if;
--SPSP tariff type pk - use actual seq
   if r.tar_type_swd_tariff_code is not null then
l_progress := 'processing MO_TARIFF_TYPE_SW';
insert into MO_TARIFF_TYPE_SW (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
SWCOMBAND,
SWFIXEDCHARGE,
SWRVPOUNDAGE,
SWRVTHRESHOLD,
SWRVMAXIMUMCHARGE,
SWRVMINIMUMCHARGE,
SWMETERFIXEDCHARGES)
values
(
SW_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
TARIFF_VERSION_PK_SEQ.currval,--TARIFF_VERSION_PK
r.SWCOMBAND,--SWCOMBAND
r.SWFIXEDCHARGE,--SWFIXEDCHARGE
r.SWRVPOUNDAGE,--SWRVPOUNDAGE
r.SWRVTHRESHOLD,--SWRVTHRESHOLD
r.SWRVMAXCHARGE,--SWRVMAXIMUMCHARGE
r.SWRVMINCHARGE,--SWRVMINIMUMCHARGE
null--SWMETERFIXEDCHARGES
);--SECTION154PAYMENTVALUE););
end if;

if r.mo_area_band_tariff_code is not null then
l_progress := 'processing MO_SW_AREA_BAND';
insert into MO_SW_AREA_BAND (
TARIFF_AREA_BAND_PK,
TARIFF_TYPE_PK,
LOWERAREA,
UPPERAREA,
BAND )
values
(
SW_TARIFF_AREA_BAND_PK_SEQ.nextval,--TARIFF_AREA_BAND_PK
SW_TARIFF_TYPE_PK_SEQ.currval,--TARIFF_TYPE_PK
r.MO_Area_Band_LowerArea,--LOWERAREA,
r.MO_Area_Band_UpperArea,--UPPERAREA,
r.MO_Area_Band--BAND
);

end if;

if r.mo_band_charge_tariff_code is not null then
l_progress := 'processing MO_SW_BAND_CHARGE';
insert into MO_SW_BAND_CHARGE (
TARIFF_BAND_CHARGE_PK,
TARIFF_TYPE_PK,
BAND,
CHARGE )
values
(
SW_TARIFF_AREA_BAND_PK_SEQ.nextval,--TARIFF_AREA_BAND_PK
SW_TARIFF_TYPE_PK_SEQ.currval,--TARIFF_TYPE_PK
r.Band_charge_Band,--BAND,
r.Band_charge_Charge--CHARGE,
);
end if;


if r.mo_block_tariff_code is not null then
insert into MO_SW_BLOCK_SWBT (
TARIFF_SWBT_PK,
TARIFF_TYPE_PK,
UPPERANNUALVOL,
CHARGE )
values
(
SW_TARIFF_SWBT_PK_SEQ.nextval,--TARIFF_AREA_BAND_PK
SW_TARIFF_TYPE_PK_SEQ.currval,--TARIFF_TYPE_PK
r.MO_Block_UpperAnnualVol,--BAND,
r.MO_Block_Charge--CHARGE,
);
end if;

 if r.mo_meter_tariff_code is not null then
 l_progress := 'processing MO_SW_METER_SWMFC';
insert into MO_SW_METER_SWMFC (
TARIFF_SWMFC_PK,
TARIFF_TYPE_PK,
LOWERMETERSIZE,
UPPERMETERSIZE,
CHARGE )
values
(
SW_TARIFF_SWMFC_PK_SEQ.nextval,--TARIFF_AREA_BAND_PK
SW_TARIFF_TYPE_PK_SEQ.currval,--TARIFF_TYPE_PK
r.MO_Meter_LowerMeterSize,--BAND,
r.MO_Meter_UpperMeterSize,--CHARGE,
r.MO_Meter_Charge
);
end if;

   EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
            -- g_err_rows := g_err_rows+1;
             l_error_number := SQLCODE;
             l_error_message := sqlerrm;
             if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2390, l_tariff_count,    'Distinct SW tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2400, l_tariff_dropped,    'Distinct SW tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2410, l_db_rows_inserted,  'Distinct SW tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;

l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2390, l_tariff_count,    'Distinct SW tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2400, l_tariff_dropped,    'Distinct SW tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2410, l_db_rows_inserted,  'Distinct SW tariffs written to MO_TARIFFs during Transform');

IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

Commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_SW;

  procedure P_MOU_TRAN_TARIFF_TE     (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for TE
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
-----------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_TE';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing TE xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

SELECT COUNT(TARIFFCODE_PK) INTO l_db_count_before_insert FROM MO_TARIFF;
l_serv_comp := 'TE';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_TE_XML.xml', 'R');
LOOP
  BEGIN
    UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
    xmlClob_full_file := xmlClob_full_file||xmlClob_line;
  EXCEPTION
  WHEN No_Data_Found THEN
    EXIT;
  END;
END LOOP;
UTL_FILE.FCLOSE(xmlFile);
x := XMLType.createXML(xmlClob_full_file);

FOR r IN
(SELECT ExtractValue(Value(p),'/Row/TariffCode/text()')                          AS TariffCode ,
  ExtractValue(Value(p),'/Row/TariffName/text()')                                AS TariffName ,
  ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()')                   AS TARIFFEFFECTIVEFROMDATE ,
  ExtractValue(Value(p),'/Row/TariffStatus/text()')                              AS TariffStatus ,
  ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()')             AS TARIFFLEGACYEFFECTIVEFROMDATE ,
  ExtractValue(Value(p),'/Row/ServiceComponent/text()')                          AS ServiceComponent ,
  ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()')                   AS Tariffauthorisationcode ,
  ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()')                 AS VACANCYCHARGIGMETHODWATER ,
  ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()')             AS VACANCYCHARGINGMETHODSEWERAGE ,
  ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()')               AS TEMPDISCONCHARGINGMETHODWAT ,
  ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()')               AS TEMPDISCONCHARGINGMETHODSEW ,
  ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()')    AS Default_Return_to_Sewer ,
  ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()')                 AS tar_ver_tariffcode ,
  ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') AS TARIFFVEREFFECTIVEFROMDATE ,
  ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()')               AS tarver_TariffStatus ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TariffCode/text()')              AS MO_TARIFF_TE_TariffCode ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPRA/text()')          AS MO_TARIFF_TE_TECHARGECOMPRA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPVA/text()')          AS MO_TARIFF_TE_TECHARGECOMPVA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPBVA/text()')         AS MO_TARIFF_TE_TECHARGECOMPBVA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPMA/text()')          AS MO_TARIFF_TE_TECHARGECOMPMA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPBA/text()')          AS MO_TARIFF_TE_TECHARGECOMPBA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPSA/text()')          AS MO_TARIFF_TE_TECHARGECOMPSA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPAA/text()')          AS MO_TARIFF_TE_TECHARGECOMPAA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPVO/text()')          AS MO_TARIFF_TE_TECHARGECOMPVO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPBVO/text()')         AS MO_TARIFF_TE_TECHARGECOMPBVO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPMO/text()')          AS MO_TARIFF_TE_TECHARGECOMPMO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPSO/text()')          AS MO_TARIFF_TE_TECHARGECOMPSO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPAO/text()')          AS MO_TARIFF_TE_TECHARGECOMPAO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPOS/text()')          AS MO_TARIFF_TE_TECHARGECOMPOS ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPSS/text()')          AS MO_TARIFF_TE_TECHARGECOMPSS ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPAS/text()')          AS MO_TARIFF_TE_TECHARGECOMPAS ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPAM/text()')          AS MO_TARIFF_TE_TECHARGECOMPAM ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TEMINCHARGE/text()')             AS MO_TARIFF_TE_TEMINCHARGE ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TEFIXEDCHARGE/text()')           AS MO_TARIFF_TE_TEFIXEDCHARGE ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPXA/text()')          AS MO_TARIFF_TE_TECHARGECOMPXA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TCHARGECOMPYA/text()')           AS MO_TARIFF_TE_TCHARGECOMPYA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPZA/text()')          AS MO_TARIFF_TE_TECHARGECOMPZA ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPXO/text()')          AS MO_TARIFF_TE_TECHARGECOMPXO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPYO/text()')          AS MO_TARIFF_TE_TECHARGECOMPYO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TEHARGECOMPZO/text()')           AS MO_TARIFF_TE_TEHARGECOMPZO ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPXS/text()')          AS MO_TARIFF_TE_TECHARGECOMPXS ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPYS/text()')          AS MO_TARIFF_TE_TECHARGECOMPYS ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPZS/text()')          AS MO_TARIFF_TE_TECHARGECOMPZS ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPXM/text()')          AS MO_TARIFF_TE_TECHARGECOMPXM ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHRGECOMPYM/text()')           AS MO_TARIFF_TE_TECHRGECOMPYM ,
  ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_TE/TECHARGECOMPZM/text()')          AS MO_TARIFF_TE_TECHARGECOMPZM ,
  ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/TariffCode/text()')                 AS MO_BAND_CHARGE_TariffCode ,
  ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Band/text()')                       AS MO_BAND_CHARGE_Band ,
  ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Charge/text()')                     AS MO_BAND_CHARGE_Charge ,
  ExtractValue(Value(p),'/Row/MO_RO_BLOCK/TariffCode/text()')                    AS MO_RO_BLOCK_TariffCode ,
  ExtractValue(Value(p),'/Row/MO_RO_BLOCK/UpperAnnualVol/text()')                AS MO_RO_BLOCK_UpperAnnualVol ,
  ExtractValue(Value(p),'/Row/MO_RO_BLOCK/Charge/text()')                        AS MO_RO_BLOCK_Charge ,
  ExtractValue(Value(p),'/Row/MO_BO_BLOCK/TariffCode/text()')                    AS MO_BO_BLOCK_TariffCode ,
  ExtractValue(Value(p),'/Row/MO_BO_BLOCK/UpperAnnualVol/text()')                AS MO_BO_BLOCK_UpperAnnualVol ,
  ExtractValue(Value(p),'/Row/MO_BO_BLOCK/Charge/text()')                        AS MO_BO_BLOCK_Charge
FROM TABLE(XMLSequence(Extract(x,'/TARIFF_TE/Row'))) p
)
LOOP
l_progress := 'processing TE xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode||r.MO_TARIFF_TE_TariffCode;

BEGIN

IF r.TariffCode  IS NOT NULL THEN
  l_progress     := 'processing MO_TARIFF';
  l_tariff_count := l_tariff_count+1;
  
  INSERT
  INTO MO_TARIFF
    (
      TARIFFCODE_PK,
      TARIFFEFFECTIVEFROMDATE,
      TARIFFSTATUS,
      TARIFFLEGACYEFFECTIVEFROMDATE,
      APPLICABLESERVICECOMPONENT,
      TARIFFAUTHCODE,
      --VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
      --VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
      --TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
      --TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
      SERVICECOMPONENTTYPE,
      TARIFFNAME
    )
    VALUES
    (
      trim(r.TariffCode),
      to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
      upper(r.TariffStatus), -- issue 28 fix
      to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
      l_serv_comp,
      r.Tariffauthorisationcode,
      --r.VACANCYCHARGIGMETHODWATER,
      --r.VACANCYCHARGINGMETHODSEWERAGE,
      --r.TEMPDISCONCHARGINGMETHODWAT,
      --r.TEMPDISCONCHARGINGMETHODSEW ,
      L_SERV_COMP,
      trim(r.TariffName)
    );
END IF;

IF r.tar_ver_tariffcode IS NOT NULL THEN
  l_progress            := 'processing MO_TARIFF_VERSION';
  SELECT NVL(MAX(TARIFFVERSION),0)
  INTO l_tariff_ver
  FROM MO_TARIFF_VERSION
  WHERE tariffcode_pk=r.tar_ver_tariffcode;
  INSERT
  INTO MO_TARIFF_VERSION
    (
      TARIFF_VERSION_PK,
      TARIFFCODE_PK,
      TARIFFVERSION,
      TARIFFVEREFFECTIVEFROMDATE,
      TARIFFSTATUS,
      APPLICABLESERVICECOMPONENT,
      --DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
      TARIFFCOMPONENTTYPE,
      SECTION154PAYMENTVALUE,
      STATE
      /* DB patch 15 fix*/
    )
    VALUES
    (
      TARIFF_VERSION_PK_SEQ.NEXTVAL,                     --TARIFF_VERSION_PK
      trim(r.tar_ver_tariffcode),                        --TARIFFCODE_PK
      l_tariff_ver+1,                                    --TARIFFVERSION
      to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
      upper(r.tarver_TariffStatus),                      --TARIFFSTATUS-- issue 28 fix
      L_SERV_COMP,                                       --APPLICABLESERVICECOMPONENT
      --nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
      l_serv_comp,--TARIFFCOMPONENTTYPE
      NULL,
      g_state
    );--SECTION154PAYMENTVALUE););
END IF;

--
   if r.MO_TARIFF_TE_TariffCode is not null then
l_progress := 'processing MO_TARIFF_TYPE_TE';
insert into MO_TARIFF_TYPE_TE (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
TEFIXEDCHARGE,
TECHARGECOMPRA,
TECHARGECOMPVA,
TECHARGECOMPBVA,
TECHARGECOMPMA,
TECHARGECOMPBA,
TECHARGECOMPSA,
TECHARGECOMPAA,
TECHARGECOMPVO,
TECHARGECOMPBVO,
TECHARGECOMPMO,
TECHARGECOMPSO,
TECHARGECOMPAO,
TECHARGECOMPOS,
TECHARGECOMPSS,
TECHARGECOMPAS,
TECHARGECOMPAM,
TEMINCHARGE,
TECHARGECOMPXA,
TECHARGECOMPYA,
TECHARGECOMPZA,
TECHARGECOMPXO,
TECHARGECOMPYO,
TECHARGECOMPZO,
TECHARGECOMPXS,
TECHARGECOMPYS,
TECHARGECOMPZS,
TECHARGECOMPXM,
TECHARGECOMPYM,
TECHARGECOMPZM
)
values
(
TE_TARIFF_TYPE_PK_SEQ.nextval,
TARIFF_VERSION_PK_SEQ.currval,
r.MO_TARIFF_TE_TEFIXEDCHARGE,--TEFIXEDCHARGE,
r.MO_TARIFF_TE_TECHARGECOMPRA  ,--TECHARGECOMPRA,
r.MO_TARIFF_TE_TECHARGECOMPVA,--TECHARGECOMPVA,
r.MO_TARIFF_TE_TECHARGECOMPBVA,--TECHARGECOMPBVA,
r.MO_TARIFF_TE_TECHARGECOMPMA ,--TECHARGECOMPMA,
r.MO_TARIFF_TE_TECHARGECOMPBA ,--TECHARGECOMPBA,
r.MO_TARIFF_TE_TECHARGECOMPSA,--TECHARGECOMPSA,
r.MO_TARIFF_TE_TECHARGECOMPAA ,--TECHARGECOMPAA,
r.MO_TARIFF_TE_TECHARGECOMPVO ,--TECHARGECOMPVO,
r.MO_TARIFF_TE_TECHARGECOMPBVO ,--TECHARGECOMPBVO,
r.MO_TARIFF_TE_TECHARGECOMPMO,--TECHARGECOMPMO,
r.MO_TARIFF_TE_TECHARGECOMPSO ,--TECHARGECOMPSO,
r.MO_TARIFF_TE_TECHARGECOMPAO,--TECHARGECOMPAO,
r.MO_TARIFF_TE_TECHARGECOMPOS   ,--TECHARGECOMPOS,
r.MO_TARIFF_TE_TECHARGECOMPSS,--TECHARGECOMPSS,
r.MO_TARIFF_TE_TECHARGECOMPAS  ,--TECHARGECOMPAS,
r.MO_TARIFF_TE_TECHARGECOMPAM ,--TECHARGECOMPAM,
r.MO_TARIFF_TE_TEMINCHARGE,--TEMINCHARGE,
r.MO_TARIFF_TE_TECHARGECOMPXA ,--TECHARGECOMPXA,
r.MO_TARIFF_TE_TCHARGECOMPYA ,--TECHARGECOMPYA,
r.MO_TARIFF_TE_TECHARGECOMPZA,--TECHARGECOMPZA,
r.MO_TARIFF_TE_TECHARGECOMPXO,--TECHARGECOMPXO,
r.MO_TARIFF_TE_TECHARGECOMPYO,--TECHARGECOMPYO,
r.MO_TARIFF_TE_TEHARGECOMPZO,--TECHARGECOMPZO,
r.MO_TARIFF_TE_TECHARGECOMPXS,--TECHARGECOMPXS,
r.MO_TARIFF_TE_TECHARGECOMPYS,--TECHARGECOMPYS,
r.MO_TARIFF_TE_TECHARGECOMPZS,--TECHARGECOMPZS,
r.MO_TARIFF_TE_TECHARGECOMPXM,--TECHARGECOMPXM,
r.MO_TARIFF_TE_TECHRGECOMPYM,--TECHARGECOMPYM,
r.MO_TARIFF_TE_TECHARGECOMPZM--TECHARGECOMPZM
  );
end if;


if r.MO_RO_BLOCK_TariffCode is not null then
l_progress := 'processing MO_TE_BLOCK_ROBT';
insert into MO_TE_BLOCK_ROBT (
TARIFF_ROBT_PK,
TARIFF_TYPE_PK,
UPPERANNUALVOL,
CHARGE
)
values
(
TE_TARIFF_ROBT_PK_SEQ.nextval,
TE_TARIFF_TYPE_PK_SEQ.currval,
r.MO_RO_BLOCK_UpperAnnualVol,
r.MO_RO_BLOCK_Charge
);

end if;

if r.MO_BO_BLOCK_TariffCode is not null then
l_progress := 'processing MO_TE_BLOCK_BOBT';
insert into MO_TE_BLOCK_BOBT (
TARIFF_BOBT_PK,
TARIFF_TYPE_PK,
UPPERANNUALVOL,
CHARGE
)
values
(
TE_TARIFF_BOBT_PK_SEQ.nextval,
TE_TARIFF_TYPE_PK_SEQ.currval,
r.MO_BO_BLOCK_UpperAnnualVol,
r.MO_BO_BLOCK_Charge
);
end if;

 if r.MO_BAND_CHARGE_TariffCode is not null then
 l_progress := 'processing MO_TE_BAND_CHARGE';
insert into MO_TE_BAND_CHARGE (
TARIFF_BAND_CHARGE_PK,
TARIFF_TYPE_PK,
BAND,
CHARGE
)
values
(
TE_TARIFF_BAND_CHARGE_PK_SEQ.nextval,
TE_TARIFF_TYPE_PK_SEQ.currval,
r.MO_BAND_CHARGE_Band,
r.MO_BAND_CHARGE_Charge
);
end if;

   EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
            -- g_err_rows := g_err_rows+1;
             l_error_number := SQLCODE;
             l_error_message := sqlerrm;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2392, l_tariff_count,    'Distinct TE tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2402, l_tariff_dropped,    'Distinct TE tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2412, l_db_rows_inserted,  'Distinct TE tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;


l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2392, l_tariff_count,    'Distinct TE tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2402, l_tariff_dropped,    'Distinct TE tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2412, l_db_rows_inserted,  'Distinct TE tariffs written to MO_TARIFFs during Transform');

IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

Commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_TE;


  procedure P_MOU_TRAN_TARIFF_US   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for US
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
-----------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_US';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing US xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

 select count(TARIFFCODE_PK) into l_db_count_before_insert from MO_TARIFF;

l_serv_comp := 'US';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_US_XML.xml', 'R');
LOOP
BEGIN
UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
xmlClob_full_file := xmlClob_full_file||xmlClob_line;
EXCEPTION WHEN No_Data_Found THEN EXIT; END;
END LOOP;

UTL_FILE.FCLOSE(xmlFile);

x := XMLType.createXML(xmlClob_full_file);

  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus




          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/TariffCode/text()') as MO_TARIFF_TYPE_US_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USFIXEDCHARGE/text()') as MO_TARIFF_US_USFIXEDCHARGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USRVPOUNDAGE/text()') as MO_TARIFF_US_USRVPOUNDAGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USRVTHRESHOLD/text()') as MO_TARIFF_US_USRVTHRESHOLD
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USRVMAXIMUMCHARGE/text()') as MO_TARIFF_US_USRVMAXIMUMCHAR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USRVMinimumCharge/text()') as MO_TARIFF_US_USRVMinimumChar
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEACHARGE/text()') as MO_TARIFF_US_USMISCTYPEACHAR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEBCHARGE/text()') as MO_TARIFF_US_USMISCTYPEBCHAR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPECCHARGE/text()') as MO_TARIFF_US_USMISCTYPECCHAR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEDCHARGE/text()') as MO_TARIFF_US_USMISCTYPEDCHAR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEECHARGE/text()') as MO_TARIFF_US_USMISCTYPEECHAR
           ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEFCHARGE/text()') as MO_TARIFF_US_USMISCTYPEFCHAR
           ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEGCHARGE/text()') as MO_TARIFF_US_USMISCTYPEGCHAR
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_US/USMISCTYPEHCHARGE/text()') as MO_TARIFF_US_USMISCTYPEHCHAR


            ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as MO_METER_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_METER_LowerMeterSize
            ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_METER_UpperMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_METER_Charge



    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_US/Row'))) p
    ) LOOP


l_progress := 'processing US xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN

if r.TariffCode is not null then
l_progress := 'processing MO_TARIFF';
l_tariff_count := l_tariff_count+1;
insert into MO_TARIFF (TARIFFCODE_PK,
TARIFFEFFECTIVEFROMDATE,
TARIFFSTATUS,
TARIFFLEGACYEFFECTIVEFROMDATE,
APPLICABLESERVICECOMPONENT,
TARIFFAUTHCODE,
--VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
--VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
SERVICECOMPONENTTYPE,
TARIFFNAME)
VALUES
(trim(r.TariffCode),
to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
upper(r.TariffStatus), -- issue 28 fix
to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
l_serv_comp,
r.Tariffauthorisationcode,
--r.VACANCYCHARGIGMETHODWATER,
--r.VACANCYCHARGINGMETHODSEWERAGE,
--r.TEMPDISCONCHARGINGMETHODWAT,
--r.TEMPDISCONCHARGINGMETHODSEW ,
L_SERV_COMP,
trim(r.TariffName));
 end if;


    if r.tar_ver_tariffcode is not null then
    l_progress := 'processing MO_TARIFF_VERSION';
select nvl(max(TARIFFVERSION),0) into l_tariff_ver from MO_TARIFF_VERSION where tariffcode_pk=r.tar_ver_tariffcode;

insert into MO_TARIFF_VERSION  (
TARIFF_VERSION_PK,
TARIFFCODE_PK,
TARIFFVERSION,
TARIFFVEREFFECTIVEFROMDATE,
TARIFFSTATUS,
APPLICABLESERVICECOMPONENT,
--DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
TARIFFCOMPONENTTYPE,
SECTION154PAYMENTVALUE,
STATE/* DB patch 15 fix*/)
values
(TARIFF_VERSION_PK_SEQ.NEXTVAL,--TARIFF_VERSION_PK
trim(r.tar_ver_tariffcode),--TARIFFCODE_PK
l_tariff_ver+1,--TARIFFVERSION
to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
upper(r.tarver_TariffStatus),--TARIFFSTATUS-- issue 28 fix
L_SERV_COMP,--APPLICABLESERVICECOMPONENT
--nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
l_serv_comp,--TARIFFCOMPONENTTYPE
NULL,
g_state);--SECTION154PAYMENTVALUE););

end if;

 if r.MO_TARIFF_TYPE_US_TariffCode is not null then
l_progress := 'processing MO_TARIFF_TYPE_US';

insert into MO_TARIFF_TYPE_US (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
USFIXEDCHARGE,
USRVPOUNDAGE,
USRVTHRESHOLD,
USRVMAXIMUMCHARGE,
USRVMINIMUMCHARGE,
USMISCTYPEACHARGE,
USMISCTYPEBCHARGE,
USMISCTYPECCHARGE,
USMISCTYPEDCHARGE,
USMISCTYPEECHARGE,
USMISCTYPEFCHARGE,
USMISCTYPEGCHARGE,
USMISCTYPEHCHARGE
)
values
(
US_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
TARIFF_VERSION_PK_SEQ.currval,
r.MO_TARIFF_US_USFIXEDCHARGE,
r.MO_TARIFF_US_USRVPOUNDAGE,
r.MO_TARIFF_US_USRVTHRESHOLD,
r.MO_TARIFF_US_USRVMAXIMUMCHAR,
r.MO_TARIFF_US_USRVMinimumChar ,
r.MO_TARIFF_US_USMISCTYPEACHAR  ,
r.MO_TARIFF_US_USMISCTYPEBCHAR   ,
r.MO_TARIFF_US_USMISCTYPECCHAR,
r.MO_TARIFF_US_USMISCTYPEDCHAR ,
r.MO_TARIFF_US_USMISCTYPEECHAR,
r.MO_TARIFF_US_USMISCTYPEFCHAR,
r.MO_TARIFF_US_USMISCTYPEGCHAR,
r.MO_TARIFF_US_USMISCTYPEHCHAR
  );
end if;


 if r.MO_METER_TariffCode is not null then
 l_progress := 'processing MO_US_METER_USPFC';
insert into MO_US_METER_USPFC (
TARIFF_USPFC_PK,
TARIFF_TYPE_PK,
LOWERMETERSIZE,
UPPERMETERSIZE,
CHARGE
)
values
(
US_TARIFF_USPFC_PK_SEQ.nextval,
US_TARIFF_TYPE_PK_SEQ.currval,
r.MO_METER_LowerMeterSize,
r.MO_METER_UpperMeterSize,
r.MO_METER_Charge
);
end if;

    EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
            -- g_err_rows := g_err_rows+1;
             l_error_number := SQLCODE;
             l_error_message := sqlerrm;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2391, l_tariff_count,    'Distinct US tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2401, l_tariff_dropped,    'Distinct US tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2411, l_db_rows_inserted,  'Distinct US tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;

l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2391, l_tariff_count,    'Distinct US tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2401, l_tariff_dropped,    'Distinct US tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2411, l_db_rows_inserted,  'Distinct US tariffs written to MO_TARIFFs during Transform');

  IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;
Commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_US;

  procedure P_MOU_TRAN_TARIFF_UW   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
-----------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_UW';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing UW xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

 select count(TARIFFCODE_PK) into l_db_count_before_insert from MO_TARIFF;


l_serv_comp := 'UW';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_UW_XML.xml', 'R');
LOOP
BEGIN
UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
xmlClob_full_file := xmlClob_full_file||xmlClob_line;
EXCEPTION WHEN No_Data_Found THEN EXIT; END;
END LOOP;

UTL_FILE.FCLOSE(xmlFile);

x := XMLType.createXML(xmlClob_full_file);
  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus

          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/TariffCode/text()') as MO_TARIFF_TYPE_UW_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWFIXEDCHARGE/text()') as MO_TARIFF_TYPE_UW_UWFIXEDCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWRVPOUNDAGE/text()') as MO_TARIFF_TYPE_UW_UWRVPOUND
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWRVTHRESHOLD/text()') as MO_TARIFF_UW_UWRVTHRESHOLD
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWRVMAXIMUMCHARGE/text()') as MO_TARIFF_UW_UWRVMAXIMUMCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWRVMinimumCharge/text()') as MO_TARIFF_UW_UWRVMinimumCha
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEACHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEACHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEBCHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEBCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPECCHARGE/text()') as MO_TARIFF_UW_UWMISCTYPECCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEDCHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEDCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEECHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEECHA
           ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEFCHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEFCHA
           ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEGCHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEGCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_UW/UWMISCTYPEHCHARGE/text()') as MO_TARIFF_UW_UWMISCTYPEHCHA


          ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as MO_METER_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_METER_LowerMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_METER_UpperMeterSize
          ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_METER_Charge



    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_UW/Row'))) p
    ) LOOP

l_progress := 'processing UW xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN

if r.TariffCode is not null then
l_progress := 'processing MO_TARIFF';
l_tariff_count := l_tariff_count+1;
insert into MO_TARIFF (TARIFFCODE_PK,
TARIFFEFFECTIVEFROMDATE,
TARIFFSTATUS,
TARIFFLEGACYEFFECTIVEFROMDATE,
APPLICABLESERVICECOMPONENT,
TARIFFAUTHCODE,
--VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
--VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
SERVICECOMPONENTTYPE,
TARIFFNAME)
VALUES
(trim(r.TariffCode),
to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
upper(r.TariffStatus), -- issue 28 fix
to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
l_serv_comp,
r.Tariffauthorisationcode,
--r.VACANCYCHARGIGMETHODWATER,
--r.VACANCYCHARGINGMETHODSEWERAGE,
--r.TEMPDISCONCHARGINGMETHODWAT,
--r.TEMPDISCONCHARGINGMETHODSEW ,
L_SERV_COMP,
trim(r.TariffName));
 end if;


    if r.tar_ver_tariffcode is not null then
    l_progress := 'processing MO_TARIFF_VERSION';
select nvl(max(TARIFFVERSION),0) into l_tariff_ver from MO_TARIFF_VERSION where tariffcode_pk=r.tar_ver_tariffcode;

insert into MO_TARIFF_VERSION  (
TARIFF_VERSION_PK,
TARIFFCODE_PK,
TARIFFVERSION,
TARIFFVEREFFECTIVEFROMDATE,
TARIFFSTATUS,
APPLICABLESERVICECOMPONENT,
--DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
TARIFFCOMPONENTTYPE,
SECTION154PAYMENTVALUE,
STATE/* DB patch 15 fix*/)
values
(TARIFF_VERSION_PK_SEQ.NEXTVAL,--TARIFF_VERSION_PK
trim(r.tar_ver_tariffcode),--TARIFFCODE_PK
l_tariff_ver+1,--TARIFFVERSION
to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
upper(r.tarver_TariffStatus),--TARIFFSTATUS-- issue 28 fix
L_SERV_COMP,--APPLICABLESERVICECOMPONENT
--nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
l_serv_comp,--TARIFFCOMPONENTTYPE
NULL,
g_state);--SECTION154PAYMENTVALUE););

end if;


   if r.MO_TARIFF_TYPE_UW_TariffCode is not null then
l_progress := 'processing MO_TARIFF_TYPE_UW';

insert into MO_TARIFF_TYPE_UW (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
UWFIXEDCHARGE,
UWRVPOUNDAGE,
UWRVTHRESHOLD,
UWRVMAXCHARGE,
UWRVMINCHARGE,
UWMISCTYPEACHARGE,
UWMISCTYPEBCHARGE,
UWMISCTYPECCHARGE,
UWMISCTYPEDCHARGE,
UWMISCTYPEECHARGE,
UWMISCTYPEFCHARGE,
UWMISCTYPEGCHARGE,
UWMISCTYPEHCHARGE
)
values
(
UW_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
TARIFF_VERSION_PK_SEQ.currval,
r.MO_TARIFF_TYPE_UW_UWFIXEDCHA,
r.MO_TARIFF_TYPE_UW_UWRVPOUND,
r.MO_TARIFF_UW_UWRVTHRESHOLD,
r.MO_TARIFF_UW_UWRVMAXIMUMCHA,
r.MO_TARIFF_UW_UWRVMinimumCha,
r.MO_TARIFF_UW_UWMISCTYPEACHA,
r.MO_TARIFF_UW_UWMISCTYPEBCHA ,
r.MO_TARIFF_UW_UWMISCTYPECCHA,
r.MO_TARIFF_UW_UWMISCTYPEDCHA,
r.MO_TARIFF_UW_UWMISCTYPEECHA,
r.MO_TARIFF_UW_UWMISCTYPEFCHA,
r.MO_TARIFF_UW_UWMISCTYPEGCHA,
r.MO_TARIFF_UW_UWMISCTYPEHCHA
  );
end if;


 if r.MO_METER_TariffCode is not null then
 l_progress := 'processing MO_UW_METER_UWPFC';
insert into MO_UW_METER_UWPFC (
TARIFF_UWPFC_PK,
TARIFF_TYPE_PK,
LOWERMETERSIZE,
UPPERMETERSIZE,
CHARGE
)
values
(
UW_TARIFF_UWPFC_PK_SEQ.nextval,
UW_TARIFF_TYPE_PK_SEQ.currval,
r.MO_METER_LowerMeterSize,
r.MO_METER_UpperMeterSize,
r.MO_METER_Charge
);
end if;

   EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
            -- g_err_rows := g_err_rows+1;
             l_error_number := SQLCODE;
             l_error_message := sqlerrm;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2396, l_tariff_count,    'Distinct UW tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2406, l_tariff_dropped,    'Distinct UW tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2416, l_db_rows_inserted,  'Distinct UW tariffs written to MO_TARIFFs during Transform');

                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;

l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2396, l_tariff_count,    'Distinct UW tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2406, l_tariff_dropped,    'Distinct UW tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2416, l_db_rows_inserted,  'Distinct UW tariffs written to MO_TARIFFs during Transform');
IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

Commit;

EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_UW;

procedure P_MOU_TRAN_TARIFF_HD   (   no_batch   IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Tariff Transform MO Extract for HD
--
-- AUTHOR         : Sreedhar Pallati
--
-- FILENAME       :
--
-- CREATED        : 25/02/2016
--
-- DESCRIPTION    : Procedure to create the Tariff MO Extract
-- NOTES  :
--
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      25/02/2016          S.Pallati        Initial Draft
-- V 0.02      11/03/2016          S.Pallati        Issue 28 fixed -- upper case conversion of tariff status
-- V 0.03      28-April-2016      S Pallati      Fix for issue I-202. Removing spaces from MO_TARIFF and MO_TARIFF_VERSION tables 
--                                               for tariff code and tariff names
-- v 0.04      29/04/2016          L. Smith         Control Point CP44 mapped to new measures 
----------------------------------------------------------------------------------------------

l_tariff_ver number;
l_serv_comp varchar2(5);
xmlClob_line clob;
xmlClob_full_file clob;
xmlFile UTL_FILE.FILE_TYPE;
x XMLType;
l_err_rows number := 0;
l_tariff_code varchar2(100);
l_tariff_count number :=0;
l_tariff_dropped number :=0;
l_db_count_before_insert number :=0;
l_db_count_after_insert number :=0;
l_db_rows_inserted  number :=0;
c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TARIFF_HD';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
BEGIN


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

   l_progress := 'processing HD xml file';

    IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

 select count(TARIFFCODE_PK) into l_db_count_before_insert from MO_TARIFF;


l_serv_comp := 'HD';
-- xml file reading and storing the data into xmlClob_full_file
xmlFile := UTL_FILE.FOPEN ('FILES', 'TARIFF_HD_XML.xml', 'R');
LOOP
BEGIN
UTL_FILE.GET_LINE(xmlFile,xmlClob_line,NULL);
xmlClob_full_file := xmlClob_full_file||xmlClob_line;
EXCEPTION WHEN No_Data_Found THEN EXIT; END;
END LOOP;

UTL_FILE.FCLOSE(xmlFile);

x := XMLType.createXML(xmlClob_full_file);
  FOR r IN (
    SELECT ExtractValue(Value(p),'/Row/TariffCode/text()') as TariffCode
          ,ExtractValue(Value(p),'/Row/TariffName/text()') as TariffName
          ,ExtractValue(Value(p),'/Row/TARIFFEFFECTIVEFROMDATE/text()') as TARIFFEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TariffStatus/text()') as TariffStatus
          ,ExtractValue(Value(p),'/Row/TARIFFLEGACYEFFECTIVEFROMDATE/text()') as TARIFFLEGACYEFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/ServiceComponent/text()') as ServiceComponent
          ,ExtractValue(Value(p),'/Row/Tariffauthorisationcode/text()') as Tariffauthorisationcode
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGIGMETHODWATER/text()') as VACANCYCHARGIGMETHODWATER
          ,ExtractValue(Value(p),'/Row/VACANCYCHARGINGMETHODSEWERAGE/text()') as VACANCYCHARGINGMETHODSEWERAGE
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODWAT/text()') as TEMPDISCONCHARGINGMETHODWAT
          ,ExtractValue(Value(p),'/Row/TEMPDISCONCHARGINGMETHODSEW/text()') as TEMPDISCONCHARGINGMETHODSEW

          ,ExtractValue(Value(p),'/Row/TARIFF_Version/Default_Return_to_Sewer/text()') as Default_Return_to_Sewer
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffCode/text()') as tar_ver_tariffcode
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TARIFFVEREFFECTIVEFROMDATE/text()') as TARIFFVEREFFECTIVEFROMDATE
          ,ExtractValue(Value(p),'/Row/TARIFF_Version/TariffStatus/text()') as tarver_TariffStatus

          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/TariffCode/text()') as MO_TARIFF_TYPE_HD_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/HDCOMBAND/text()') as MO_TARIFF_TYPE_HD_HDCOMBAND
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/HDFIXEDCHARGE/text()') as MO_TARIFF_TYPE_HD_HDFIXEDCHARG
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/HDRVPOUNDAGE/text()') as MO_TARIFF_TYPE_HD_HDRVPOUNDAGE
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/HDRVTHRESHOLD/text()') as MO_TARIFF_TYPE_HD_HDRVTHRESH
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/HDRVMAXCHARGE/text()') as MO_TARIFF_TYPE_HD_HDRVMAXCHA
          ,ExtractValue(Value(p),'/Row/MO_TARIFF_TYPE_HD/HDRVMINCHARGE/text()') as MO_TARIFF_TYPE_HD_HDRVMINCHA



          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/TariffCode/text()') as MO_AREA_TariffCode
          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/LowerArea/text()') as MO_AREA_LowerArea
          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/UpperArea/text()') as MO_AREA_UpperArea
          ,ExtractValue(Value(p),'/Row/MO_AREA_BAND/Band/text()') as MO_AREA_Band

            ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/TariffCode/text()') as MO_BAND_CH_TariffCode
             ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Band/text()') as MO_BAND_CH_Band
            ,ExtractValue(Value(p),'/Row/MO_BAND_CHARGE/Charge/text()') as MO_BAND_CH_Charge

         ,ExtractValue(Value(p),'/Row/MO_BLOCK/TariffCode/text()') as MO_BLOCK_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_BLOCK/UpperAnnualVol/text()') as MO_BLOCK_UpperAnnualVol
            ,ExtractValue(Value(p),'/Row/MO_BLOCK/Charge/text()') as MO_BLOCK_Charge

         ,ExtractValue(Value(p),'/Row/MO_METER/TariffCode/text()') as MO_METER_tariff_code
          ,ExtractValue(Value(p),'/Row/MO_METER/LowerMeterSize/text()') as MO_METER_LowerMeterSize
            ,ExtractValue(Value(p),'/Row/MO_METER/UpperMeterSize/text()') as MO_METER_UpperMeterSize
            ,ExtractValue(Value(p),'/Row/MO_METER/Charge/text()') as MO_METER_Charge

    FROM   TABLE(XMLSequence(Extract(x,'/TARIFF_HD/Row'))) p
    ) LOOP

l_progress := 'processing HD xml parse';

l_tariff_code := 'Tariff code '||r.TariffCode||r.tar_ver_tariffcode;

BEGIN

if r.TariffCode is not null then
l_progress := 'processing MO_TARIFF';
l_tariff_count := l_tariff_count+1;
insert into MO_TARIFF (TARIFFCODE_PK,
TARIFFEFFECTIVEFROMDATE,
TARIFFSTATUS,
TARIFFLEGACYEFFECTIVEFROMDATE,
APPLICABLESERVICECOMPONENT,
TARIFFAUTHCODE,
--VACANCYCHARGINGMETHODWATER,/* DB patch 15 fix*/
--VACANCYCHARGINGMETHODSEWERAGE,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODWAT,/* DB patch 15 fix*/
--TEMPDISCONCHARGINGMETHODSEW,/* DB patch 15 fix*/
SERVICECOMPONENTTYPE,
TARIFFNAME)
VALUES
(trim(r.TariffCode),
to_date(r.TARIFFEFFECTIVEFROMDATE,'DD/MM/YYYY'),
upper(r.TariffStatus), -- issue 28 fix
to_date(r.TARIFFLEGACYEFFECTIVEFROMDATE,'DD/MM/YYYY'),
l_serv_comp,
r.Tariffauthorisationcode,
--r.VACANCYCHARGIGMETHODWATER,
--r.VACANCYCHARGINGMETHODSEWERAGE,
--r.TEMPDISCONCHARGINGMETHODWAT,
--r.TEMPDISCONCHARGINGMETHODSEW ,
L_SERV_COMP,
trim(r.TariffName));
 end if;


    if r.tar_ver_tariffcode is not null then
    l_progress := 'processing MO_TARIFF_VERSION';
select nvl(max(TARIFFVERSION),0) into l_tariff_ver from MO_TARIFF_VERSION where tariffcode_pk=r.tar_ver_tariffcode;

insert into MO_TARIFF_VERSION  (
TARIFF_VERSION_PK,
TARIFFCODE_PK,
TARIFFVERSION,
TARIFFVEREFFECTIVEFROMDATE,
TARIFFSTATUS,
APPLICABLESERVICECOMPONENT,
--DEFAULTRETURNTOSEWER,/* DB patch 15 fix*/
TARIFFCOMPONENTTYPE,
SECTION154PAYMENTVALUE,
STATE/* DB patch 15 fix*/)
values
(TARIFF_VERSION_PK_SEQ.NEXTVAL,--TARIFF_VERSION_PK
trim(r.tar_ver_tariffcode),--TARIFFCODE_PK
l_tariff_ver+1,--TARIFFVERSION
to_date(r.TARIFFVEREFFECTIVEFROMDATE,'DD/MM/YYYY'),--TARIFFVEREFFECTIVEFROMDATE
upper(r.tarver_TariffStatus),--TARIFFSTATUS-- issue 28 fix
L_SERV_COMP,--APPLICABLESERVICECOMPONENT
--nvl(r.Default_Return_to_Sewer,100),--DEFAULTRETURNTOSEWER
l_serv_comp,--TARIFFCOMPONENTTYPE
NULL,
g_state);--SECTION154PAYMENTVALUE););

end if;

   if r.MO_TARIFF_TYPE_HD_TariffCode is not null then
l_progress := 'processing MO_TARIFF_TYPE_HD';

insert into MO_TARIFF_TYPE_HD (
TARIFF_TYPE_PK,
TARIFF_VERSION_PK,
HDCOMBAND,
HDFIXEDCHARGE,
HDRVPOUNDAGE,
HDRVTHRESHOLD,
HDRVMAXCHARGE,
HDRVMINCHARGE
)
values
(
HD_TARIFF_TYPE_PK_SEQ.nextval,--TARIFF_TYPE_PK
TARIFF_VERSION_PK_SEQ.currval,
r.MO_TARIFF_TYPE_HD_HDCOMBAND ,
r.MO_TARIFF_TYPE_HD_HDFIXEDCHARG,
r.MO_TARIFF_TYPE_HD_HDRVPOUNDAGE,
r.MO_TARIFF_TYPE_HD_HDRVTHRESH,
r.MO_TARIFF_TYPE_HD_HDRVMAXCHA ,
r.MO_TARIFF_TYPE_HD_HDRVMINCHA
  );
end if;



if r.MO_AREA_TariffCode is not null then
 l_progress := 'processing MO_HD_AREA_BAND';
insert into MO_HD_AREA_BAND (
TARIFF_AREA_BAND_PK,
TARIFF_TYPE_PK,
LOWERAREA,
UPPERAREA,
BAND
)
values
(
HD_TARIFF_AREA_BAND_PK_SEQ.nextval,
HD_TARIFF_TYPE_PK_SEQ.currval,
r.MO_AREA_LowerArea,
r.MO_AREA_UpperArea,
r.MO_AREA_Band
);
end if;


if r.MO_BAND_CH_TariffCode is not null then
 l_progress := 'processing MO_HD_BAND_CHARGE';
insert into MO_HD_BAND_CHARGE (
TARIFF_BAND_CHARGE_PK,
TARIFF_TYPE_PK,
BAND,
CHARGE
)
values
(
HD_TARIFF_BAND_CHARGE_PK_SEQ.nextval,
HD_TARIFF_TYPE_PK_SEQ.currval,
r.MO_BAND_CH_Band,
r.MO_BAND_CH_Charge
);
end if;

 if r.MO_BLOCK_tariff_code is not null then
 l_progress := 'processing MO_HD_BLOCK_HDBT';
insert into MO_HD_BLOCK_HDBT (
TARIFF_HDBT_PK,
TARIFF_TYPE_PK,
UPPERANNUALVOL,
CHARGE
)
values
(
HD_TARIFF_HDBT_PK_SEQ.nextval,
HD_TARIFF_TYPE_PK_SEQ.currval,
r.MO_BLOCK_UpperAnnualVol,
r.MO_BLOCK_Charge
);

end if;

 if r.MO_METER_tariff_code is not null then
 l_progress := 'processing MO_HD_METER_HDMFC';
insert into MO_HD_METER_HDMFC (
TARIFF_HDMFC_PK,
TARIFF_TYPE_PK,
LOWERMETERSIZE,
UPPERMETERSIZE,
CHARGE
)
values
(
HD_TARIFF_HDMFC_PK_SEQ.nextval,
HD_TARIFF_TYPE_PK_SEQ.currval,
r.MO_METER_LowerMeterSize,
r.MO_METER_UpperMeterSize,
r.MO_METER_Charge
);
end if;

    EXCEPTION
        WHEN OTHERS THEN
             IF r.TariffCode is not null then
             l_tariff_dropped := l_tariff_dropped + 1;
             l_err.TXT_KEY := r.TariffCode;
             g_err_rows := g_err_rows+1;
             END IF;
             --g_err_rows := g_err_rows+1;
             l_error_number := sqlcode;
             l_error_message := SQLERRM;
            if (   l_no_row_exp > l_job.exp_tolerance
                 or g_err_rows >= g_err_tol
                 or l_no_row_war > l_job.war_tolerance)
             THEN
                  l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2398, l_tariff_count,    'Distinct HD tariffs read during Transform');
                 P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2408, l_tariff_dropped,    'Distinct HD tariffs dropped during Transform');
                  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2418, l_db_rows_inserted,  'Distinct HD tariffs written to MO_TARIFFs during Transform');
                 COMMIT;
                 return_code := -1;
                 RETURN;
                ELSE
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              END IF;
       END;

  END LOOP;

l_progress := 'Writing Counts';
select count(TARIFFCODE_PK) into l_db_count_after_insert from MO_TARIFF;
l_db_rows_inserted := l_db_count_after_insert - l_db_count_before_insert;


  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2398, l_tariff_count,    'Distinct HD tariffs read during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2408, l_tariff_dropped,    'Distinct HD tariffs dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP44', 2418, l_db_rows_inserted,  'Distinct HD tariffs written to MO_TARIFFs during Transform');

  IF l_tariff_count <> l_db_rows_inserted+l_tariff_dropped THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_tariff_count || ',' || l_db_rows_inserted, l_err.TXT_DATA || ',' || l_progress);
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
      IF (l_db_rows_inserted =0) THEN
     l_job.IND_STATUS := 'ERR';
     END IF;
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

  Commit;


EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_tariff_code,  l_err.TXT_DATA  || ',' || l_progress);
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Job Ended - Unexpected Error', l_tariff_code, l_err.TXT_DATA || ',' || l_progress);
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
Commit;
  END P_MOU_TRAN_TARIFF_HD;

END P_MIG_TARIFF;
/

exit;
