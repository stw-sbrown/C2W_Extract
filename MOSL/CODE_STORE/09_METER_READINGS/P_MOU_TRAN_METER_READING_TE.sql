create or replace
PROCEDURE           P_MOU_TRAN_METER_READING_TE (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter Reading Transform MO Extract for TE
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_READING_TE.sql
--
-- Subversion $Revision: 6171 $
--
-- CREATED        : 25/05/2016
--
-- DESCRIPTION    : Procedure to create the Meter Reading Extract for TE Meters
--                 Will read from key gen and target tables, apply any transformation
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         CR/DEF    Description
-- ---------   ---------------     -------        ----      ----------------------------------
-- v 2.08      09/11/2016          D.Cheung                 Change readings to pick up from TE_READ_TOLERANCE view instead
-- v 2.07      02/11/2016          D.Cheung                 Change for Meter CDV calculations - exclude ESTIMATE reads
-- V 2.06      18/07/2016          D.Cheung                 I-300 - Fix rollover issue due to dropped readings
-- V 2.05      15/07/2016          D.Cheung                 I-300 - MOSL Test2.2 rejection - need to check for duplicate readdate (using date part string)
-- V 2.04      14/07/2016          D.Cheung                 I-295 - Get FIRST available read as INSTALL and INITIAL read
-- V 2.03      13/07/2016          D.Cheung                 I-294 - Dropped readings due to 'No Initial Meter Read Date Found'
--                                                          I-295 - Change of logic for INITIALMETERREADDATE required to meet MOSL rules
-- V 2.02      04/07/2016          D.Cheung                 I-270 - MeterRead greater than initial meter read issue
--                                                          Meter Read cannot be a future date
-- V 2.01      21/06/2016          D.Cheung                 I-242 - Invalid NULL Initial Meter Read Date
-- V 1.01      14/06/2016          D.Cheung       D_52      I-234 - set Initial Read Dates to Supply Point Effective From Date if greater
--                                                          Drop any readings before Supply Point effective from Date
-- V 0.01      25/05/2016          D.Cheung                 Initial Draft
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_READING_TE';  -- modify
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    MO_METER.METERREF%TYPE; --modify
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  L_ERR                         MIG_ERRORLOG%ROWTYPE;
  L_MO                          MO_METER_READING%ROWTYPE; --modify
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
  l_initialmeterreaddate        MO_METER_READING.INITIALMETERREADDATE%TYPE; --v2.03
  l_prev_readdate               VARCHAR2(15);

CURSOR cur_met (P_NO_EQUIPMENT_START   MO_METER.METERREF%type,
                 P_NO_EQUIPMENT_END     MO_METER.METERREF%type)
    IS
      SELECT DISTINCT * FROM (
--          SELECT /*+ PARALLEL(MO,12) PARALLEL(BTW,12) */
--          DISTINCT
--              TRIM(MO.MANUFACTURER_PK) AS NM_PREFERRED
--              , TRIM(MO.MANUFACTURERSERIALNUM_PK) AS NO_UTL_EQUIP
--              , MO.METERREF AS NO_EQUIPMENT
--              , NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) AS METERREADDATE
--              , 'P' AS METERREADTYPE
--              , 0 AS REREAD  -- **** ISSUE - CONVERT RULE REQUIRED
--              , NVL(BTW.END_READ,BTW.START_READ) AS METERREAD
--              , 'VISUAL' AS METERREADMETHOD
--              , MO.INSTALLEDPROPERTYNUMBER
--              , MO.MANUFCODE
--              , NULL AS ESTIMATEDREADREASONCODE
--              , NULL AS ESTIMATEDREADREMEDIALWORKIND
--              , NVL(MSP.SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')) SUPPLYPOINTEFFECTIVEFROMDATE  --*** V9.01
--              , 0  AS Record_Nr
--          FROM MO_METER MO
--          LEFT JOIN MO_SUPPLY_POINT MSP ON (MSP.STWPROPERTYNUMBER_PK = MO.INSTALLEDPROPERTYNUMBER AND MSP.SPID_PK = MO.SPID_PK)    --*** V9.01
--          JOIN BT_TE_WORKING BTW ON (
--              BTW.NO_IWCS || BTW.MET_REF = MO.METERREF
--              AND MONTHS_BETWEEN(SYSDATE, NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy')))) <= 24    --v2.03
--              AND NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) <= SYSDATE
--              AND NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) >= MSP.SUPPLYPOINTEFFECTIVEFROMDATE --*** V9.01
--              AND UPPER(BTW.CODE) IN ('B','T','V')
--          )
--          WHERE MO.METERTREATMENT IN ('PRIVATETE', 'PRIVATEWATER')
          SELECT /*+ PARALLEL(MO,12) PARALLEL(BTW,12) */
          DISTINCT
              TRIM(MO.MANUFACTURER_PK) AS NM_PREFERRED
              , TRIM(MO.MANUFACTURERSERIALNUM_PK) AS NO_UTL_EQUIP
              , MO.METERREF AS NO_EQUIPMENT
              , NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) AS METERREADDATE
              , 'P' AS METERREADTYPE
              , 0 AS REREAD  -- **** ISSUE - CONVERT RULE REQUIRED
              , NVL(BTW.END_READ,BTW.START_READ) AS METERREAD
              , 'VISUAL' AS METERREADMETHOD
              , MO.INSTALLEDPROPERTYNUMBER
              , MO.MANUFCODE
              , NULL AS ESTIMATEDREADREASONCODE
              , NULL AS ESTIMATEDREADREMEDIALWORKIND
              , NVL(MSP.SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')) SUPPLYPOINTEFFECTIVEFROMDATE  --*** V9.01
              , 0  AS Record_Nr
          FROM MO_METER MO
          LEFT JOIN MO_SUPPLY_POINT MSP ON (MSP.STWPROPERTYNUMBER_PK = MO.INSTALLEDPROPERTYNUMBER AND MSP.SPID_PK = MO.SPID_PK)    --*** V9.01
          JOIN TE_READ_TOLERANCE BTW ON (
              BTW.NO_IWCS || BTW.MET_REF = MO.METERREF
              AND MONTHS_BETWEEN(SYSDATE, NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy')))) <= 24    --v2.03
              AND NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) <= SYSDATE
              AND NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) >= MSP.SUPPLYPOINTEFFECTIVEFROMDATE --*** V9.01
              AND BTW.END_READ BETWEEN BTW.TOLERANCE_FROM AND BTW.TOLERANCE_TO
          )
          WHERE MO.METERTREATMENT IN ('PRIVATETE', 'PRIVATEWATER')
          UNION
          SELECT * FROM (
              SELECT /*+ PARALLEL(MO,12) PARALLEL(BTW,12) */
              DISTINCT
                  TRIM(MO.MANUFACTURER_PK) AS NM_PREFERRED
                  , TRIM(MO.MANUFACTURERSERIALNUM_PK) AS NO_UTL_EQUIP
                  , MO.METERREF AS NO_EQUIPMENT
                  , CASE WHEN BTW.PERIOD IS NULL THEN NULL ELSE NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) END AS METERREADDATE
                  , 'I' AS METERREADTYPE
                  , 0 AS REREAD  -- **** ISSUE - CONVERT RULE REQUIRED
                  , NVL(BTW.END_READ,BTW.START_READ) AS METERREAD
                  , 'VISUAL' AS METERREADMETHOD
                  , MO.INSTALLEDPROPERTYNUMBER
                  , MO.MANUFCODE
                  , NULL AS ESTIMATEDREADREASONCODE
                  , NULL AS ESTIMATEDREADREMEDIALWORKIND
                  , NVL(MSP.SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')) SUPPLYPOINTEFFECTIVEFROMDATE
--                , BTW.PERIOD
--                , BTW.STAGE
                  , ROW_NUMBER() OVER ( PARTITION BY MO.METERREF ORDER BY MO.METERREF, NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) ASC NULLS FIRST) AS Record_Nr
              FROM MO_METER MO
              LEFT JOIN MO_SUPPLY_POINT MSP ON (MSP.STWPROPERTYNUMBER_PK = MO.INSTALLEDPROPERTYNUMBER AND MSP.SPID_PK = MO.SPID_PK)
              LEFT JOIN BT_TE_WORKING BTW ON (
                  BTW.NO_IWCS || BTW.MET_REF = MO.METERREF
                  AND MONTHS_BETWEEN(SYSDATE, NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy')))) > 24
              )
              WHERE MO.METERTREATMENT IN ('PRIVATETE', 'PRIVATEWATER')
--            ORDER BY MO.METERREF ASC, NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) ASC;
          ) x
          WHERE Record_Nr = 1
      )
      ORDER BY NO_EQUIPMENT, METERREADDATE ASC NULLS FIRST;

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

   l_progress := 'processing ';

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
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

          L_ERR.TXT_KEY := t_met(I).NO_EQUIPMENT; -- modify
          L_MO := null;
          L_REC_EXC := false;
          L_REC_WAR := false;

          l_progress := 'GETTING REREADFLAG';
          --Mapping ReReadFlag
-- ***** ISSUE - NEED Mapping Rule
          L_MO.REREADFLAG := 0;

          l_progress := 'GETTING ROLLOVERINDICATOR';
          IF (l_prev_met = t_met(i).NO_EQUIPMENT AND t_met(i).METERREAD <= l_prev_am_reading) THEN
              l_mo.ROLLOVERINDICATOR := 1;
              l_mo.ROLLOVERFLAG := 1;
          ELSE
              l_mo.ROLLOVERINDICATOR := 0;
              l_mo.ROLLOVERFLAG := 0;
          END IF;

--v2.03
--          BEGIN
--              L_PROGRESS := 'GETTING INITIALMETERREADDATE';
--              SELECT LEAST(MIN(NVL(START_DATE, to_date('01/04/2016','dd/mm/yyyy'))),MIN(NVL(END_DATE, to_date('01/04/2016','dd/mm/yyyy')))) -- v2.02 v2.03
--                  INTO  L_MO.INITIALMETERREADDATE
--              FROM  BT_TE_WORKING
--              WHERE  NO_IWCS || MET_REF = t_met(i).NO_EQUIPMENT;
----***v9.01
--              IF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > L_MO.INITIALMETERREADDATE OR L_MO.INITIALMETERREADDATE < SYSDATE) THEN
--                  l_progress := 'Initial Meter Read Date set to SPID Date';
--                  L_MO.INITIALMETERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;  --v2.01
--                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Meter Read Date changed to SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
--                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
--                  L_REC_WAR := TRUE;
--              END IF;
--          END;
          
--          L_PROGRESS := 'CHECK FOR FUTURE DATE';
--          IF (t_met(i).METERREADDATE > SYSDATE) THEN
--              BEGIN
--                  SELECT LTBC_START
--                      INTO t_met(i).METERREADDATE
--                  FROM LU_TE_BILLING_CYCLE
--                  WHERE LTBC_PERIOD = t_met(i).PERIOD
--                      AND LTBC_CYCLE_NUMBER = t_met(i).STAGE;
--              EXCEPTION
--                   WHEN NO_DATA_FOUND THEN
--                      t_met(i).METERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;
--                      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - FUTURE Meter Read Date changed to SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
--                      L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
--                      L_REC_WAR := TRUE;
--              END;
--          END IF;
          
--          L_PROGRESS := 'CHECK FOR LATER SPID DATE';
--          IF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > t_met(i).METERREADDATE) THEN
--              t_met(i).METERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;
--              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Meter Read Date changed to LATER SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
--              L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
--              L_REC_WAR := TRUE;
--          END IF;
          
--v2.03
          --IF DIFFERENT METER FROM PREVIOUS, UPDATE METER READ TYPE AND INITIALMETERREAD
          IF (l_prev_met <> t_met(i).NO_EQUIPMENT) THEN
              l_no_meter_read := l_no_meter_read + 1;
              l_curr_meter_written := FALSE;
              
              IF (t_met(i).METERREADDATE IS NULL) THEN
                  l_progress := 'NO ACTUAL READING - DEFAULT IN VALUES';
--                  BEGIN
--                      SELECT METERREAD
--                      INTO t_met(i).METERREAD
--                      FROM
--                          (SELECT NVL(BTW.END_READ,NVL(BTW.START_READ,0)) METERREAD
--                          FROM  BT_TE_WORKING BTW 
--                          WHERE  BTW.NO_IWCS || BTW.MET_REF = t_met(i).NO_EQUIPMENT
--                          ORDER BY NVL(BTW.END_DATE,NVL(BTW.START_DATE, to_date('01/04/2016','dd/mm/yyyy'))) DESC
--                          )
--                      WHERE rownum = 1;
--                  EXCEPTION
--                      WHEN NO_DATA_FOUND THEN
                          t_met(i).METERREAD := 0;
                          t_met(i).METERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;
                          t_met(i).METERREADTYPE := 'I';
--                  END;
              ELSE              
                  --SET FIRST READ FOR METER AS INSTALL READ
                  L_PROGRESS := 'DEFAULTING INSTALL METER READ';
                  t_met(i).METERREADTYPE := 'I';
              END IF;
              IF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > t_met(i).METERREADDATE) THEN
                  t_met(i).METERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Initial Meter Read Date changed to LATER SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                  L_REC_WAR := TRUE;
              END IF;
              l_initialmeterreaddate :=  t_met(i).METERREADDATE;
          ELSE
              IF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > t_met(i).METERREADDATE) THEN
                  t_met(i).METERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Meter Read Date changed to SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                  L_REC_WAR := TRUE;
              END IF;
              
              --v2.05 - Check for duplicate METERREADDATES (DATE PART)
              IF (l_prev_readdate = TO_CHAR(t_met(i).METERREADDATE, 'DD-MON-YYYY')) THEN
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('DUPLICATE METERREADDATE FOR METER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                  L_REC_EXC := TRUE;
              END IF;
          END IF;
             
          L_PROGRESS := 'GETTING INITIALMETERREADDATE';
          L_MO.INITIALMETERREADDATE := l_initialmeterreaddate;
          
          IF  L_REC_EXC = TRUE THEN  --using this if statement to pick up data that passes the biz rules into the table otherwise they will be dropped and would have be sent into the exception table
              IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                     OR l_no_row_err > l_job.ERR_TOLERANCE
                     OR l_no_row_war > l_job.WAR_TOLERANCE
                     )
              THEN
                  CLOSE cur_met;
                  L_JOB.IND_STATUS := 'ERR';
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                  commit;
                  return_code := -1;
                  RETURN;
              END IF;
              L_REC_WRITTEN := FALSE;
          ELSE
              IF (L_REC_WAR = true AND l_no_row_war > l_job.WAR_TOLERANCE) THEN
                  CLOSE cur_met;
                  L_JOB.IND_STATUS := 'ERR';
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Warning tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                  commit;
                  return_code := -1;
                  return;
              END IF;
              L_REC_WRITTEN := TRUE;
              --DBMS_OUTPUT.PUT_LINE('INSERT');
              l_progress := 'loop processing ABOUT TO INSERT INTO TABLE';
              BEGIN
                  INSERT INTO MO_METER_READING
                  (MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, METERREF, METERREADDATE, METERREADTYPE, ROLLOVERINDICATOR, REREADFLAG
                  , METERREAD, METERREADMETHOD, ESTIMATEDREADREASONCODE, ROLLOVERFLAG, ESTIMATEDREADREMEDIALWORKIND, INITIALMETERREADDATE
                  , METRREADDATEFORREMOVAL, RDAOUTCOME, METERREADSTATUS, METERREADERASEDFLAG, METERREADREASONTYPE, METERREADSETTLEMENTFLAG, PREVVALCDVCANDIDATEDAILYVOLUME
                  , INSTALLEDPROPERTYNUMBER, MANUFCODE
                  )
              VALUES
                  (t_met(i).NM_PREFERRED, t_met(i).NO_UTL_EQUIP, t_met(i).NO_EQUIPMENT, t_met(i).METERREADDATE, t_met(i).METERREADTYPE, l_mo.ROLLOVERINDICATOR, L_MO.REREADFLAG
                  , t_met(i).METERREAD, t_met(i).METERREADMETHOD, t_met(i).ESTIMATEDREADREASONCODE, l_mo.ROLLOVERFLAG, t_met(i).ESTIMATEDREADREMEDIALWORKIND, L_MO.INITIALMETERREADDATE
                  , NULL, NULL, NULL, NULL, NULL, NULL, NULL
                  , t_met(i).INSTALLEDPROPERTYNUMBER, t_met(i).MANUFCODE
                  );
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
                     OR l_no_row_war > l_job.WAR_TOLERANCE
                     )
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
          END IF;  --close of if  L_REC_EXC statement

          IF l_rec_written THEN
              l_no_row_insert := l_no_row_insert + 1;
-- CR01 - add recon counts for METERS
              l_curr_meter_written := TRUE;
              IF (l_prev_met <> t_met(i).NO_EQUIPMENT) THEN
                  l_no_meter_written := l_no_meter_written + 1;
              END IF;
              l_prev_am_reading := t_met(i).METERREAD;
              l_prev_readdate := TO_CHAR(t_met(i).METERREADDATE, 'DD-MON-YYYY');
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
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP35', 1180, L_NO_ROW_READ,    'Distinct TE Meter Readings read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP35', 1181, l_no_meter_read,  'Distinct TE Meters read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP35', 1190, L_NO_ROW_DROPPED, 'TE Meter Readings dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP35', 1191, l_no_meter_dropped,  'Distinct TE Meters dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP35', 1200, l_no_row_insert,  'TE Meter Readings written to MO_METER_READING during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP35', 1201, l_no_meter_written,  'Distinct TE Meters written during Transform');

  --  check counts match
  IF l_no_row_read <> l_no_row_insert + L_NO_ROW_DROPPED THEN
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_no_row_read || ',' || l_no_row_insert, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
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
WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE(L_MO.METERREAD);
      L_ERROR_NUMBER := SQLCODE;
      L_ERROR_MESSAGE := SUBSTR(SQLERRM,1,512);
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY,  SUBSTR(L_ERR.TXT_DATA || ',' || l_progress,1,100));
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      commit;
      RETURN_CODE := -1;
END P_MOU_TRAN_METER_READING_TE;
/
/
show errors;
exit;
