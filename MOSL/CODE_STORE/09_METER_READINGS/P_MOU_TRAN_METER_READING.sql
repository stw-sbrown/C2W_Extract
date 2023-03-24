create or replace
PROCEDURE           P_MOU_TRAN_METER_READING (no_batch     IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter Reading Transform MO Extract
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_READING.sql
--
-- Subversion $Revision: 6139 $
--
-- CREATED        : 07/04/2016
--
-- DESCRIPTION    : Procedure to create the Meter Reading Extract
--                 Will read from key gen and target tables, apply any transformation
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         CR/DEF    Description
-- ---------   ---------------     -------        ----      ----------------------------------
-- v 9.14      07/11/2016          L.Smith                  Performance changes.
-- v 9.13      07/11/2016          S.Badhan                 Performance changes.
-- v 9.12      01/11/2016          D.Cheung                 I-352 - For Aggregates - set property as master
-- V 9.11      26/09/2016          D.Cheung       CR_037    Exclude Non-billable reads
-- V 9.10      21/09/2016          D.Cheung                 Performance changes - remove redundant joins from main cursor
-- V 9.09      12/09/2016          D.Cheung                 I-355 - Interim workaround for future dates issue - set to current date
-- V 9.08      02/08/2016          D.Cheung                 I-325 - MOSL rejections - initial meter read date before spid effective date
-- V 9.07      26/07/2016          D.Cheung                 I-317 - MOSL rejections - include non-market meters
-- V 9.06      18/07/2016          D.Cheung                 I.306 - Fix BUG causing NULL meter Reads
--                                                          I-300 - Fix BUG with rollover alignment due to dropped readings
-- V 9.05      15/07/2016          D.Cheung                 I-300 - MOSL Test2.2 rejection - need to check for duplicate readdate (using date part string)
-- V 9.04      14/07/2016          D.Cheung                 I-295 - Get FIRST available read as INSTALL and INITIAL read
-- V 9.03      13/07/2016          D.Cheung                 I-295 - Change of logic for INITIALMETERREADDATE required to meet MOSL rules
-- V 9.02      21/06/2016          D.Cheung                 I-245 - MOSL change in guidance v1.7 remove restriction for flat readings
-- V 9.01      14/06/2016          D.Cheung       D_52      I-234 - set Initial Read Dates to Supply Point Effective From Date if greater
--                                                          Drop any readings before Supply Point effective from Date
-- V 8.03      31/05/2016          D.Cheung       D_56      MOSL TEST1 Defect mr2 - Change rule to drop reading if SAME as previous
-- V 8.02      27/05/2016          D.Cheung       CR_017    New busines rules for eliminating data Exceptions
--                                                          2.	Meter Read Method Translation Table - Map Target Value of O (Service Order) to VISUAL.
-- V 8.01      26/05/2016          D.Cheung       D_56      MOSL TEST1 Defect mr2 - ROLLOVERFLAG not set - change to same as ROLLOVERINDICATOR
--                                                D_55      MOSL TEST1 Defect mr1 - Meter Read Date cannot be before 01-10-2014
--                                                D_51      MOSL TEST1 Defect m1 - Remove spaces from ManufacturerSerialNum
--                                                D_52      MOSL TEST1 Defect m2 - initial meter read date cannot be before SPID effective date
-- V 7.03      24/05/2016          D.Cheung                 Change for TE - filter extract on POTABLE MeterTreatment
-- V 7.02      23/05/2016          D.Cheung                 Add INSTALLEDPROPERTYNUMBER and MANUFCODE fields
-- V 7.01      20/05/2016          D.Cheung       CR_015    MOSL requirement change - If Meter Read Type is I then Meter Read Method must be populated with the value VISUAL
--                                                          MOSL requirement change - First read must be of type I
-- V 6.01      12/05/2016          D.Cheung       CR_013    D40 - Added ESTIMATE Read Sources to Extract Criteria   
-- V 5.01      11/05/2016          D.Cheung                 Issue I-220 - WORKAROUND for No Billiable reads
--                                                           - get earliest available read as an Install-System Estimate (INFO-ONLY) read
-- V 4.01      10/04/2016          D.Cheung                 Issue I-173 - Rollover rule misalignment - Add Read Source filters
-- V 3.01      29/04/2016          D.Cheung                 Issue I-173 - Rollover rule misalignment - change to get non-billable readings as well
--                                                D_22      FINAL ruling - set ALL initial meter reading date to earliest date
-- V 2.03      28/04/2016          D.Cheung                 Issue I-116 - Final rule for ESTIMATEDREADREASONCODE
-- V 2.02      27/04/2016          D.Cheung       D_22      Fix to Get INSTALL Meter Reading Date
-- V 2.01      25/04/2016          D.Cheung                 Performance Tweaks (removed unnecessary plsql trims - use SQL trim)
-- V 1.03      22/04/2016          D.Cheung                 Issue I-172 - New Convert Rule for ESTIMATEDREADREMEDIALWORKIND
-- V 1.02      20/04/2016          D.Cheung                 Correct bugs on Meter Recon counts
--                                                          Correct bugs on RECON codes
-- V 1.01      19/04/2016          D.Cheung       CR01      Change Cursor to join to MO_METER (Only get written Meters)
--                                                          Change cursor to only get ACTUAL reads
--                                                          Add recon counts for METERS read and written
-- V 0.02      18/04/2016          D.Cheung                 TRIM at source on cursor - fix rogue exceptions
-- V 0.01      07/04/2016          D.Cheung                 Initial Draft
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_READING';  -- modify
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_met                    CIS.TVP043METERREG.NO_EQUIPMENT%TYPE; --modify
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
  l_initialmeterreaddate        MO_METER_READING.INITIALMETERREADDATE%TYPE;
  l_prev_readdate               VARCHAR2(15);

CURSOR cur_met (P_NO_EQUIPMENT_START   BT_TVP163.NO_EQUIPMENT%type,
                 P_NO_EQUIPMENT_END     BT_TVP163.NO_EQUIPMENT%type)
    IS
-- CR01 - CHANGE CURSOR TO ONLY GET METERS WRITTEN TO MO_METER
      SELECT DISTINCT * FROM (
          SELECT /*+ FULL(T195) PARALLEL(MO 6) PARALLEL(T195 6) PARALLEL(MSP 6) PARALLEL(T1 6) */ 
                TRIM(MO.MANUFACTURER_PK) NM_PREFERRED   -- V8.01
                , TRIM(MO.MANUFACTURERSERIALNUM_PK) NO_UTL_EQUIP
                , MO.METERREF NO_EQUIPMENT
                , T195.TS_CAPTURED
                , TRIM(T195.TP_READING) TP_READING
                , NVL(T1.ROLLOVERINDICATOR,0) ROLLOVERINDICATOR
                , 0 REREAD  -- **** ISSUE - CONVERT RULE REQUIRED
                , TRIM(T195.AM_READING) AM_READING
                , TRIM(T195.CD_MTR_RD_SRCE_98) CD_MTR_RD_SRCE_98
                , NVL(T1.ROLLOVERINDICATOR,0) AS ROLLOVERFLAG     --V8.01
                , NVL(MO.MASTER_PROPERTY, MO.INSTALLEDPROPERTYNUMBER) INSTALLEDPROPERTYNUMBER    --*** v7.02 v9.12
                , MO.MANUFCODE                  --*** v7.02
                , MO.SPID_PK                    --V8.01
                , NVL(MSP.SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')) SUPPLYPOINTEFFECTIVEFROMDATE  --*** V9.01
                , 0 AS Record_Nr
          FROM MO_METER MO
              LEFT JOIN MO_SUPPLY_POINT MSP ON (MSP.SPID_PK = MO.SPID_PK)    --*** V9.01, V9.08
          JOIN CIS.TVP195READING T195 ON
              T195.NO_EQUIPMENT = MO.METERREF
--                  AND T195.CD_COMPANY_SYSTEM = 'STW1'
                  AND T195.TP_READING = 'M'
                  AND T195.CD_MTR_RD_SRCE_98 IN ('N','W','L','P','C','U','H','O','F','G','S','X')   --*** V4.01 and V6.01
                  AND T195.ST_READING_168 IN ('B')  -- V 9.11
                  AND MONTHS_BETWEEN(SYSDATE, T195.TS_CAPTURED) <= 24
                  AND T195.TS_CAPTURED >= MSP.SUPPLYPOINTEFFECTIVEFROMDATE --*** V9.01
                  --AND T195.AM_READING > 0   --v9.11
          --JOIN CIS.TVP063EQUIPMENT T063 ON (T063.NO_EQUIPMENT = MO.METERREF AND TRIM(T063.CD_COMPANY_SYSTEM) = 'STW1')    --V9.10
          --JOIN CIS.TVP036LEGALENTITY T036 ON (T036.NO_LEGAL_ENTITY = T063.NO_BUSINESS)                                    --V9.10
          LEFT JOIN BT_CLOCKOVER T1 ON (T1.STWMETERREF_PK = MO.METERREF AND T1.METERREADDATE = T195.TS_CAPTURED)
          WHERE  MO.METERREF BETWEEN p_no_equipment_start AND p_no_equipment_end
--         WHERE  MO.METERREF BETWEEN 1 AND 999999999
--              AND MO.NONMARKETMETERFLAG = 0   --v9.07
              AND MO.METERTREATMENT = 'POTABLE'  --V7.03
          UNION
          SELECT * FROM (
              SELECT /*+ use_hash(mo t195) parallel(mo 6) parallel(t195 6)  */
                  TRIM(MO.MANUFACTURER_PK) NM_PREFERRED
                  , TRIM(MO.MANUFACTURERSERIALNUM_PK) NO_UTL_EQUIP
                  , MO.METERREF NO_EQUIPMENT
                  , T195.TS_CAPTURED
                  , 'I' TP_READING
                  , 0 ROLLOVERINDICATOR
                  , 0 REREAD
                  , TRIM(T195.AM_READING) AM_READING
                  , 'H' CD_MTR_RD_SRCE_98
                  , 0 AS ROLLOVERFLAG
                  , NVL(MO.MASTER_PROPERTY, MO.INSTALLEDPROPERTYNUMBER) INSTALLEDPROPERTYNUMBER    --v9.12
                  , MO.MANUFCODE              
                  , MO.SPID_PK                
                  , NVL(MSP.SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')) SUPPLYPOINTEFFECTIVEFROMDATE
                  , ROW_NUMBER() OVER ( PARTITION BY MO.METERREF ORDER BY MO.METERREF, T195.TS_CAPTURED ASC NULLS FIRST) AS Record_Nr
              FROM MO_METER MO
                  LEFT JOIN MO_SUPPLY_POINT MSP ON (MSP.SPID_PK = MO.SPID_PK) --V9.08
                  LEFT JOIN CIS.TVP195READING T195 ON (T195.NO_EQUIPMENT = MO.METERREF AND MONTHS_BETWEEN(SYSDATE, T195.TS_CAPTURED) > 24)
              WHERE  MO.METERREF BETWEEN p_no_equipment_start AND p_no_equipment_end
--              WHERE  MO.METERREF BETWEEN 1 AND 999999999
--                  AND MO.NONMARKETMETERFLAG = 0   --v9.07
                  AND MO.METERTREATMENT = 'POTABLE'
          ) x
          WHERE Record_Nr = 1
      )
      ORDER BY NO_EQUIPMENT, TS_CAPTURED ASC NULLS FIRST;
      

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
-- CR01 - add recon counts for METERS
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

          l_progress := 'CHECKING MANUFACTURER SERIALNUM IN METER TABLE';
          BEGIN
              SELECT COUNT(METERREF)
              INTO   l_manufserialchk
              FROM   MO_METER
              WHERE  MANUFACTURER_PK = T_MET(I).NM_PREFERRED
                  AND    MANUFACTURERSERIALNUM_PK   = T_MET(I).NO_UTL_EQUIP;
          EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  l_manufserialchk := 0;
          END;

-- CR01 - add recon counts for METERS
          --IF DIFFERENT METER FROM PREVIOUS, UPDATE METER READ COUNT
          IF (l_prev_met <> t_met(i).NO_EQUIPMENT) THEN
              l_no_meter_read := l_no_meter_read + 1;
              l_curr_meter_written := FALSE;
--*** V5.01 - WORKAROUND for NO ACTUAL readings              
              IF t_met(i).TS_CAPTURED IS NULL THEN
                  l_progress := 'NO READINGS FOR METER - DEFAULT IN VALUES';
--                  BEGIN
--                      L_PROGRESS := 'GETTING EARLIEST AVAILABLE READING';
--                      SELECT TS_CAPTURED
--                          , TP_READING
--                          , AM_READING
--                          , CD_MTR_RD_SRCE_98
--                      INTO t_met(i).TS_CAPTURED
--                          , t_met(i).TP_READING
--                          , t_met(i).AM_READING
--                          , t_met(i).CD_MTR_RD_SRCE_98
--                      FROM
--                          (SELECT TS_CAPTURED
--                              , 'I' AS TP_READING            
--                              , TRIM(AM_READING) AM_READING
--                              , 'H' AS CD_MTR_RD_SRCE_98      --v 7.01
--                          FROM  CIS.TVP195READING
--                          WHERE  NO_EQUIPMENT = t_met(i).NO_EQUIPMENT
--                          ORDER BY TS_CAPTURED DESC
--                          )
--                      WHERE rownum = 1;
--                  EXCEPTION
--                      WHEN NO_DATA_FOUND THEN
                          t_met(i).TS_CAPTURED := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;
                          t_met(i).TP_READING := 'I';
                          t_met(i).AM_READING := 0;
                          t_met(i).CD_MTR_RD_SRCE_98 := 'H';
--                  END;
              ELSE
                  --SET FIRST READ FOR METER AS INSTALL READ
                  L_PROGRESS := 'DEFAULTING INSTALL METER READ';
                  t_met(i).TP_READING := 'I';
                  t_met(i).CD_MTR_RD_SRCE_98 := 'H';
              END IF;
              IF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > t_met(i).TS_CAPTURED) THEN
                  t_met(i).TS_CAPTURED := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;    --V9.01
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Initial Meter Read Date changed to SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                  L_REC_WAR := TRUE;
--v9.09 - interim workaround for future dates
              ELSIF (t_met(i).TS_CAPTURED > SYSDATE) THEN
                  t_met(i).TS_CAPTURED := SYSDATE;
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - FUTURE Initial Meter Read Date changed to Current Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                  L_REC_WAR := TRUE;
--v9.09                  
              END IF;
              l_initialmeterreaddate := t_met(i).TS_CAPTURED;   --v9.03
          ELSE
              IF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > t_met(i).TS_CAPTURED) THEN
                  t_met(i).TS_CAPTURED := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;    --V9.01
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Meter Read Date changed to SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                  L_REC_WAR := TRUE;
--v9.09 - interim workaround for future dates
              ELSIF (t_met(i).TS_CAPTURED > SYSDATE) THEN
                  t_met(i).TS_CAPTURED := SYSDATE;
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - FUTURE Meter Read Date changed to Current Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                  L_REC_WAR := TRUE;
--v9.09                  
              END IF;
              
              --v9.05 - Check for duplicate METERREADDATES (DATE PART)
              IF (l_prev_readdate = TO_CHAR(t_met(i).TS_CAPTURED, 'DD-MON-YYYY')) THEN
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('DUPLICATE METERREADDATE FOR METER',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                  L_REC_EXC := TRUE;
              END IF;
          END IF;

          IF (l_prev_met <> t_met(i).NO_EQUIPMENT AND l_manufserialchk = 0) THEN
              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('FK-Manufacturer and SerialNum not in MO_Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              L_REC_EXC := TRUE;
          ELSE
              l_progress := 'GETTING METERREADTYPE';
              --Mapping METERREADTYPE - D3010
              IF (t_met(i).TP_READING ='I') THEN
                  L_MO.METERREADTYPE := 'I';
              ELSIF (t_met(i).TP_READING IS NOT NULL) THEN
                  L_MO.METERREADTYPE := 'P';
              ELSE
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('INVALID METERREADTYPE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                  L_REC_EXC := TRUE;
              END IF;

              l_progress := 'GETTING REREADFLAG';
              --Mapping ReReadFlag
-- ***** ISSUE - NEED Mapping Rule
              L_MO.REREADFLAG := 0;


              l_progress := 'VALIDATING METERREAD';
              -- VALIDATION - METERREAD MUST BE GREATER THAN PREVIOUS UNLESS ROLLOVERINDICATOR = 1
              IF (l_prev_met <> t_met(i).NO_EQUIPMENT) THEN
                  L_MO.METERREAD := t_met(i).AM_READING;   --NO_EQUIPMENT changed from prev, write reading
              ELSIF (t_met(i).AM_READING >= l_prev_am_reading) THEN   --V8.03
                  L_MO.METERREAD := t_met(i).AM_READING;   -- LATEST READING GREATER THEN PREVIOUS READING - WRITE READING
              ELSIF (t_met(i).ROLLOVERINDICATOR = 1) THEN
                  L_MO.METERREAD := t_met(i).AM_READING;   -- ROLLOVER - WRITE READING
--**** V9.02 -- remove restriction  V8.03 Drop reading if SAME as previous
              --ELSIF (t_met(i).AM_READING = l_prev_am_reading) THEN   
              --    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Meter Read SAME as previous-Reading Dropped',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
              --    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
              --    L_REC_EXC := TRUE;
--*** V9.02 - V8.03
              ELSE
                  t_met(i).ROLLOVERINDICATOR := 1;
                  L_MO.METERREAD := t_met(i).AM_READING;    --v9.06
--                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-Invalid ROLLOVER Meter Read',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
--                  L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
--                  L_REC_WAR := TRUE;
              END IF;
              --L_MO.METERREAD := t_met(i).AM_READING;

              l_progress := 'GETTING METERREADMETHOD';
              -- MAPPING MaterReadMethod D3044
              CASE
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='C' THEN L_MO.METERREADMETHOD := 'CUSTOMER';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='F' THEN L_MO.METERREADMETHOD := 'ESTIMATED';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='G' THEN L_MO.METERREADMETHOD := 'ESTIMATED';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='H' THEN L_MO.METERREADMETHOD := 'VISUAL';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='I' THEN L_MO.METERREADMETHOD := 'CUSTOMER';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='L' THEN L_MO.METERREADMETHOD := 'CUSTOMER';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='N' THEN L_MO.METERREADMETHOD := 'VISUAL';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='P' THEN L_MO.METERREADMETHOD := 'CUSTOMER';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='S' THEN L_MO.METERREADMETHOD := 'ESTIMATED';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='U' THEN L_MO.METERREADMETHOD := 'VISUAL';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='W' THEN L_MO.METERREADMETHOD := 'CUSTOMER';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='X' THEN L_MO.METERREADMETHOD := 'ESTIMATED';
                  WHEN t_met(i).CD_MTR_RD_SRCE_98 ='O' THEN L_MO.METERREADMETHOD := 'VISUAL';       --*** v8.02 (2) - workaround for O (Service Order)
                  ELSE
                      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('METERREADMETHOD NOT FOUND IN Mappings',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                      L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                      L_REC_EXC := TRUE;
              END CASE;

              l_progress := 'GETTING ESTIMATEDREADREASONCODE ESTIMATEDREADREMEDIALWORKIND';
              -- MAPPING ESTIMATEDREADREASONCODE D3028 AND ESTIMATEDREADREMEDIALWORKIND D3029
              IF (L_MO.METERREADMETHOD = 'ESTIMATED') THEN
                  L_MO.ESTIMATEDREADREASONCODE := 'MI';   --**** V2.03 I-116 - Final RULE
                  L_MO.ESTIMATEDREADREMEDIALWORKIND := 0;
              END IF;

--*** D_22 GETTING INITIALMETERREADDATE BASED ON EARLIEST METER READ
--*** V3.01 Final rule Initial read date              
--*** V9.03 MOSL RULE - INITIALMETERREADDATE MUST BE SAME AS I TYPE (install) METER READ
--                      BEGIN
                          L_PROGRESS := 'GETTING INITIALMETERREADDATE';
--                          SELECT MIN(T195.TS_CAPTURED)
--                          INTO  L_MO.INITIALMETERREADDATE
--                          FROM  CIS.TVP195READING T195
--                          WHERE  T195.NO_EQUIPMENT = t_met(i).NO_EQUIPMENT;
--
--                          IF (L_MO.INITIALMETERREADDATE IS NULL) THEN
--                          --IF STILL NULL THEN NO READINGS FOUND, DROP METER
--                              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('No Initial Meter Read Date Found',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--                              L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--                              L_REC_EXC := true;
--                          ELSIF (t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE > L_MO.INITIALMETERREADDATE) THEN
--                              l_progress := 'Initial Meter Read Date set to SPID Date';
--                              L_MO.INITIALMETERREADDATE := t_met(i).SUPPLYPOINTEFFECTIVEFROMDATE;                          
--                              P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Warning - Initial Meter Read Date changed to SPID Effective Date',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
--                              L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
--                              L_REC_WAR := TRUE;
--                          END IF;
--                      END;
                L_MO.INITIALMETERREADDATE := l_initialmeterreaddate;

          END IF;


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
                  , INSTALLEDPROPERTYNUMBER, MANUFCODE  --*** v7.02
                  )
              VALUES
                  (t_met(i).NM_PREFERRED, t_met(i).NO_UTL_EQUIP, t_met(i).NO_EQUIPMENT, t_met(i).TS_CAPTURED, L_MO.METERREADTYPE, t_met(i).ROLLOVERINDICATOR, L_MO.REREADFLAG
                  , L_MO.METERREAD, L_MO.METERREADMETHOD, L_MO.ESTIMATEDREADREASONCODE, t_met(i).ROLLOVERFLAG, L_MO.ESTIMATEDREADREMEDIALWORKIND, L_MO.INITIALMETERREADDATE
                  , NULL, NULL, NULL, NULL, NULL, NULL, NULL
                  , t_met(i).INSTALLEDPROPERTYNUMBER, t_met(i).MANUFCODE  --*** v7.02
                  );
              EXCEPTION
              WHEN OTHERS THEN
                 --L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
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
              l_prev_am_reading := t_met(i).AM_READING;
              l_prev_readdate := TO_CHAR(t_met(i).TS_CAPTURED, 'DD-MON-YYYY');
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
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1150, L_NO_ROW_READ,    'Distinct Meter Readings read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1151, l_no_meter_read,  'Distinct Meters read during Transform');  -- CR01 - add recon counts for METERS
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1160, L_NO_ROW_DROPPED, 'Meter Readings dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1161, l_no_meter_dropped,  'Distinct Meters dropped during Transform');  -- CR01 - add recon counts for METERS
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP34', 1170, l_no_row_insert,  'Meter Readings written to MO_METER_READING during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP34', 1171, l_no_meter_written,  'Distinct Meters written during Transform');  -- CR01 - add recon counts for METERS

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
END P_MOU_TRAN_METER_READING;
/
/
show errors;
exit;
