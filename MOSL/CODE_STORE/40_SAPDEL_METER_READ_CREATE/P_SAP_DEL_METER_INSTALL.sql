create or replace
PROCEDURE P_SAP_DEL_METER_INSTALL(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter (Device) Read Delivery
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_SAP_DEL_METER_INSTALL.sql
--
-- Subversion $Revision: 6406 $
--
-- CREATED        : 09/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP METER INSTALL upload files
--                  Queries Transform tables and populates tables SAP_DEL_METER_INSTALL
--                  Writes to file SAP_DEL_MIG_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/06/2016  D.Cheung   Initial Draft
-- V 0.02      14/06/2016  D.Cheung   Add Control Points and Reconciliation Points
-- V 0.03      15/06/2016  D.Cheung   Add Service Component Type to Legacy Key
-- V 0.04      17/06/2016  D.Cheung   Change Date format 'YYYYMMDD'
-- V 0.05      20/06/2016  D.Cheung   Remove extra SCM field
-- v 0.06      22/06/2016  D.Cheung   Change Output RP to count distinct LEGACYRECNUM
--                                    Add recon points for distinct legacy key counts
-- v 0.07      29/06/2016  K.Burton   CR_005 - Added D3004 and D3036 to output
-- v 0.08      30/06/2016  K.Burton   CR_006 - Removed Period Consumption from SAP_DEL_METER_INSTALL
-- v 0.09      05/07/2016  K.Burton   SI-017 - altered join MO_ELIGIBLE_PREMISES to make it
--                                    JOIN rather than LEFT JOIN - only need meters from eligible properties
--                         D.Cheung   Use MASTER_PROPERTY on join to MEP if available 
-- v 0.10      11/07/2016  D.Cheung   SI_026 - D3004 and D3036 fields wrong way round in output file
-- v 0.11      13/07/2016  K.Burton   SI_027,SI_028  - Rebuilt Cursor to derive Sewerage rows
-- v 0.12      14/07/2016  D.Cheung   SI_027,SI_028 - Fix for duplicate KEYS on TE, join by DPID
--                         D.Cheung   CR_014 - Get MEASUREUNITATMETER as PRE-TRANSFORM value
-- v 0.13      19/07/2016  K.Burton   CR_019 - Activity Reason code ZL added for inferred meter relationships
-- v 0.14      22/07/2016  K.Burton   Defect 110 - CR_014 fix was incomplete - original MEASUREUNITATMETER still
--                                    being retrieved for TE meters instead of new UNITOFMEASURE value
-- v 0.15      25/07/2016  K.Burton   Defect 123 - Added addition criteria on METERTREATMENT to main cursor
--                                    to restrict meters being returned to only one side of the UNION or the other
-- v 0.16      30/11/2016  D.Cheung   Fix issue with CROSSBORDER metertreatments not being included
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_METER_INSTALL';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_written              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_keys_written             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  
  l_rec_written                 BOOLEAN;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  l_no_row_dropped_cb           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- count of cross border rows NOT output to any file
  
  l_count                       NUMBER;
  l_delimiter                   VARCHAR2(1) := '|';
--  l_delimiter                   VARCHAR2(1) := chr(9); 
  l_filepath VARCHAR2(30) := 'SAPDELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100); 
  l_filehandle UTL_FILE.FILE_TYPE;
  l_sql VARCHAR2(2000);

  l_seq NUMBER := 0;
  l_prev_parent VARCHAR2(30) := 'X';
  l_timestamp VARCHAR2(20); 
  l_sap_floca     NUMBER(30);
  l_sap_equipment NUMBER(10);
  
-- DEV Installation Table
  CURSOR cur_mic IS
    SELECT DISTINCT * 
    FROM (
      SELECT /*+ full(mm) leading(mm)   */ 
            CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
                THEN 'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) 
                           || '_' || MM.SAPEQUIPMENT || SDS.SERVICECOMPONENTTYPE || '_E'
                ELSE 'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) 
                           || '_' || MM.METERREF || SDS.SERVICECOMPONENTTYPE || '_N'
             END AS LEGACYRECNUM,
             DECODE(MM.SAPEQUIPMENT,NULL, (SELECT LEGACYRECNUM FROM SAP_DEL_DVLCRT WHERE SAPFLOCNUMBER = MEP.SAPFLOCNUMBER AND STWMETERREF = MM.METERREF),NULL) DVLEGACYRECNUM,
             SDS.LEGACYRECNUM SCMLEGACYRECNUM,
             MR2.INITIALMETERREADDATE INITIALMETERREADDATE,
             DECODE(MM.SAPEQUIPMENT,NULL,'01','04') INSTALLTYPE,
             MR2.METERREAD  METERREAD,
             MM.NUMBEROFDIGITS,     -- D3004 - v 0.07
             NVL(MM.UNITOFMEASURE,DECODE(MM.MEASUREUNITATMETER,'METRICm3','M3','GAL')) AS MEASUREUNITATMETER,   --v0.16
--             MM.UNITOFMEASURE MEASUREUNITATMETER,   -- D3036 - v 0.07 v 0.12
             SDD.LEGACYRECNUM  DEVLEGACYRECNUM ,
             NULL PERCENTAGEDISCHARGE,             --D3024  
             DECODE(SDS.SERVICECOMPONENTTYPE,'MS','ZL','ZA') AS ACTIVITYREASON,    --D3045/D3046  -- v 0.13 
--             'ZA' AS ACTIVITYREASON,    --D3045/D3046  -- v 0.13 
             NULL EFFECTIVEFROMDATE,
              MM.INSTALLEDPROPERTYNUMBER STWPROPERTYNUMBER_PK, 
              MEP.SAPFLOCNUMBER,
              SDS.SPID_PK,
              MM.SAPEQUIPMENT,        
             MM.METERREF
      FROM SAPTRAN.MO_SUPPLY_POINT MSP, 
           SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
           (SELECT METERREF, MIN(METERREADDATE) FIRSTDATE FROM SAPTRAN.MO_METER_READING GROUP BY METERREF) MR,
           SAPTRAN.MO_METER_READING MR2,
           SAP_DEL_SCM SDS,
           SAP_DEL_POD SDP,
           SAP_DEL_DEV SDD,
           SAPTRAN.MO_METER MM
      WHERE SDP.STWPROPERTYNUMBER = MEP.STWPROPERTYNUMBER_PK
      AND MSP.SPID_PK = SDP.SPID_PK
      AND SDS.SPID_PK = MSP.SPID_PK
      AND MM.METERREF = MR.METERREF
      AND MR2.METERREF = MR.METERREF 
      AND MR2.METERREADDATE = MR.FIRSTDATE
      AND MM.MANUFACTURER_PK = SDD.MANUFACTURER_PK 
      AND MM.MANUFACTURERSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK
      AND SUBSTR(MM.SPID_PK,1,10) = MSP.CORESPID_PK
      AND SDS.SERVICECOMPONENTTYPE IN ('MPW','MS')
      AND MM.NONMARKETMETERFLAG = 0
      AND MM.METERTREATMENT IN ('POTABLE','CROSSBORDER') -- v 0.15   --v0.16
      UNION
      SELECT CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
                THEN 'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) 
                           || '_' || MM.SAPEQUIPMENT || MDP.SERVICECOMPTYPE || '_E'
                ELSE 'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) 
                           || '_' || MM.METERREF || MDP.SERVICECOMPTYPE || '_N'
             END AS LEGACYRECNUM,
             DECODE(MM.SAPEQUIPMENT,NULL, (SELECT LEGACYRECNUM FROM SAP_DEL_DVLCRT WHERE SAPFLOCNUMBER = MEP.SAPFLOCNUMBER AND STWMETERREF = MM.METERREF),NULL) DVLEGACYRECNUM,
            (SELECT LEGACYRECNUM FROM SAP_DEL_SCMTE WHERE SAPFLOCNUMBER = MEP.SAPFLOCNUMBER AND DPID_PK = MDP.DPID_PK) SCMLEGACYRECNUM,
             MR2.INITIALMETERREADDATE INITIALMETERREADDATE,
             DECODE(MM.SAPEQUIPMENT,NULL,'01','04') INSTALLTYPE,
             MR2.METERREAD  METERREAD,
             MM.NUMBEROFDIGITS,     -- D3004 - v 0.07
             NVL(MM.UNITOFMEASURE,DECODE(MM.MEASUREUNITATMETER,'METRICm3','M3','GAL')) AS MEASUREUNITATMETER,   --v0.16
--             MM.UNITOFMEASURE MEASUREUNITATMETER,   -- D3036 - v 0.07 / v 0.14 (Defect 110)
             SDD.LEGACYRECNUM  DEVLEGACYRECNUM ,
             (SELECT MD.PERCENTAGEDISCHARGE FROM SAPTRAN.MO_METER_DPIDXREF MD, SAP_DEL_SCMTE SDST  WHERE MM.METERREF = MD.METERDPIDXREF_PK AND SDST.DPID_PK = MD.DPID_PK) PERCENTAGEDISCHARGE,
             'ZA' AS ACTIVITYREASON,    --D3045/D3046    
             (SELECT MD.EFFECTIVEFROMDATE FROM SAPTRAN.MO_METER_DPIDXREF MD, SAP_DEL_SCMTE SDST WHERE MM.METERREF = MD.METERDPIDXREF_PK AND SDST.DPID_PK = MD.DPID_PK) EFFECTIVEFROMDATE,
              MM.INSTALLEDPROPERTYNUMBER STWPROPERTYNUMBER_PK, 
              MEP.SAPFLOCNUMBER,
              MDP.SPID_PK,
              MM.SAPEQUIPMENT,        
             MM.METERREF
      FROM SAPTRAN.MO_DISCHARGE_POINT MDP, 
           SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
           (SELECT METERREF, MIN(METERREADDATE) FIRSTDATE FROM SAPTRAN.MO_METER_READING GROUP BY METERREF) MR,
           SAPTRAN.MO_METER_READING MR2,
           SAP_DEL_POD SDP,
           SAP_DEL_DEV SDD,
           SAPTRAN.MO_METER MM
      WHERE SDP.STWPROPERTYNUMBER = MEP.STWPROPERTYNUMBER_PK
      AND MDP.SPID_PK = SDP.SPID_PK
      AND MM.METERREF = MR.METERREF
      AND MR2.METERREF = MR.METERREF 
      AND MR2.METERREADDATE = MR.FIRSTDATE
      AND MM.MANUFACTURER_PK = SDD.MANUFACTURER_PK 
      AND MM.MANUFACTURERSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK
      AND MM.DPID_PK = MDP.DPID_PK    --v0.12
      AND MM.SPID_PK = MDP.SPID_PK
      AND MM.METERTREATMENT NOT IN ('POTABLE','CROSSBORDER') -- v 0.15   --v0.16
    )
    ORDER BY METERREF;  
--    SELECT DISTINCT
--          --'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),'FLOCA') || '_' || MM.MANUFACTURERSERIALNUM_PK || '_N' LEGACYRECNUM,
--          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
--               THEN 'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.SAPEQUIPMENT || MSC.SERVICECOMPONENTTYPE || '_E'
--               ELSE 'DI_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.METERREF || MSC.SERVICECOMPONENTTYPE || '_N'
--          END AS LEGACYRECNUM,
--      --DI_INT
--          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
--              THEN NULL
--              ELSE SDLC.LEGACYRECNUM
--          END AS  DVLEGACYRECNUM,               --DVLCRT (D3002)
--          CASE WHEN SDST.DPID_PK IS NULL
--              THEN SDS.LEGACYRECNUM
--              ELSE SDST.LEGACYRECNUM
--          END AS SCMLEGACYRECNUM,
--          MR2.INITIALMETERREADDATE INITIALMETERREADDATE,  --D3042
--          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
--              THEN '04'
--              ELSE '01'
--          END AS INSTALLTYPE,
--      --DI_ZW
--          MR2.METERREAD  METERREAD,               --D3008
----          MM.YEARLYVOLESTIMATE,                   --D2010 - revmoved - v 0.08
--          MM.NUMBEROFDIGITS,     -- D3004 - v 0.07
--          MM.MEASUREUNITATMETER,   -- D3036 - v 0.07
--          SDD.LEGACYRECNUM  DEVLEGACYRECNUM,
--      --DI_GER
--          CASE WHEN SDST.DPID_PK IS NULL
--              THEN NULL
--              ELSE MD.PERCENTAGEDISCHARGE
--          END AS PERCENTAGEDISCHARGE,             --D3024          
--          'ZA' AS ACTIVITYREASON,    --D3045/D3046
--              --NVL(METERADDITIONREASON,METERREMOVALREASON) AS ACTIVITYREASON,    --D3045/D3046
--          CASE WHEN SDST.DPID_PK IS NULL
--              THEN NULL
--              ELSE MD.EFFECTIVEFROMDATE
--          END AS EFFECTIVEFROMDATE,             --D4006
--      --OTHER KEYS
--          MM.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
--          MEP.SAPFLOCNUMBER,
--          MM.SPID_PK AS SPID,
--          MM.SAPEQUIPMENT,
--          MM.METERREF AS STWMETERREF
--          --MSC.SERVICECOMPONENTTYPE
--    FROM SAPTRAN.MO_METER MM
--    JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK)  -- v 0.09 
--    JOIN (SELECT METERREF, MIN(METERREADDATE) FIRSTDATE FROM SAPTRAN.MO_METER_READING GROUP BY METERREF) MR ON MM.METERREF = MR.METERREF
--    JOIN SAPTRAN.MO_METER_READING MR2 ON (MR2.METERREF = MR.METERREF
--        AND MR2.METERREADDATE = MR.FIRSTDATE)
--    JOIN SAP_DEL_DEV SDD ON (MM.MANUFACTURER_PK = SDD.MANUFACTURER_PK
--        AND MM.MANUFACTURERSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK)
--    JOIN SAPTRAN.MO_SERVICE_COMPONENT MSC ON MM.SPID_PK = MSC.SPID_PK
--    LEFT JOIN SAP_DEL_SCM SDS ON (MEP.SAPFLOCNUMBER = SDS.SAPFLOCNUMBER
--        AND MM.SPID_PK = SDS.SPID_PK
--        AND MSC.SERVICECOMPONENTTYPE = SDS.SERVICECOMPONENTTYPE)
--    LEFT JOIN SAP_DEL_SCMTE SDST ON (MEP.SAPFLOCNUMBER = SDST.SAPFLOCNUMBER
--        AND MM.SPID_PK = SDST.SPID_PK
--        AND MSC.SERVICECOMPONENTTYPE = SDST.SERVICECOMPONENTTYPE)
--    LEFT JOIN SAPTRAN.MO_METER_DPIDXREF MD ON (MM.METERREF = MD.METERDPIDXREF_PK
--        AND SDST.DPID_PK = MD.DPID_PK)
--    LEFT JOIN SAP_DEL_DVLCRT SDLC ON (SDLC.SAPFLOCNUMBER = SDD.SAPFLOCNUMBER
--        AND SDLC.STWMETERREF = SDD.STWMETERREF)
--    WHERE MM.NONMARKETMETERFLAG = 0
--    ORDER BY MM.METERREF;

  TYPE tab_mic IS TABLE OF cur_mic%ROWTYPE INDEX BY PLS_INTEGER;
  t_mic  tab_mic;

     
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
--*** INSERT METER INSTALL (CREATE) FILE RECORDS ***
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04)
    SELECT DISTINCT 7 COL_COUNT, -- indicates that rows of type DI_INT will have max 7 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A DI_INT' SECTION_ID,  -- work around to get sections in sequence
      DVLLEGACYRECNUM COL_01,
      SCMLEGACYRECNUM COL_02,
      TO_CHAR(INITIALMETERREADDATE,'YYYYMMDD') COL_03,
      INSTALLTYPE COL_04
    FROM SAP_DEL_METER_INSTALL 
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04)  -- v 0.08
    SELECT DISTINCT 7 COL_COUNT, -- indicates that rows of type DI_ZW will have max 7 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B DI_ZW' SECTION_ID,  -- work around to get sections in sequence
      METERREAD COL_01,
--      YEARLYVOLESTIMATE COL_02, -- v 0.08
      MEASUREUNITATMETER COL_02,  -- v 0.07     
      NUMBEROFDIGITS COL_03,   -- v 0.07
      DEVLEGACYRECNUM COL_04
    FROM SAP_DEL_METER_INSTALL 
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04)
    SELECT DISTINCT 7 COL_COUNT, -- indicates that rows of type DI_GER will have max 7 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C DI_GER' SECTION_ID,  -- work around to get sections in sequence
      DEVLEGACYRECNUM COL_01,
      PERCENTAGEDISCHARGE COL_02,
      ACTIVITYREASON COL_03,
      TO_CHAR(EFFECTIVEFROMDATE,'YYYYMMDD') COL_04
    FROM SAP_DEL_METER_INSTALL 
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT, -- indicates that rows of type ENDE will have max 2 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'D ENDE' SECTION_ID   -- work around to get sections in sequence
    FROM SAP_DEL_METER_INSTALL
    ORDER BY LEGACYRECNUM;    
    
    SELECT COUNT(DISTINCT KEY_COL) 
    INTO l_row_count
    FROM SAP_DEL_OUTPUT;
    
    RETURN l_row_count;
  END PopulateOutputTable;

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
   l_no_row_war := 10;
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

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- POPULATE DEV INSTALL TABLE
  -- start processing all records for range supplied
  OPEN cur_mic;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_mic BULK COLLECT INTO t_mic LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_mic.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
--      l_err.TXT_KEY := t_mic(i).LEGACYRECNUM; 
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_METER_INSTALL ';
          l_seq := l_seq + 1;
          l_err.TXT_KEY := t_mic(i).LEGACYRECNUM;          
          INSERT INTO SAP_DEL_METER_INSTALL VALUES t_mic(i);
          
          l_no_row_insert := l_no_row_insert + 1;          
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
                 CLOSE cur_mic;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_mic
     
    IF t_mic.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_mic

  COMMIT;
  CLOSE cur_mic;  
  
  --  write the DEV INSTALL table recon figures
  l_progress := 'Writing Header Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3400, l_no_row_read, 'METER INSTALL read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3410, l_no_row_dropped, 'METER INSTALL  dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3420, l_no_row_insert, 'METER INSTALL inserted to SAP_DEL_METER_INSTALL');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_METER_INSTALL;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3425, l_no_keys_written, 'METER INSTALL distinct legacy keys inserted to SAP_DEL_METER_INSTALL');  
  
  -- reset count
  l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_no_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3430, l_no_keys_written, 'METER INSTALL distinct legacy keys inserted into SAP_DEL_OUTPUT');
  
  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3435, l_no_row_insert, 'METER INSTALL output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_METER_INSTALL_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''DI\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  --l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''%\_DI'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_no_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3440, l_no_row_written, 'METER INSTALL output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP68', 3445, l_no_keys_written, 'METER INSTALL distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_METER_INSTALL',
                           p_batch_no => no_batch,
                           p_filename => l_filename);
                          
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
END P_SAP_DEL_METER_INSTALL;
/
/
show errors;
exit;