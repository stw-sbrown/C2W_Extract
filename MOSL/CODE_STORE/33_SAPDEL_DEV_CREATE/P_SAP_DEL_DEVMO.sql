create or replace
PROCEDURE P_SAP_DEL_DEVMO(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter (Device) MO UPDATE Delivery
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_SAP_DEL_DEVMO.sql
--
-- Subversion $Revision: 5060 $
--
-- CREATED        : 09/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP DEVMO upload file
--                  Queries Transform tables and populates table SAP_DEL_DEVMO
--                  Writes to file SAP_DEL_DEVMO_<date/timestamp>.dat
--
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      23/05/2016  K.Burton   Initial Draft
-- V 0.02      10/06/2016  D.Cheung   Relink to get SAPEQUIPMENT from MO_METER directly - post SAPTRAN rebuild  
-- V 0.03      14/06/2016  D.Cheung   Add Control Points and Reconciliation Points
--                                    Change Effectivefromdate to installationdate
-- V 0.04      17/06/2016  D.Cheung   Change Date Format 'YYYYMMDD'
-- v 0.05      22/06/2016  D.Cheung   Change Output RP to count distinct LEGACYRECNUM
--                                    Add recon points for distinct legacy key counts
-- v 0.06      08/07/2016  D.Cheung   Issues SI-023, SI-024 and SI-025 - removed LEFT JOIN to MO_ELIGIBLE_PREMISES
--                                      Use MASTER_PROPERTY on join to MEP if available 
-- V 0.07      28/07/2016  D.Cheung   SAP Defect 138+140 - Set RTS to 100 for SAP for Potable water meters (i.e. where RTS is null)
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_DEVMO';
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

-- Child Table - DEVMO
  CURSOR cur_devmo IS
    SELECT /*+ full(mm) index(mm,pk_man_serial_ref_comp) leading(mm,mo) */
           DISTINCT
          --'DEVMO_' || SUBSTR(SDD.LEGACYRECNUM,1,LENGTH(SDD.LEGACYRECNUM)-2) LEGACYRECNUM,
          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
               THEN 'DMO_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.SAPEQUIPMENT || '_E'
               ELSE 'DMO_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.METERREF || '_N'
          END AS LEGACYRECNUM,
          SDD.LEGACYRECNUM PARENTLEGACYRECNUM,
          MR.INITIALMETERREADDATE AS EFFECTIVEFROMDATE,
          MM.YEARLYVOLESTIMATE,
          MM.WATERCHARGEMETERSIZE,
          MM.SEWCHARGEABLEMETERSIZE,
          NVL(MM.RETURNTOSEWER,100) RETURNTOSEWER,
          MM.METERTREATMENT,
          MM.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
          MEP.SAPFLOCNUMBER,
          MM.SAPEQUIPMENT,
          MM.METERREF AS STWMETERREF
    FROM SAPTRAN.MO_METER MM
    JOIN SAPTRAN.MO_METER_READING MR ON (MM.METERREF = MR.METERREF)
    JOIN SAP_DEL_DEV SDD ON (MM.MANUFACTURER_PK = SDD.MANUFACTURER_PK
        AND MM.MANUFACTURERSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK)
    JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK)
    WHERE SDD.NONMARKETMETERFLAG = 0
    ORDER BY SDD.LEGACYRECNUM;
      
  TYPE tab_devmo IS TABLE OF cur_devmo%ROWTYPE INDEX BY PLS_INTEGER;
  t_devmo  tab_devmo;  
    
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN  
--*** INSERT DEVMO FILE RECORDS ***
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04)
    SELECT DISTINCT 7 COL_COUNT,  -- indicates that rows of type HEADER will have max 7 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A HEADER' SECTION_ID, -- work around to get sections in sequence
      NULL COL_01,
      NULL COL_02,
      NULL COL_03,
      PARENTLEGACYRECNUM COL_04
    FROM SAP_DEL_DEVMO
    ORDER BY LEGACYRECNUM; 
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05)
    SELECT 8 COL_COUNT, -- indicates that rows of type DEVMO will have max 8 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'B DEVMO' SECTION_ID, -- work around to get sections in sequence
      TO_CHAR(EFFECTIVEFROMDATE,'YYYYMMDD') COL_01,
      YEARLYVOLESTIMATE COL_02,
      WATERCHARGEMETERSIZE COL_04,
      SEWCHARGEABLEMETERSIZE COL_04,
      RETURNTOSEWER COL_05     
    FROM SAP_DEL_DEVMO
    ORDER BY LEGACYRECNUM;

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT, -- indicates that rows of type ENDE will have max 2 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'C ENDE' SECTION_ID   -- work around to get sections in sequence
    FROM SAP_DEL_DEVMO
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
  
--PROCESS CHILD DEVICE MOSL ****  
   -- reset counts
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   
  -- POPULATE DEVMO TABLE BASED ON HEADER TABLE CONTENTS
  -- start processing all records for range supplied
  OPEN cur_devmo;

  l_progress := 'loop dec processing ';
  l_prev_parent := NULL;
  l_seq := 0;  -- reset key sequence number

  LOOP
    FETCH cur_devmo BULK COLLECT INTO t_devmo LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_devmo.COUNT
    LOOP
        l_no_row_read := l_no_row_read + 1;
        l_err.TXT_KEY := t_devmo(i).PARENTLEGACYRECNUM;  
        l_rec_written := TRUE;
        BEGIN
            -- write the data to the delivery table
            l_progress := 'insert row into SAP_DEL_DEVMO ';

            IF t_devmo(i).PARENTLEGACYRECNUM <> l_prev_parent THEN
                l_seq := l_seq + 1;
                l_prev_parent := t_devmo(i).PARENTLEGACYRECNUM;
            END IF;

            INSERT INTO SAP_DEL_DEVMO VALUES t_devmo(i);
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
                 CLOSE cur_devmo;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_devmo
     
    IF t_devmo.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_devmo

  COMMIT;
  CLOSE cur_devmo; 
  
  --  write the DEVMO table recon figures
  l_progress := 'Writing DEVMO Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3200, l_no_row_read,    'DEVMO (Meter) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3210, l_no_row_dropped, 'DEVMO (Meter) dropped during extract from Transform tables');  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3220, l_no_row_insert, 'DEVMO (Meter) inserted to SAP_DEL_DEVMO');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_DEVMO;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3225, l_no_keys_written, 'DEVMO distinct legacy keys inserted to SAP_DEL_DEVMO');  

  -- reset count
  l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_no_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3230, l_no_keys_written, 'DEVMO distinct legacy keys inserted into SAP_DEL_OUTPUT');
  
  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3235, l_no_row_insert, 'DEVMO output rows inserted into SAP_DEL_OUTPUT');


    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  
  l_filename := 'SAP_DEL_DEVMO_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''DMO\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  --l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''%\_DMO'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_no_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3240, l_no_row_written, 'DEVMO (Meter) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP64', 3245, l_no_keys_written, 'DEVMO distinct legacy keys written to file'); 

  -- archive the latest batch                          
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_DEVMO',
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
END P_SAP_DEL_DEVMO;
/
/
show errors;
exit;