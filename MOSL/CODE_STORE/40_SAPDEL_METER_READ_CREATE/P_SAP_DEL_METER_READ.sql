create or replace
PROCEDURE P_SAP_DEL_METER_READ(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter (Device) Read Delivery
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_SAP_DEL_METER_READ.sql
--
-- Subversion $Revision: 4935 $
--
-- CREATED        : 08/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP METER READ upload file
--                  Queries Transform tables and populates tables SAP_DEL_METER_READ
--                  Writes to file SAP_DEL_METER_READ_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      08/06/2016  D.Cheung   Initial Draft
-- V 0.02      09/06/2016  D.Cheung   Move prefixes to table instead of file write
-- V 0.03      10/06/2016  D.Cheung   Relink to get SAPEQUIPMENT from MO_METER directly - post SAPTRAN rebuild  
-- V 0.04      14/06/2016  D.Cheung   Add Control Points and Reconciliation Points
-- V 0.05      17/06/2016  D.Cheung   Change Date format 'YYYYMMDD'
-- v 0.06      22/06/2016  D.Cheung   Change Output RP to count distinct LEGACYRECNUM
--                                    Add recon points for distinct legacy key counts
-- v 0.07      08/07/2016  D.Cheung   Issues SI-023, SI-024 and SI-025 - removed LEFT JOIN to MO_ELIGIBLE_PREMISES
--                                      Use MASTER_PROPERTY on join to MEP if available 
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_METER_READ';
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
  
-- DEV Header Table
  CURSOR cur_dev IS
    SELECT /*+ full(mo) leading(MR) */ 
          DISTINCT
          --'MRD_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),'FLOCA') || '_' || MM.MANUFACTURERSERIALNUM_PK || '_N' LEGACYRECNUM,
          CASE WHEN SDD.SAPEQUIPMENT IS NOT NULL
               THEN 'MRD_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MR.INSTALLEDPROPERTYNUMBER)) || '_' || SDD.SAPEQUIPMENT || '_E'
               ELSE 'MRD_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MR.INSTALLEDPROPERTYNUMBER)) || '_' || MR.METERREF || '_N'
          END AS LEGACYRECNUM,
          SDD.LEGACYRECNUM PARENTLEGACYRECNUM,
          MR.METERREAD,
          MR.METERREADTYPE,
          NULL AS METERREADERNUMBER,
          MR.METERREADDATE,
          NULL AS ACTUALREADDATE,
          MR.REREADFLAG,
          MR.ROLLOVERINDICATOR,
          MR.ROLLOVERFLAG,
          MR.ESTIMATEDREADREASONCODE,
          MR.ESTIMATEDREADREMEDIALWORKIND,
          MR.METERREADMETHOD,
          SDD.STWPROPERTYNUMBER_PK,
          SDD.SAPFLOCNUMBER,
          SDD.SAPEQUIPMENT,
          SDD.STWMETERREF
    FROM SAPTRAN.MO_METER_READING MR
    JOIN SAPTRAN.MO_METER MM ON (MR.METERREF = MM.METERREF)
    JOIN SAP_DEL_DEV SDD ON (MR.MANUFACTURER_PK = SDD.MANUFACTURER_PK
        AND MR.MANUFACTURERSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK)
    JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK) 
    WHERE SDD.NONMARKETMETERFLAG = 0
    ORDER BY SDD.STWMETERREF, MR.METERREAD;
   
  TYPE tab_dev IS TABLE OF cur_dev%ROWTYPE INDEX BY PLS_INTEGER;
  t_dev  tab_dev;
     
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
--*** INSERT READ (CREATE) FILE RECORDS ***
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09,COL_10,COL_11,COL_12)
    SELECT 15 COL_COUNT, -- indicates that rows of type IEABLU will have max 15 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A IEABLU' SECTION_ID,  -- work around to get sections in sequence
      PARENTLEGACYRECNUM COL_01,
      METERREAD COL_02,
      METERREADTYPE COL_03,
      METERREADERNUMBER COL_04,
      TO_CHAR(METERREADDATE,'YYYYMMDD') COL_05,
      ACTUALREADDATE COL_06,
      REREADFLAG COL_07,
      ROLLOVERINDICATOR COL_08,
      ROLLOVERFLAG COL_09,
      ESTIMATEDREADREASONCODE COL_10,
      ESTIMATEDREADREMEDIALWORKIND COL_11,
      METERREADMETHOD COL_12
    FROM SAP_DEL_METER_READ
    ORDER BY LEGACYRECNUM, METERREADDATE;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT, -- indicates that rows of type ENDE will have max 2 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'B ENDE' SECTION_ID   -- work around to get sections in sequence
    FROM SAP_DEL_METER_READ
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

  -- POPULATE DEV (HEADER) TABLE
  -- start processing all records for range supplied
  OPEN cur_dev;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_dev BULK COLLECT INTO t_dev LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_dev.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
--      l_err.TXT_KEY := t_dev(i).LEGACYRECNUM; 
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_METER_READ ';
          l_seq := l_seq + 1;
          l_err.TXT_KEY := t_dev(i).LEGACYRECNUM;          
          INSERT INTO SAP_DEL_METER_READ VALUES t_dev(i);
          
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
                 CLOSE cur_dev;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_dev
     
    IF t_dev.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_dev

  COMMIT;
  CLOSE cur_dev;  
  
  --  write the DEV header table recon figures
  l_progress := 'Writing Header Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3450, l_no_row_read, 'METER READ read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3460, l_no_row_dropped, 'METER READ  dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3470, l_no_row_insert, 'METER READ inserted to SAP_DEL_METER_READ');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_METER_READ;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3475, l_no_keys_written, 'METER READ distinct legacy keys inserted to SAP_DEL_METER_READ');  

  -- reset count
  l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_no_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3480, l_no_keys_written, 'METER READ distinct legacy keys inserted into SAP_DEL_OUTPUT');
  
  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3485, l_no_row_insert, 'METER READ output rows inserted into SAP_DEL_OUTPUT');


    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_METER_READ_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''MRD\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  --l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''%\_MR'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_no_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3490, l_no_row_written, 'METER READ output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP69', 3495, l_no_keys_written, 'METER READ distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_METER_READ',
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
END P_SAP_DEL_METER_READ;
/
/
show errors;
exit;