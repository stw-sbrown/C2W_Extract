create or replace
PROCEDURE P_SAP_DEL_DEVCHAR(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter (Device) Characteristics Update Delivery
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_SAP_DEL_DEVCHAR.sql
--
-- Subversion $Revision: 4928 $
--
-- CREATED        : 09/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP DEVCHAR upload file
--                  Queries Transform tables and populates tables SAP_DEL_DEVCHAR
--                  Writes to files SAP_DEL_DEC_<date/timestamp>.dat
--
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/06/2016  D.Cheung   Initial Draft
-- V 1.01      10/06/2016  D.Cheung   Relink to get SAPEQUIPMENT from MO_METER directly - post SAPTRAN rebuild  
-- V 1.02      14/06/2016  D.Cheung   Add Control Points and Reconciliation Points
-- v 1.03      22/06/2016  D.Cheung   Change Output RP to count distinct LEGACYRECNUM
--                                    Add recon points for distinct legacy key counts
-- v 1.04      08/07/2016  D.Cheung   Issues SI-023, SI-024 and SI-025 - removed LEFT JOIN to MO_ELIGIBLE_PREMISES
--                                      Use MASTER_PROPERTY on join to MEP if available  
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_DEVCHAR';
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
  CURSOR cur_dec IS
    SELECT /*+ full(mo) leading(mm, sdd) */ 
          DISTINCT
          --'DEC_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),'FLOCA') || '_' || MM.MANUFACTURERSERIALNUM_PK || '_N' LEGACYRECNUM,
          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
               THEN 'DEC_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.SAPEQUIPMENT || '_E'
               ELSE 'DEC_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.METERREF || '_N'
          END AS LEGACYRECNUM,
          SDD.LEGACYRECNUM PARENTLEGACYRECNUM,
      --CHARACTERISTICS
          MM.METERREADFREQUENCY,
          MM.DATALOGGERWHOLESALER,
          MM.DATALOGGERNONWHOLESALER,
          MM.METERLOCATIONCODE,
          MM.METEROUTREADERGPSX,
          MM.METEROUTREADERGPSY,
          MM.METEROUTREADERLOCCODE,
          MM.COMBIMETERFLAG,
          MM.REMOTEREADFLAG,
          MM.OUTREADERID,
          MM.OUTREADERPROTOCOL,
          MM.REMOTEREADTYPE,
          MM.PHYSICALMETERSIZE,
          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
               THEN MM.METERTREATMENT
               ELSE NULL
          END AS METERTREATMENT,
          MM.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
          MEP.SAPFLOCNUMBER,
          MM.SAPEQUIPMENT,
          MM.METERREF AS STWMETERREF
    FROM SAPTRAN.MO_METER MM
    JOIN SAP_DEL_DEV SDD ON (MM.MANUFACTURER_PK = SDD.MANUFACTURER_PK
        AND MM.MANUFACTURERSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK)
    JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK)
    WHERE MM.NONMARKETMETERFLAG = 0
    ORDER BY MM.METERREF;
    
  TYPE tab_dec IS TABLE OF cur_dec%ROWTYPE INDEX BY PLS_INTEGER;
  t_dec  tab_dec;
     
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
--*** INSERT DEC (DEVCHAR) FILE RECORDS ***
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01)
    SELECT DISTINCT 4 COL_COUNT, -- indicates that rows of type CLSOBJ will have max 4 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A CLSOBJ' SECTION_ID,  -- work around to get sections in sequence
      METERTREATMENT COL_01
    FROM SAP_DEL_DEVCHAR
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01)
    SELECT DISTINCT 4 COL_COUNT, -- indicates that rows of type CLSDAT will have max 4 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B CLSDAT' SECTION_ID,  -- work around to get sections in sequence
      PARENTLEGACYRECNUM COL_01
    FROM SAP_DEL_DEVCHAR
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09,COL_10,COL_11,COL_12,COL_13)
    SELECT 16 COL_COUNT, -- indicates that rows of type CLSVAL will have max 5 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'C ZCHVAL' SECTION_ID, -- work around to get sections in sequence
      METERREADFREQUENCY COL_01,
      DATALOGGERWHOLESALER COL_02,
      DATALOGGERNONWHOLESALER COL_03,
      METERLOCATIONCODE COL_04,
      TRIM(TO_CHAR(METEROUTREADERGPSX,'000000.9')) COL_05,
      TRIM(TO_CHAR(METEROUTREADERGPSY,'000000.9')) COL_06,
      METEROUTREADERLOCCODE COL_07,
      COMBIMETERFLAG COL_08,
      REMOTEREADFLAG COL_09,
      OUTREADERID COL_10,
      OUTREADERPROTOCOL COL_11,
      REMOTEREADTYPE COL_12,
      PHYSICALMETERSIZE COL_13
    FROM SAP_DEL_DEVCHAR
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT, -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'D ENDE' SECTION_ID   -- work around to get sections in sequence
    FROM SAP_DEL_DEVCHAR
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

  -- POPULATE DEVCHAR TABLE
  -- start processing all records for range supplied
  OPEN cur_dec;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_dec BULK COLLECT INTO t_dec LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_dec.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_DEVCHAR ';
          l_seq := l_seq + 1;
          l_err.TXT_KEY := t_dec(i).LEGACYRECNUM;          
          INSERT INTO SAP_DEL_DEVCHAR VALUES t_dec(i);
          
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
                 CLOSE cur_dec;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_dec
     
    IF t_dec.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_dec

  COMMIT;
  CLOSE cur_dec;  
  
  --  write the DEV header table recon figures
  l_progress := 'Writing Header Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3250, l_no_row_read, 'DEC (Meter) Characteristic read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3260, l_no_row_dropped, 'DEC (Meter) Characteristic dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3270, l_no_row_insert, 'DEC (Meter) Characteristic inserted to SAP_DEL_DEVCHAR');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_DEVCHAR;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3275, l_no_keys_written, 'DEC distinct legacy keys inserted to SAP_DEL_DEVCHAR');  

  -- reset count
  l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_no_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3280, l_no_keys_written, 'DEC distinct legacy keys inserted into SAP_DEL_OUTPUT');
  
  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3285, l_no_row_insert, 'DEC output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  --WRITE DEC FILE
  l_filename := 'SAP_DEL_DEVCHAR_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''DEC\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID,COL_01';
  --l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''%\_DEC'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_no_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the char output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3290, l_no_row_written, 'DEC (Meter) Characteristic output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP65', 3295, l_no_keys_written, 'DEC distinct legacy keys written to file'); 


  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_DEVCHAR',
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
END P_SAP_DEL_DEVCHAR;
/
/
show errors;
exit;