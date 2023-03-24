create or replace
PROCEDURE P_SAP_DEL_COB_MO(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Control Object Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_COB_MO.sql
--
-- Subversion $Revision: 4889 $
--
-- CREATED        : 09/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP COB upload file
--                  Queries Transform tables and populates table SAP_DEL_COBMO
--                  Writes to file SAP_DEL_COBMO_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      09/06/2016  K.Burton   Initial Draft - Split from P_SAP_DEL_COB
-- V 0.02      22/06/2016  K.Burton   Added additional reconciliation measures to count legacy keys
-- V 0.03      23/06/2016  D.Cheung   Set output date format
-- V 0.04      18/07/2016  K.Burton   CR_017 - getting plan data from SAP_SENSI_EMERGENCY_PLANS
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_COB_MO';
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
  
  l_rec_written                 BOOLEAN;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  l_no_row_dropped_cb           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- count of cross border rows NOT output to any file
  l_keys_written                NUMBER;

  l_count                       NUMBER;
  l_delimiter                   VARCHAR2(1) := '|';
--  l_delimiter                   VARCHAR2(1) := chr(9); 
  l_filepath VARCHAR2(30) := 'SAPDELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100); 
  l_filehandle UTL_FILE.FILE_TYPE;
  l_sql VARCHAR2(2000);
  
  l_timestamp VARCHAR2(20);
  l_prefix VARCHAR2(6) := 'CMO_';
  
  CURSOR cur_tab IS
    SELECT l_prefix || NVL(SDC.SAPFLOCNUMBER,SDC.STWPROPERTYNUMBER) LEGACYRECNUM,
        SDC.LEGACYRECNUM PARENTLEGACYRECNUM,
        SDC.SAPFLOCNUMBER,
--        SDC.SAPEQUIPMENT,
        SDC.STWPROPERTYNUMBER,
        SPA.EFFECTIVEFROMDATE,
        MEP.RATEABLEVALUE,
        MEP.OCCUPENCYSTATUS,
        MEP.BUILDINGWATERSTATUS,
        0 VACANCYCHALLENGEFLAG, -- This is D2032 - not included in MOSL data
        MEP.VOABAREFERENCE,
        MEP.VOABAREFRSNCODE,
        MEP.SECTION154,
--        MEP.PUBHEALTHRELSITEARR, -- V 0.04 
--        MEP.NONPUBHEALTHRELSITE, -- V 0.04 
--        MEP.NONPUBHEALTHRELSITEDSC -- V 0.04 
        NVL(SEP.PUBHEALTHRELSITEARR,0) PUBHEALTHRELSITEARR, -- V 0.04 
        NVL(SEP.NONPUBHEALTHRELSITE,0) NONPUBHEALTHRELSITE, -- V 0.04 
        SEP.NONPUBHEALTHRELSITEDSC -- V 0.04 
    FROM SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
         SAPTRAN.SAP_PROPERTY_ADDRESS SPA,
         SAPTRAN.SAP_SENSI_EMERGENCY_PLANS SEP, -- V 0.04 
         SAP_DEL_COB SDC
    WHERE MEP.STWPROPERTYNUMBER_PK = SDC.STWPROPERTYNUMBER
    AND MEP.STWPROPERTYNUMBER_PK = SPA.STWPROPERTYNUMBER_PK
    AND MEP.STWPROPERTYNUMBER_PK = SEP.STWPROPERTYNUMBER_PK(+); -- V 0.04 
    
  
  TYPE tab IS TABLE OF cur_tab%ROWTYPE INDEX BY PLS_INTEGER;
  t_tab  tab;  
  
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04)
    SELECT DISTINCT 7 COL_COUNT,  -- indicates that rows of type HEADER will have max 7 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A HEADER' SECTION_ID, -- work around to get sections is sequence
      PARENTLEGACYRECNUM COL_01,
      NULL COL_02,
      NULL COL_03,
      NULL COL_04
    FROM SAP_DEL_COBMO
    ORDER BY LEGACYRECNUM;   
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09,COL_10,COL_11)
    SELECT 14 COL_COUNT,  -- indicates that rows of type COBMO will have max 14 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B COBMO' SECTION_ID, -- work around to get sections is sequence
    TO_CHAR(EFFECTIVEFROMDATE,'YYYYMMDD') COL_01,
    RATEABLEVALUE COL_02,
    OCCUPENCYSTATUS COL_03,
    BUILDINGWATERSTATUS COL_04,
    VACANCYCHALLENGEFLAG COL_05,
    VOABAREFERENCE COL_06,
    VOABAREFRSNCODE COL_07,
    SECTION154 COL_08,
    PUBHEALTHRELSITEARR COL_09,
    NONPUBHEALTHRELSITE COL_10,
    NONPUBHEALTHRELSITEDSC COL_11
    FROM SAP_DEL_COBMO
    ORDER BY LEGACYRECNUM;   
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT,  -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C ENDE' SECTION_ID   -- work around to get sections is sequence
    FROM SAP_DEL_COBMO
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

  OPEN cur_tab;

  l_progress := 'loop processing ';
  
  LOOP
    FETCH cur_tab BULK COLLECT INTO t_tab LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_tab.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
      l_err.TXT_KEY := t_tab(i).LEGACYRECNUM;  
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_COBMO ';
          
          INSERT INTO SAP_DEL_COBMO VALUES t_tab(i);

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
                 CLOSE cur_tab;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_tab
     
    IF t_tab.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_tab
  COMMIT;
  CLOSE cur_tab; 
  
  --  write the child table recon figures
  l_progress := 'Writing Child Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3550, l_no_row_read,    'COBMO (Connection Objects) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3560, l_no_row_dropped, 'COBMO (Connection Objects) dropped during extract from Transform tables');  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3570, l_no_row_insert, 'COBMO (Connection Objects) inserted to SAP_DEL_POD');  

  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_keys_written
  FROM SAP_DEL_COBMO;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3575, l_keys_written, 'COBMO (Connection Objects) distinct legacy keys inserted to SAP_DEL_COBMO');  

   -- reset count
   l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3580, l_keys_written, 'COBMO (Connection Objects) distinct legacy keys inserted into SAP_DEL_OUTPUT');

  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3585, l_no_row_insert, 'COBMO (Connection Objects) output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_COBMO_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''CMO\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3590, l_no_row_written, 'COBMO (Connection Objects) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP71', 3595, l_keys_written, 'COBMO (Connection Objects) distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_COBMO',
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
END P_SAP_DEL_COB_MO;
/
exit;