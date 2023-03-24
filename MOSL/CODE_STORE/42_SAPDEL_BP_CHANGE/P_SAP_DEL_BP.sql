create or replace
PROCEDURE P_SAP_DEL_BP(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: SAP Business Partner (Customer)
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_BP.sql
--
-- Subversion $Revision: 5514 $
--
-- CREATED        : 14/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP BP upload file
--                  Queries Transform tables and populates table SAP_DEL_BP
--                  Writes to file SAP_DEL_BP_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      14/06/2016  K.Burton   Initial Draft
-- V 0.02      22/06/2016  K.Burton   Added additional reconciliation measures to count legacy keys
-- V 0.03      12/07/2016  K.Burton   CR_015 - Added section BUT0ID and moved D2005 from BUT000 to BUT0ID
-- V 0.04      18/07/2016  K.Burton   CR_017 - Changed cursor get sensitive customer data from new SAP
--                                    lookup SAP_SENSITIVE
-- V 0.05      01/08/2016  K.Burton   Backed out changes for CR_017 - SAP is not yet ready to take reason codes
--                                    therefore reverting back to previous NA/SEMDV values
-- V 0.06      15/08/2016  K.Burton   CR_021 / Defect 172 - Only single LE/BP should be uploaded to SAP per property. If we have 2
--                                    different LEs for a property the Water customer should take preference. New view SAP_PROPERTY_CUSTOMER_V.
-- V 0.07      16/08/2016  K.Burton   Defect 124 - SIC codes need to be 5 characters for SAP - left pad with leading 0's where required
-- V 0.08      15/09/2016  K.Burton   CR_017 - reinstate changes for sensitive customer data from SAP_SENSITIVE and new lookup LU_SAP_SENSITIVITY_CODES
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_BP';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_keys_written                NUMBER;
  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_written              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  
  l_rec_written                 BOOLEAN;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  
  l_count                       NUMBER;
  l_delimiter                   VARCHAR2(1) := '|';
--  l_delimiter                   VARCHAR2(1) := chr(9); 
  l_filepath VARCHAR2(30) := 'SAPDELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100); 
  l_filehandle UTL_FILE.FILE_TYPE;
  l_sql VARCHAR2(2000);
  
  l_timestamp VARCHAR2(20);
  l_prefix VARCHAR2(6) := 'BP_';

  CURSOR cur_tab IS
    SELECT DISTINCT 'BP_' || MC.CUSTOMERNUMBER_PK || '_' || MC.STWPROPERTYNUMBER_PK LEGACYRECNUM,
        COB.SAPFLOCNUMBER,
        MC.CUSTOMERNUMBER_PK,
        MC.STWPROPERTYNUMBER_PK,
        SS.SENSITIVE_REASON, -- V 0.08
        LSC.SAP_IDENTIFICATION_TYPE,-- V 0.08
        MC.CUSTOMERCLASSIFICATION, -- V 0.04 - V 0.08
--        NVL(SS.SENSITIVE_REASON,'NA') CUSTOMERCLASSIFICATION, -- V 0.04
--        DECODE(SS.SENSITIVE,'Y',NVL(SS.SENSITIVE_REASON,'SEMDV'),'NA') CUSTOMERCLASSIFICATION, -- V 0.04
        MC.CUSTOMERNAME,
        MC.CUSTOMERBANNERNAME,
--        MC.STDINDUSTRYCLASSCODE,
        TO_CHAR(MC.STDINDUSTRYCLASSCODE,'00000') STDINDUSTRYCLASSCODE, -- V 0.07
        MC.STDINDUSTRYCLASSCODETYPE
    FROM SAPTRAN.MO_CUSTOMER MC,
         SAP_PROPERTY_CUSTOMER_V SPC, -- V 0.06  
         SAPTRAN.SAP_SENSITIVE SS, -- V 0.04-- V 0.08
         LU_SAP_SENSITIVITY_CODES LSC,-- V 0.08
         SAP_DEL_COB COB
    WHERE MC.STWPROPERTYNUMBER_PK = SPC.STWPROPERTYNUMBER_PK -- V 0.06  
    AND COB.STWPROPERTYNUMBER = MC.STWPROPERTYNUMBER_PK
    AND MC.STWPROPERTYNUMBER_PK = SS.STWPROPERTYNUMBER_PK(+)-- V 0.08
    AND SS.SENSITIVE_REASON = LSC.SENSITIVE_REASON(+)-- V 0.08
    AND MC.CUSTOMERNUMBER_PK = NVL(SPC.WATER_CUSTOMERNUMBER,SPC.SEWERAGE_CUSTOMERNUMBER);
  
  TYPE tab IS TABLE OF cur_tab%ROWTYPE INDEX BY PLS_INTEGER;
  t_tab  tab;
  
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01)
    SELECT DISTINCT 4 COL_COUNT, -- indicates that rows of type INIT will have max 4 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID) -- V 0.04
      LEGACYRECNUM KEY_COL,
      'A INIT' SECTION_ID,  -- work around to get sections is sequence
      CUSTOMERNUMBER_PK COL_01
    FROM SAP_DEL_BP
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03)
    SELECT DISTINCT 6 COL_COUNT, -- indicates that rows of type BUT000 will have max 6 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID) -- V 0.04
      LEGACYRECNUM KEY_COL,
      'B BUT000' SECTION_ID,  -- work around to get sections is sequence
      SAPFLOCNUMBER COL_01,
      CUSTOMERNAME COL_02,
      CUSTOMERBANNERNAME COL_03
    FROM SAP_DEL_BP
    ORDER BY LEGACYRECNUM; 
    
    -- V 0.03
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01)
    SELECT 4 COL_COUNT, -- indicates that rows of type BUT000 will have max 4 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C BUT0ID' SECTION_ID,  -- work around to get sections is sequence
      NVL(SAP_IDENTIFICATION_TYPE,CUSTOMERCLASSIFICATION) COL_01 -- V 0.08
--      CUSTOMERCLASSIFICATION COL_01  -- V 0.04
    FROM SAP_DEL_BP
    ORDER BY LEGACYRECNUM; 
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02)
    SELECT DISTINCT 5 COL_COUNT, -- indicates that rows of type CUT0IS will have max 5 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  -- V 0.04
      LEGACYRECNUM KEY_COL,
      'D BUT0IS' SECTION_ID,  -- work around to get sections is sequence
      STDINDUSTRYCLASSCODE COL_01,
      STDINDUSTRYCLASSCODETYPE COL_02
    FROM SAP_DEL_BP
    ORDER BY LEGACYRECNUM;    

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT,  -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID) -- V 0.04
      LEGACYRECNUM KEY_COL,
      'E ENDE' SECTION_ID   -- work around to get sections is sequence
    FROM SAP_DEL_BP
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
--   l_no_row_dropped_cb := 0;
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
          l_progress := 'insert row into SAP_DEL_BP ';

          INSERT INTO SAP_DEL_BP VALUES t_tab(i);
          
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
  
  --  write the header table recon figures
  l_progress := 'Writing Header Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3900, l_no_row_read,    'BP (Business Partners / Customers) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3910, l_no_row_dropped, 'BP (Business Partners / Customers) dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3920, l_no_row_insert, 'BP (Business Partners / Customers) inserted to SAP_DEL_BP');  

  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_keys_written
  FROM SAP_DEL_BP;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3925, l_keys_written, 'BP (Business Partners / Customers) distinct legacy keys inserted to SAP_DEL_BP');  

   -- reset count
   l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3930, l_keys_written, 'BP (Business Partners / Customers) distinct legacy keys inserted into SAP_DEL_OUTPUT');

  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3935, l_no_row_insert, 'BP (Business Partners / Customers) output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_BP_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''BP\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3940, l_no_row_written, 'BP (Business Partners / Customers) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP78', 3945, l_keys_written, 'BP (Business Partners / Customers) distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_BP',
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
END P_SAP_DEL_BP;
/
exit;