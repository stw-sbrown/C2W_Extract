create or replace
PROCEDURE P_SAP_DEL_POD(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: SAP Point of Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_POD.sql
--
-- Subversion $Revision: 6026 $
--
-- CREATED        : 18/05/2016
--
-- DESCRIPTION    : Procedure to create the SAP POD upload file
--                  Queries Transform tables and populates table SAP_DEL_POD
--                  Writes to file SAP_DEL_POD_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      18/05/2016  K.Burton   Initial Draft
-- V 0.02      24/05/2016  K.Burton   Updates to split output to 2 files and to accomodate
--                                    NULL values within the data items being output
-- V 0.03      25/05/2016  K.Burton   Changes to file write process for performance issues
-- V 0.04      02/06/2016  K.Burton   Alterations to LEGACYRECNUM field following spec finalization
-- V 0.05      03/06/2016  K.Burton   First draft of SAP_DEL_PODSRV code added
-- V 1.00      07/06/2016  K.Burton   Completed updates to include key generation from FLOC and SPID
--                                    and finalized file formats
-- V 2.00      08/06/2016  K.Burton   Split code out into separated files for POD, PODMO and PODSRV
-- V 2.01      22/06/2016  K.Burton   Added additional reconciliation measures to count legacy keys
-- V 2.02      01/08/2016  K.Burton   Corrected logic to exclude crossborder SPIDs
-- V 2.03      18/08/2016  D.Cheung   SI_032 - Issue with OTHERWHOLESALER filters dropping supply points incorrectly
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_POD';
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
  l_prefix VARCHAR2(6) := 'POD_';

  CURSOR cur_tab IS
    SELECT l_prefix || NVL(MEP.SAPFLOCNUMBER,MSP.STWPROPERTYNUMBER_PK) || '_' || MSP.SPID_PK LEGACYRECNUM,
           MEP.SAPFLOCNUMBER,
           MSP.SPID_PK,
           MSP.STWPROPERTYNUMBER_PK,
           MSP.SERVICECATEGORY,
           MSP.SUPPLYPOINTEFFECTIVEFROMDATE,
           MSP.NEWCONNECTIONTYPE,
           MSP.ACCREDITEDENTITYFLAG,
           MSP.GAPSITEALLOCATIONMETHOD,
           MSP.OTHERSERVICECATPROVIDED,
           MSP.OTHERSERVICECATPROVIDEDREASON,
           MSP.VOLTRANSFERFLAG,
           0 INTERIMDUTYSUPPLYPOINT,
           MSP.SPIDSTATUS,
           MSP.LATEREGAPPLICATION,
           MSP.OTHERSPID
    FROM SAPTRAN.MO_SUPPLY_POINT MSP,
         SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
         SAPTRAN.SAP_PROPERTY_ADDRESS SPA,
         SAPTRAN.SAP_ADDRESS SA
    WHERE MSP.STWPROPERTYNUMBER_PK = MEP.STWPROPERTYNUMBER_PK
    AND MSP.WHOLESALERID_PK = 'SEVERN-W'
--    AND MSP.OTHERWHOLESALERID IS NOT NULL   --v2.03
--    AND MSP.OTHERWHOLESALERID = 'SEVERN-W'  --v2.03
    AND SPA.STWPROPERTYNUMBER_PK = MSP.STWPROPERTYNUMBER_PK
    AND SPA.ADDRESS_PK = SA.ADDRESS_PK
    ORDER BY MSP.SPID_PK;
  
  TYPE tab IS TABLE OF cur_tab%ROWTYPE INDEX BY PLS_INTEGER;
  t_tab  tab;
  
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09,COL_10,COL_11,COL_12)
    SELECT 15 COL_COUNT, -- indicates that rows of type UIHEAD will have max 15 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A UIHEAD' SECTION_ID,  -- work around to get sections is sequence
      SERVICECATEGORY COL_01,
      TO_CHAR(SUPPLYPOINTEFFECTIVEFROMDATE,'YYYYMMDD') COL_02,
      NEWCONNECTIONTYPE COL_03,
      ACCREDITEDENTITYFLAG COL_04,
      GAPSITEALLOCATIONMETHOD COL_05,
      OTHERSERVICECATPROVIDED COL_06,
      OTHERSERVICECATPROVIDEDREASON COL_07,
      VOLTRANSFERFLAG COL_08,
      INTERIMDUTYSUPPLYPOINT COL_09,
      SPIDSTATUS COL_10,
      LATEREGAPPLICATION COL_11,
      OTHERSPID COL_12
    FROM SAP_DEL_POD
    ORDER BY LEGACYRECNUM;
  
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02)
    SELECT 5 COL_COUNT,  -- indicates that rows of type UIEXT will have max 5 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B UIEXT' SECTION_ID, -- work around to get sections is sequence
      TO_CHAR(SUPPLYPOINTEFFECTIVEFROMDATE,'YYYYMMDD') COL_01,
      SPID_PK COL_02
    FROM SAP_DEL_POD
    ORDER BY LEGACYRECNUM;
  
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT 3 COL_COUNT,  -- indicates that rows of type UIGRID will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C UIGRID' SECTION_ID -- work around to get sections is sequence
    FROM SAP_DEL_POD
    ORDER BY LEGACYRECNUM;

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT 3 COL_COUNT,  -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'D ENDE' SECTION_ID   -- work around to get sections is sequence
    FROM SAP_DEL_POD
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
          l_progress := 'insert row into SAP_DEL_POD ';

          INSERT INTO SAP_DEL_POD VALUES t_tab(i);
          
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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3000, l_no_row_read,    'POD (Supply Points) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3010, l_no_row_dropped, 'POD (Supply Points) dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3020, l_no_row_insert, 'POD (Supply Points) rows inserted to SAP_DEL_POD');  

  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_keys_written
  FROM SAP_DEL_POD;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3025, l_keys_written, 'POD (Supply Points) distinct legacy keys inserted to SAP_DEL_POD');  

   -- reset count
   l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3030, l_keys_written, 'POD (Supply Points) distinct legacy keys  into SAP_DEL_OUTPUT');

  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3035, l_no_row_insert, 'POD (Supply Points) output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_POD_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''POD\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3040, l_no_row_written, 'POD (Supply Points) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP60', 3045, l_keys_written, 'POD (Supply Points) distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_POD',
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
END P_SAP_DEL_POD;
/
exit;