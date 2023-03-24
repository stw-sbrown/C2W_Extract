create or replace
PROCEDURE P_SAP_DEL_SCM(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: SAP Service Component
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_SCM.sql
--
-- Subversion $Revision: 5073 $
--
-- CREATED        : 10/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP SCM upload file
--                  Queries Transform tables and populates table SAP_DEL_SCM
--                  Writes to file SAP_DEL_SCM_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      10/06/2016  K.Burton   Initial Draft
-- V 0.02      22/06/2016  K.Burton   Added additional reconciliation measures to count legacy keys
-- V 0.03      12/07/2016  K.Burton   CR_009 - Removed CALD_TXT
-- V 0.04      14/07/2016  K.Burton   CR_016 - Added VOLUME_LIMIT
-- V 0.05      20/07/2016  K.Burton   CR_020 - Changed TECOMPONENTTYPE for DPID_TYPE and pulled data from
--                                    new lookup SAP_DPID_TYPE
-- V 0.06      01/08/2016  K.Burton   Defect 150 - restricted main cursor to only include SPIDs which are included
--                                    in SAP_DEL_POD. Previously it only matched on property number which meant some
--                                    cross border data was included
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_SCM';
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
  l_keys_written                NUMBER;
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
  l_prefix VARCHAR2(6) := 'SCM_';

  CURSOR cur_tab IS
    SELECT /*+ full(msp) leading(msp,sdp,prm)  */
       l_prefix || NVL(SDP.SAPFLOCNUMBER,SDP.STWPROPERTYNUMBER) || '_' || MSP.SPID_PK || MSC.SERVICECOMPONENTTYPE LEGACYRECNUM,
       PRM.LEGACYRECNUM PARENTLEGACYRECNUM,
       MSC.SERVICECOMPONENTTYPE,
       MSC.EFFECTIVEFROMDATE,
       NULL DPID_PK,  -- don't need this for the SCM header file
       NULL NO_IWCS,
--       NULL TECOMPONENTTYPE, -- V 0.05
       NULL DPID_TYPE, -- V 0.05
       NULL NO_SAMPLE_POINT,
       NULL CONSENT_NO,
       NULL VOLUME_LIMIT, -- V 0.04
--       NULL CALD_TXT, -- V 0.03
       NULL MAININST,
       MSP.SPID_PK,
       SDP.SAPFLOCNUMBER,
       SDP.STWPROPERTYNUMBER
    FROM SAPTRAN.MO_SERVICE_COMPONENT MSC,
         SAPTRAN.MO_SUPPLY_POINT MSP,
         SAP_DEL_POD SDP,
         SAP_DEL_PREM PRM
    WHERE MSP.STWPROPERTYNUMBER_PK = SDP.STWPROPERTYNUMBER
    AND SDP.STWPROPERTYNUMBER = PRM.STWPROPERTYNUMBER
    AND MSP.SPID_PK = MSC.SPID_PK
    AND SDP.SPID_PK = MSP.SPID_PK
    UNION
    SELECT /*+ full(msp) leading(msp,sdp,prm)  */
       l_prefix || NVL(SDP.SAPFLOCNUMBER,SDP.STWPROPERTYNUMBER) || '_' || MSP.CORESPID_PK || '_' || MDP.SERVICECOMPTYPE LEGACYRECNUM,
       PRM.LEGACYRECNUM PARENTLEGACYRECNUM,
       MDP.SERVICECOMPTYPE SERVICECOMPONENTTYPE,
       NULL EFFECTIVEFROMDATE, -- MDP.DPEFFECTFROMDATE EFFECTIVEFROMDATE,
       NULL DPID_PK,  -- don't need this for the SCM header file
       NULL NO_IWCS, --MDP.NO_IWCS,
--       NULL TECOMPONENTTYPE, --MDP.SERVICECOMPTYPE TECOMPONENTTYPE, -- V 0.05
       NULL DPID_TYPE, -- V 0.05
       NULL NO_SAMPLE_POINT, --MDP.NO_SAMPLE_POINT,
       NULL CONSENT_NO, --MDP.CONSENT_NO,
       NULL VOLUME_LIMIT, -- V 0.04
--       NULL CALD_TXT, -- V 0.03
       NULL MAININST,
       MDP.SPID_PK,
       SDP.SAPFLOCNUMBER,
       SDP.STWPROPERTYNUMBER
    FROM SAPTRAN.MO_DISCHARGE_POINT MDP,
         SAPTRAN.MO_SUPPLY_POINT MSP,
         SAP_DEL_POD SDP,
         SAP_DEL_PREM PRM
    WHERE MSP.STWPROPERTYNUMBER_PK = SDP.STWPROPERTYNUMBER
    AND SDP.STWPROPERTYNUMBER = PRM.STWPROPERTYNUMBER
    AND MSP.SPID_PK = MDP.SPID_PK
    AND SDP.SPID_PK = MSP.SPID_PK;
  
  TYPE tab IS TABLE OF cur_tab%ROWTYPE INDEX BY PLS_INTEGER;
  t_tab  tab;
  
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09)
    SELECT 12 COL_COUNT, -- indicates that rows of type DATA will have max 12 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A DATA' SECTION_ID,  -- work around to get sections is sequence
      PARENTLEGACYRECNUM COL_01,
      SERVICECOMPONENTTYPE COL_02,
      DPID_PK COL_03, 
--      TECOMPONENTTYPE COL_04, -- V 0.05
      DPID_TYPE COL_O4, -- V 0.05
      NO_SAMPLE_POINT COL_05,
      CONSENT_NO COL_06,
      VOLUME_LIMIT COL_07, -- V 0.04
--      CALD_TXT COL_08, -- V 0.03
      TO_CHAR(EFFECTIVEFROMDATE,'YYYYMMDD') COL_08, -- SI-008
      MAININST COL_09
    FROM SAP_DEL_SCM
    ORDER BY LEGACYRECNUM;

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01)
    SELECT 4 COL_COUNT, -- indicates that rows of type POD will have max 4 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B POD' SECTION_ID,  -- work around to get sections is sequence
      SPID_PK COL_01
    FROM SAP_DEL_SCM
    ORDER BY LEGACYRECNUM;

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT 3 COL_COUNT,  -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C ENDE' SECTION_ID   -- work around to get sections is sequence
    FROM SAP_DEL_SCM
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
          l_progress := 'insert row into SAP_DEL_SCM ';

          INSERT INTO SAP_DEL_SCM VALUES t_tab(i);
          
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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3650, l_no_row_read,    'SCM (Service Components) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3660, l_no_row_dropped, 'SCM (Service Components) dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3670, l_no_row_insert, 'SCM (Service Components) inserted to SAP_DEL_SCM');  

  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_keys_written
  FROM SAP_DEL_SCM;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3675, l_keys_written, 'SCM (Service Components) distinct legacy keys inserted to SAP_DEL_SCM');  

   -- reset count
   l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3680, l_keys_written, 'SCM (Service Components) distinct legacy keys inserted into SAP_DEL_OUTPUT');

  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3685, l_no_row_insert, 'SCM (Service Components) output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_SCM_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''SCM\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3690, l_no_row_written, 'SCM (Service Components) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP73', 3695, l_keys_written, 'SCM (Service Components) distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_SCM',
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
END P_SAP_DEL_SCM;
/
exit;