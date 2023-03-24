create or replace
PROCEDURE P_SAP_DEL_COB(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Connection Object Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_COB.sql
--
-- Subversion $Revision: 5377 $
--
-- CREATED        : 27/05/2016
--
-- DESCRIPTION    : Procedure to create the SAP COB upload file
--                  Queries Transform tables and populates tables SAP_DEL_COB and SAP_DEL_COBMO
--                  Writes to file SAP_DEL_COB_<date/timestamp>.dat and SAP_DEL_COBMO_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      27/05/2016  K.Burton   Initial Draft
-- V 0.02      08/06/2016  K.Burton   Changes to cursors and legacy keys 
-- V 1.00      09/06/2016  K.Burton   Split out P_SAP_DEL_COB_MO and P_SAP_DEL_PREM
-- V 1.01      16/06/2016  K.Burton   Changes to address field mappings for FL_ADR element - CR003
-- V 1.02      22/06/2016  K.Burton   Added additional reconciliation measures to count legacy keys
-- V 1.03      23/06/2016  K.Burton   Removed GPSX and GPSY from main cursor - Issue SI-014
-- V 1.04      15/07/2016  K.Burton   CR_018 - Added TREATMENTWORKS column
-- V 1.05      18/07/2016  K.Burton   CR_018 - Changed join to SAP_SEWERAGE_TREATMENT_WORKS to outer join
-- V 1.06      05/09/2016  K.Burton   Remove condition to exclude missing SAPFLOCNUMBER - instead 
--                                    these are trapped and reported in recon measures
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_COB';
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
  l_no_row_dropped_cb           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- count of cross border rows NOT output to any file
  
  l_count                       NUMBER;
  l_delimiter                   VARCHAR2(1) := '|';
--  l_delimiter                   VARCHAR2(1) := chr(9); 
  l_filepath VARCHAR2(30) := 'SAPDELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100); 
  l_filehandle UTL_FILE.FILE_TYPE;
  l_sql VARCHAR2(2000);
  
  l_timestamp VARCHAR2(20);
  l_prefix VARCHAR2(6) := 'COB_';
  
  CURSOR cur_tab IS  --changed for CR003
    SELECT DISTINCT * FROM (
    SELECT /*+ full(spd) index(msp,pk_spid)leading(mep,sdp,msp) */
            l_prefix || NVL(MEP.SAPFLOCNUMBER,MEP.STWPROPERTYNUMBER_PK) LEGACYRECNUM,
            MEP.SAPFLOCNUMBER PARENTLEGACYRECNUM,  
            MEP.SAPFLOCNUMBER,
            MEP.STWPROPERTYNUMBER_PK,
            MEP.UPRNREASONCODE, --D2040
            MSP.PAIRINGREFREASONCODE, --D2086
            SA.CITY,   -- D5008
            SA.DISTRICT,  -- D5007
            SA.POSTCODE, -- D5009
            SA.STREET, -- D5004
            SA.HOUSENUMBER,  -- D5004
            SA.STREET2, -- D5002     
            SA.STREET3, -- D5003
            SA.STREET4, -- D5005
            SA.PAFADDRESSKEY,  -- D5011
            SA.STREET5,  -- D5006    
            SA.COUNTRY, -- D5010
            SA.UPRN, -- D2039   
            NULL GPSCOORDINATES, -- V 1.03 - placeholder for SAP data
--            MM.GPSX, -- D3017 -- V 1.03
--            MM.GPSY -- D3018 -- V 1.03
            STW.STWWORKSFLOC TREATMENTWORKS --V 1.04
    FROM SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
         SAPTRAN.SAP_ADDRESS SA,
         SAPTRAN.SAP_PROPERTY_ADDRESS SPA,
--         SAPTRAN.MO_METER MM, -- V 1.03
         SAPTRAN.MO_SUPPLY_POINT MSP,
         SAPTRAN.SAP_SEWERAGE_TREATMENT_WORKS STW --, -- V 1.04 / V 1.05
--         SAP_DEL_POD SDP
    WHERE --SDP.STWPROPERTYNUMBER = MEP.STWPROPERTYNUMBER_PK
--    AND 
    MEP.STWPROPERTYNUMBER_PK = SPA.STWPROPERTYNUMBER_PK
    AND SPA.ADDRESS_PK = SA.ADDRESS_PK
--    AND SDP.SPID_PK = MM.SPID_PK -- V 1.03
    AND MEP.STWPROPERTYNUMBER_PK = STW.STWPROPERTYNUMBER_PK(+) -- V 1.04  / V 1.05
    AND MSP.STWPROPERTYNUMBER_PK = MEP.STWPROPERTYNUMBER_PK
    AND MSP.WHOLESALERID_PK = 'SEVERN-W'
--    AND MSP.SPID_PK = SDP.SPID_PK
--    AND MEP.SAPFLOCNUMBER IS NOT NULL -- V 1.06
--    ORDER BY SDP.SPID_PK);
    ORDER BY MEP.STWPROPERTYNUMBER_PK);
    
  TYPE tab IS TABLE OF cur_tab%ROWTYPE INDEX BY PLS_INTEGER;
  t_tab  tab;
  
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01)
    SELECT 4 COL_COUNT, -- indicates that rows of type INIT will have max 4 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,  
      'A INIT' SECTION_ID,  -- work around to get sections is sequence
      PARENTLEGACYRECNUM COL_01 
    FROM SAP_DEL_COB
    ORDER BY LEGACYRECNUM;

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03)
    SELECT 6 COL_COUNT, -- indicates that rows of type FL_DAT will have max 6 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'B FL_DAT' SECTION_ID,  -- work around to get sections is sequence
      UPRNREASONCODE COL_01,
      PAIRINGREFREASONCODE COL_02,
      TREATMENTWORKS COL_03
    FROM SAP_DEL_COB
    ORDER BY LEGACYRECNUM;
  
    -- changed for CR003
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05,COL_06,COL_07,COL_08,COL_09,COL_10,COL_11,COL_12)
    SELECT 15 COL_COUNT,  -- indicates that rows of type FL_ADR will have max 15 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'C FL_ADR' SECTION_ID, -- work around to get sections is sequence
      CITY COL_01,
      DISTRICT COL_02,
      POSTCODE COL_03,
      STREET COL_04,
      HOUSENUMBER COL_05,
      STREET2 COL_06,
      STREET3 COL_07,
      STREET4 COL_08,
      STREET5 COL_09,
      PAFADDRESSKEY COL_10,
--      TRIM(TO_CHAR(GPSX,'999999.9')) || '\' || TRIM(TO_CHAR(GPSY,'999999.9')) COL_11,  -- V 1.03
      GPSCOORDINATES COL_11,
      UPRN COL_12
    FROM SAP_DEL_COB
    ORDER BY LEGACYRECNUM;

    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT 3 COL_COUNT,  -- indicates that rows of type ENDE will have max 3 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'D ENDE' SECTION_ID   -- work around to get sections is sequence
    FROM SAP_DEL_COB
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
          l_progress := 'insert row into SAP_DEL_COB ';

          INSERT INTO SAP_DEL_COB VALUES t_tab(i);
          
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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3500, l_no_row_read,    'COB (Connection Objects) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3510, l_no_row_dropped, 'COB (Connection Objects) dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3520, l_no_row_insert, 'COB (Connection Objects) inserted to SAP_DEL_COB');  

  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_keys_written
  FROM SAP_DEL_COB;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3525, l_keys_written, 'COB (Connection Objects) distinct legacy keys inserted to SAP_DEL_COB');  
  
   -- reset count
   l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3530, l_keys_written, 'COB (Connection Objects) distinct legacy keys inserted into SAP_DEL_OUTPUT');

  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3535, l_no_row_insert, 'COB (Connection Objects) output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_COB_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''COB\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3540, l_no_row_written, 'COB (Connection Objects) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP70', 3545, l_keys_written, 'COB (Connection Objects) distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_COB',
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
END P_SAP_DEL_COB;
/
exit;