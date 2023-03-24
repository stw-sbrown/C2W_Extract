create or replace
PROCEDURE P_SAP_DEL_REG(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter (Device) Read Delivery
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_SAP_DEL_REG.sql
--
-- Subversion $Revision: 5294 $
--
-- CREATED        : 13/06/2016
--
-- DESCRIPTION    : Procedure to create the SAP REG CREATE upload files
--                  Queries Transform tables and populates tables SAP_DEL_REG
--                  Writes to file SAP_DEL_REG_CREATE_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      13/06/2016  D.Cheung   Initial Draft
-- V 0.02      17/06/2016  D.Cheung   Change output structure main_sub
--                                    Change Date format YYYYMMDD
-- v 0.03      22/06/2016  D.Cheung   Change Output RP to count distinct LEGACYRECNUM
--                                    Add recon points for distinct legacy key counts
-- v 0.04      08/07/2016  D.Cheung   Issues SI-023, SI-024 and SI-025 - removed LEFT JOIN to MO_ELIGIBLE_PREMISES
--                                      Use MASTER_PROPERTY on join to MEP if available 
-- v 0.05      03/08/2016  K.Burton   Defect 162 - changed legacy key criteria in main cursor to
--                                    correctly group and date meter networks
-- v 0.06      31/08/2016  D.Cheung   SI-041 - Fix issue with some non-market meters getting dropped in cursor
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_REG';
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
  CURSOR cur_reg IS  -- v 0.05 - cursor re-write
SELECT DISTINCT * FROM (
  SELECT /*+ full(mep) leading(mm)  */
       DISTINCT
       'REG_' || MAIN.METERREF || '_' || SUB.METERREF LEGACYRECNUM,
       GREATEST(MAIN_MR.INITIALMETERREADDATE,SUB_MR.INITIALMETERREADDATE) TIMESLICEDATE,
       DEV.LEGACYRECNUM DEVLEGACYRECNUM,
       91 AS OPERATIONCODE,
       NVL(MAIN.MASTER_PROPERTY,MAIN.INSTALLEDPROPERTYNUMBER) AS STWPROPERTYNUMBER_PK,  --v0.06
       NVL(MEP.SAPFLOCNUMBER,LU.SAPFLOCNUMBER) AS SAPFLOCNUMBER,                        --v0.06
       MAIN.SAPEQUIPMENT,
       MAIN.METERREF AS STWMETERREF
  FROM SAPTRAN.MO_METER_NETWORK MN,
       SAPTRAN.MO_METER MAIN,
       SAPTRAN.MO_METER SUB,
       SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
       SAPTRAN.MO_METER_READING MAIN_MR,
       SAPTRAN.MO_METER_READING SUB_MR,      
       SAP_DEL_DEV DEV,
       SAPTRAN.LU_SAP_FLOCA LU      --v0.06
  WHERE SUB.METERREF = MN.SUB_METERREF
  AND MAIN.METERREF = MN.MAIN_METERREF
  AND NVL(MAIN.MASTER_PROPERTY,MAIN.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK(+) --v0.06
  AND (MEP.STWPROPERTYNUMBER_PK IS NOT NULL OR MAIN.NONMARKETMETERFLAG = 1) --v0.06
  AND DEV.MANUFACTURER_PK = MN.MAIN_MANUFACTURER_PK
  AND DEV.MANUFACTURERSERIALNUM_PK = MN.MAIN_MANSERIALNUM_PK
  AND MAIN.METERREF = MAIN_MR.METERREF
  AND SUB.METERREF = SUB_MR.METERREF  
  AND NVL(MAIN.MASTER_PROPERTY,MAIN.INSTALLEDPROPERTYNUMBER) = LU.STWPROPERTYNUMBER_PK(+)   --v0.06
  UNION
  SELECT /*+ full(mo) leading(mm)  */
       DISTINCT
       'REG_' || MAIN.METERREF || '_' || SUB.METERREF LEGACYRECNUM,
       GREATEST(MAIN_MR.INITIALMETERREADDATE,SUB_MR.INITIALMETERREADDATE) TIMESLICEDATE,
       DEV.LEGACYRECNUM DEVLEGACYRECNUM,
       92 AS OPERATIONCODE,
       SUB.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
       NVL(MEP.SAPFLOCNUMBER,LU.SAPFLOCNUMBER) AS SAPFLOCNUMBER,                        --v0.06
       SUB.SAPEQUIPMENT,
       SUB.METERREF AS STWMETERREF
  FROM SAPTRAN.MO_METER_NETWORK MN,
       SAPTRAN.MO_METER SUB,
       SAPTRAN.MO_METER MAIN,
       SAPTRAN.MO_ELIGIBLE_PREMISES MEP,
       SAPTRAN.MO_METER_READING MAIN_MR,
       SAPTRAN.MO_METER_READING SUB_MR,
       SAP_DEL_DEV DEV,
       SAPTRAN.LU_SAP_FLOCA LU      --v0.06
  WHERE MAIN.METERREF = MN.MAIN_METERREF
  AND SUB.METERREF = MN.SUB_METERREF
  AND SUB.INSTALLEDPROPERTYNUMBER = MEP.STWPROPERTYNUMBER_PK(+)  --v0.06
  AND (MEP.STWPROPERTYNUMBER_PK IS NOT NULL OR SUB.NONMARKETMETERFLAG = 1) --v0.06
  AND DEV.MANUFACTURER_PK = MN.SUB_MANUFACTURER_PK
  AND DEV.MANUFACTURERSERIALNUM_PK = MN.SUB_MANSERIALNUM_PK
  AND MAIN.METERREF = MAIN_MR.METERREF
  AND SUB.METERREF = SUB_MR.METERREF
  AND SUB.INSTALLEDPROPERTYNUMBER = LU.STWPROPERTYNUMBER_PK(+)      --v0.06
)
ORDER BY LEGACYRECNUM, OPERATIONCODE;  
--    SELECT DISTINCT 
--        LEGACYRECNUM,
--        TIMESLICEDATE,
--        DEVLEGACYRECNUM,
--        OPERATIONCODE,
--        STWPROPERTYNUMBER_PK,
--        SAPFLOCNUMBER,
--        SAPEQUIPMENT,
--        STWMETERREF
--    FROM
--    (SELECT /*+ full(mep) leading(mm)  */
--          DISTINCT
--          --'REG_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),'FLOCA') || '_' || MM.MANUFACTURERSERIALNUM_PK || '_N' LEGACYRECNUM,
--          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
--               THEN 'REG_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.SAPEQUIPMENT || '_E'
--               ELSE 'REG_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.METERREF || '_N'
--          END AS LEGACYRECNUM,
--          MR.INITIALMETERREADDATE AS TIMESLICEDATE,
--          SDD.LEGACYRECNUM  DEVLEGACYRECNUM,
--          91 AS OPERATIONCODE,    --OPCODE FOR MAIN METER
--      --OTHER KEYS
--          MM.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
--          MEP.SAPFLOCNUMBER,
--          MM.SAPEQUIPMENT,
--          MM.METERREF AS STWMETERREF
--    FROM SAPTRAN.MO_METER MM
--    JOIN SAPTRAN.MO_METER_READING MR ON (MM.METERREF = MR.METERREF)
--    JOIN SAPTRAN.MO_METER_NETWORK MN ON MM.METERREF = MN.MAIN_METERREF
--    JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK) 
--    JOIN SAP_DEL_DEV SDD ON (MN.MAIN_MANUFACTURER_PK = SDD.MANUFACTURER_PK
--        AND MN.MAIN_MANSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK)
--    JOIN SAP_DEL_DEV SDD2 ON (MN.SUB_MANUFACTURER_PK = SDD2.MANUFACTURER_PK
--        AND MN.SUB_MANSERIALNUM_PK = SDD2.MANUFACTURERSERIALNUM_PK)
--    --WHERE MM.NONMARKETMETERFLAG = 0
--    )
--    UNION ALL
--    (SELECT /*+ full(mo) leading(mm)  */
--          DISTINCT
--          --'REG_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),'FLOCA') || '_' || MM.MANUFACTURERSERIALNUM_PK || '_N' LEGACYRECNUM,
--          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
--               THEN 'REG_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.SAPEQUIPMENT || '_E'
--               ELSE 'REG_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.METERREF || '_N'
--          END AS LEGACYRECNUM,
--          MR.INITIALMETERREADDATE AS TIMESLICEDATE,
--          SDD.LEGACYRECNUM  DEVLEGACYRECNUM,
--          92 AS OPERATIONCODE,      --OPCODE FOR SUB METER
--      --OTHER KEYS
--          MM.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
--          MEP.SAPFLOCNUMBER,
--          MM.SAPEQUIPMENT,
--          MM.METERREF AS STWMETERREF
--    FROM SAPTRAN.MO_METER MM
--    JOIN SAPTRAN.MO_METER_READING MR ON (MM.METERREF = MR.METERREF)
--    JOIN SAPTRAN.MO_METER_NETWORK MN ON MM.METERREF = MN.MAIN_METERREF
--    JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK) 
--    --LEFT JOIN SAPTRAN.LU_SAP_FLOCA MEP ON MM.INSTALLEDPROPERTYNUMBER = MEP.STWPROPERTYNUMBER_PK
--    JOIN SAP_DEL_DEV SDD ON (MN.SUB_MANUFACTURER_PK = SDD.MANUFACTURER_PK
--        AND MN.SUB_MANSERIALNUM_PK = SDD.MANUFACTURERSERIALNUM_PK)
--    JOIN SAP_DEL_DEV SDD2 ON (MN.MAIN_MANUFACTURER_PK = SDD2.MANUFACTURER_PK
--        AND MN.MAIN_MANSERIALNUM_PK = SDD2.MANUFACTURERSERIALNUM_PK)
--    --LEFT JOIN SAPTRAN.LU_SAP_EQUIPMENT LSE ON MM.METERREF = LSE.STWMETERREF
--    --WHERE MM.NONMARKETMETERFLAG = 0
--    )
--    ORDER BY LEGACYRECNUM, OPERATIONCODE;

  TYPE tab_reg IS TABLE OF cur_reg%ROWTYPE INDEX BY PLS_INTEGER;
  t_reg  tab_reg;

     
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
--*** INSERT RELATIONSHIP REGISTER (CREATE) FILE RECORDS ***
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03)
    SELECT DISTINCT 6 COL_COUNT, -- indicates that rows of type REGREL will have max 6 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A REGREL' SECTION_ID,  -- work around to get sections in sequence
      TO_CHAR(TIMESLICEDATE,'YYYYMMDD') COL_01,
      DEVLEGACYRECNUM COL_02,
      OPERATIONCODE COL_03
    FROM SAP_DEL_REG 
    ORDER BY LEGACYRECNUM, OPERATIONCODE;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT DISTINCT 3 COL_COUNT, -- indicates that rows of type ENDE will have max 2 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'B ENDE' SECTION_ID   -- work around to get sections in sequence
    FROM SAP_DEL_REG
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

  -- POPULATE RELATIONSHIP REGISTER TABLE
  -- start processing all records for range supplied
  OPEN cur_reg;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_reg BULK COLLECT INTO t_reg LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_reg.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
--      l_err.TXT_KEY := t_reg(i).LEGACYRECNUM; 
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_REG ';
          l_seq := l_seq + 1;
          l_err.TXT_KEY := t_reg(i).LEGACYRECNUM;          
          INSERT INTO SAP_DEL_REG VALUES t_reg(i);
          
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
                 CLOSE cur_reg;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

    END LOOP;  -- t_reg
     
    IF t_reg.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP; -- cur_reg

  COMMIT;
  CLOSE cur_reg;  
  
  --  write the RELATIONSHIP REGISTER table recon figures
  l_progress := 'Writing Header Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3850, l_no_row_read, 'RELATIONSHIP REGISTER read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3860, l_no_row_dropped, 'RELATIONSHIP REGISTER  dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3870, l_no_row_insert, 'RELATIONSHIP REGISTER inserted to SAP_DEL_REG');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_REG;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3875, l_no_keys_written, 'RELATIONSHIP REGISTER distinct legacy keys inserted to SAP_DEL_REG');  
  
  -- reset count
  l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_no_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3880, l_no_keys_written, 'RELATIONSHIP REGISTER distinct legacy keys inserted into SAP_DEL_OUTPUT');
  
  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3885, l_no_row_insert, 'RELATIONSHIP REGISTER output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_REG_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''REG\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID,COL_03';
  --l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''%\_REG'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_no_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3890, l_no_row_written, 'RELATIONSHIP REGISTER output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP77', 3895, l_no_keys_written, 'RELATIONSHIP REGISTER distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_REG',
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
END P_SAP_DEL_REG;
/
/
show errors;
exit;