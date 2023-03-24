create or replace
PROCEDURE P_SAP_DEL_DEV(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                        no_job IN MIG_JOBREF.NO_JOB%TYPE,
                        return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter (Device) Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_DEV.sql
--
-- Subversion $Revision: 5229 $
--
-- CREATED        : 23/05/2016
--
-- DESCRIPTION    : Procedure to create the SAP DEV upload file
--                  Queries Transform tables and populates tables SAP_DEL_DEV and SAP_DEL_DEVMO
--                  Writes to files SAP_DEL_DEV_<date/timestamp>.dat, SAP_DEL_DEVMO_<date/timestamp>.dat
--                      SAP_DEL_DEC_<date/timestamp>.dat and SAP_DEL_DVLCHG_<date/timestamp>.dat and SAP_DEL_DVLCRT_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      23/05/2016  K.Burton   Initial Draft
-- V 0.02      25/05/2016  K.Burton   Added DEVMO child table and changes to file write queries
--                                    for performance issues
-- V 0.03      02/06/2016  D.Cheung   Added DEVCHAR child table for Meter Characteristics
--                                    Added DVLCHG child table for Meter Location changes
--                                    Do NOT write out details for NON-MARKET METERS
-- V 0.04      07/06/2016  D.Cheung   Rebuild table structures
--                                        SAP_DEL_DEV (header, characteristics and location)
--                                        SAP_DEL_DEVMO (MOSL Update)
-- V 0.05      08/06/2016  D.Cheung   Correct foreign key link to SAP_DEL_COB
-- V 1.01      09/06/2016  D.Cheung   Rebuild to split into seprate procs and tables per file
-- V 1.02      10/06/2016  D.Cheung   Relink to get SAPEQUIPMENT from MO_METER directly - post SAPTRAN rebuild
-- V 1.03      14/06/2016  D.Cheung   Add Control Points and Reconciliation Points
-- v 1.04      20/06/2016  D.Cheung   Add non-market meters to main cursor (for networks), exclude on output to file
-- v 1.05      22/06/2016  D.Cheung   Change Output RP to count distinct LEGACYRECNUM
--                                    Add recon points for distinct legacy key counts
-- v 1.06      29/06/2016  K.Burton   CH005 - Added D3004 to output
-- v 1.07      08/07/2016  K.Burton   Issues SI-023, SI-024 and SI-025 - removed LEFT JOIN to MO_ELIGIBLE_PREMISES
--                         D.Cheung       Use MASTER_PROPERTY on join to MEP if available   
-- v 1.08      12/07/2016  D.Cheung   CR_007 - Add Meter_Model field
-- v 1.09      24/08/2016  D.Cheung   SI-040 - Fix issue with some non-market meters getting dropped in cursor
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_SAP_DEL_DEV';
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
    SELECT /*+ full(ma) index(sa,pk_04_sa_address)leading(mm,ma,mep) */
        DISTINCT
          --'DEV_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),'FLOCA') || '_' || MM.MANUFACTURERSERIALNUM_PK || '_N' LEGACYRECNUM,
          CASE WHEN MM.SAPEQUIPMENT IS NOT NULL
               THEN 'DEV_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.SAPEQUIPMENT || '_E'
               ELSE 'DEV_' || NVL(TO_CHAR(MEP.SAPFLOCNUMBER),TO_CHAR(MM.INSTALLEDPROPERTYNUMBER)) || '_' || MM.METERREF || '_N'
          END AS LEGACYRECNUM,
          MM.METERTREATMENT,
          MM.MANUFACTURER_PK,
          MM.MANUFACTURERSERIALNUM_PK,
          MM.METER_MODEL MANUFACTURERMODEL,
          MM.NUMBEROFDIGITS, -- V 1.06
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
      --LOCATION
          SA.CITY,
          SA.DISTRICT,
          SA.POSTCODE,
          SA.STREET,
          SA.HOUSENUMBER,
          SA.STREET2,
          SA.STREET3,
          SA.STREET4,
          SA.PAFADDRESSKEY,
          SA.LOCATIONFREETEXTDESCRIPTOR,
          SA.POBOX,
          SA.STREET5,
          SA.COUNTRY,
          SA.UPRNREASONCODE,
          MM.GPSX,
          MM.GPSY,
          SA.UPRN,
      --MO
          --MS.EFFECTIVEFROMDATE,       --TO CHANGE
          --MM.YEARLYVOLESTIMATE,
          --MM.WATERCHARGEMETERSIZE,
          --MM.SEWCHARGEABLEMETERSIZE,
          --MM.RETURNTOSEWER,
          MM.INSTALLEDPROPERTYNUMBER AS STWPROPERTYNUMBER_PK,
          MEP.SAPFLOCNUMBER,
          MM.SAPEQUIPMENT,
          MM.METERREF AS STWMETERREF,
          MM.NONMARKETMETERFLAG       -- v 1.04
    FROM SAPTRAN.MO_METER MM
    JOIN SAPTRAN.SAP_METER_ADDRESS MA ON (MM.MANUFACTURER_PK = MA.MANUFACTURER_PK
        AND MM.MANUFACTURERSERIALNUM_PK = MA.METERSERIALNUMBER_PK)
    JOIN SAPTRAN.SAP_ADDRESS SA ON MA.ADDRESS_PK = SA.ADDRESS_PK
    LEFT JOIN SAPTRAN.MO_ELIGIBLE_PREMISES MEP ON (NVL(MM.MASTER_PROPERTY,MM.INSTALLEDPROPERTYNUMBER) = MEP.STWPROPERTYNUMBER_PK) -- v 1.07 v1.09
    --WHERE MM.NONMARKETMETERFLAG = 0   -- v 1.04
    WHERE (MEP.STWPROPERTYNUMBER_PK IS NOT NULL OR MM.NONMARKETMETERFLAG = 1) --v1.09
    ORDER BY MM.METERREF;
    
  TYPE tab_dev IS TABLE OF cur_dev%ROWTYPE INDEX BY PLS_INTEGER;
  t_dev  tab_dev;
      
  -- Procedure to fill the generic output table with formatted data
  FUNCTION PopulateOutputTable RETURN NUMBER AS
    l_row_count NUMBER;
  BEGIN
--*** INSERT DEV (CREATE) FILE RECORDS ***
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID,COL_01,COL_02,COL_03,COL_04,COL_05)  -- V 1.06
    SELECT 8 COL_COUNT, -- indicates that rows of type EQUI will have max 8 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)
      LEGACYRECNUM KEY_COL,
      'A EQUI' SECTION_ID,  -- work around to get sections in sequence
      METERTREATMENT COL_01,
      MANUFACTURER_PK COL_02,
      MANUFACTURERSERIALNUM_PK COL_03,
      MANUFACTURERMODEL COL_04,
      NUMBEROFDIGITS COL_05  -- V 1.06
    FROM SAP_DEL_DEV
    WHERE SAPEQUIPMENT IS NULL
    AND NONMARKETMETERFLAG = 0    -- v 1.04
    ORDER BY LEGACYRECNUM;
    
    INSERT INTO SAP_DEL_OUTPUT (COL_COUNT,KEY_COL,SECTION_ID)
    SELECT 3 COL_COUNT, -- indicates that rows of type ENDE will have max 2 cols populated from data (includes COL_COUNT,KEY_COL and SECTION_ID)  
      LEGACYRECNUM KEY_COL,
      'B ENDE' SECTION_ID   -- work around to get sections in sequence
    FROM SAP_DEL_DEV
    WHERE SAPEQUIPMENT IS NULL
    AND NONMARKETMETERFLAG = 0    -- v 1.04
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
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into SAP_DEL_DEV ';
          l_seq := l_seq + 1;
          l_err.TXT_KEY := t_dev(i).LEGACYRECNUM;          
          INSERT INTO SAP_DEL_DEV VALUES t_dev(i);
          
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
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3150, l_no_row_read, 'DEV Header (Meter) read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3160, l_no_row_dropped, 'DEV Header (Meter) dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3170, l_no_row_insert, 'DEV Header (Meter) inserted to SAP_DEL_DEV');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_DEV;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3175, l_no_keys_written, 'DEV Header distinct legacy keys inserted to SAP_DEL_DEV');  
  
  SELECT COUNT(DISTINCT LEGACYRECNUM) 
  INTO l_no_keys_written
  FROM SAP_DEL_DEV
  WHERE SAPEQUIPMENT IS NULL
    AND NONMARKETMETERFLAG = 0;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3176, l_no_keys_written, 'DEV (NEW-MARKET) distinct legacy keys inserted to SAP_DEL_DEV');  

  -- reset count
  l_no_row_insert := 0;
   
  -- Populate the output temporary table
  l_no_keys_written := PopulateOutputTable;
  
  --  write the output table recon figures
  l_progress := 'Writing Output Table Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3180, l_no_keys_written, 'DEV Header distinct legacy keys inserted into SAP_DEL_OUTPUT');
  
  SELECT COUNT(*) 
  INTO l_no_row_insert
  FROM SAP_DEL_OUTPUT;
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3185, l_no_row_insert, 'DEV Header output rows inserted into SAP_DEL_OUTPUT');

    -- NOW WRITE THE FILES
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_DEL_DEV_' || l_timestamp || '.dat';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''DEV\_%'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  --l_sql := 'SELECT * FROM SAP_DEL_OUTPUT WHERE KEY_COL LIKE ''%\_DEV'' ESCAPE ''\'' ORDER BY KEY_COL,SECTION_ID';
  P_SAP_DEL_UTIL_WRITE_FILE(l_sql,l_filehandle,l_delimiter,l_no_keys_written,l_no_row_written);
--  l_no_row_written := l_no_row_written + l_rows_written; 
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  --  write the output file recon figures
  l_progress := 'Writing Output File Counts';
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3190, l_no_row_written, 'DEV Header (Meter) output records written to file'); 
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP63', 3195, l_no_keys_written, 'DEV Header distinct legacy keys written to file'); 

  -- archive the latest batch
  P_SAP_DEL_UTIL_ARCHIVE_TABLE(p_tablename => 'SAP_DEL_DEV',
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
END P_SAP_DEL_DEV;
/
/
show errors;
exit;