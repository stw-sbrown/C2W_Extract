create or replace
PROCEDURE P_MOU_DEL_METER_DISCHARGE  (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                               no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                               return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Meter Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_MOU_DEL_METER_DISCHARGE.sql
--
-- CREATED        : 12/04/2016
--
-- DESCRIPTION    : Procedure to create the Supply Point MOSL Upload file
--                  Queries Transform tables and populates table DEL_METER_TO_DISCHARGE_POINT
--                  Writes to file METER_DISCHARGE_POINT_SEVERN-W_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      12/04/2016  K.Burton   Initial Draft
-- V 0.02      26/04/2016  K.Burton   Changed call to ArchiveDeliveryTable procedure
--                                    now calls P_DEL_UTIL_ARCHIVE_TABLE stored procedure
-- V 1.01      16/05/2016  K.Burton   Changes to file write logic to accomodate cross border
--                                    files
-- V 1.02      18/05/2016  K.Burton   Changes to MANUFACTURER_PK to remove spaces from names as per
--                                    MOSL upload file feedback
-- V 1.03      25/08/2016  S.Badhan   I-320. If user FINDEL use directory FINEXPORT.
-- V 1.04      01/09/2016  K.Burton   Updates for splitting STW data into 3 batches
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_DEL_METER_DISCHARGE';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_written              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  l_no_row_dropped_cb           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- count of cross border rows NOT output to any file
  l_count                       NUMBER;

  l_filepath VARCHAR2(30) := 'DELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100) := 'DEL_METER_DISCHARGE_POINT';

  l_sql VARCHAR2(2000);
--  fHandle UTL_FILE.FILE_TYPE;
  
  -- Cross Border Control cursor
  CURSOR cb_cur IS
    SELECT WHOLESALER_ID, RUN_FLAG
    FROM BT_CROSSBORDER_CTRL;
--    WHERE RUN_FLAG = 1;

  CURSOR cur_meter_dp IS
    SELECT MANUFACTURERSERIALNUM_PK,
           MANUFACTURER_PK,
           DPID_PK,
           PERCENTAGEDISCHARGE
    FROM (
    SELECT MD.MANUFACTURERSERIALNUM_PK,
           MD.MANUFACTURER_PK MANUFACTURER,
           UPPER(REPLACE(MD.MANUFACTURER_PK,' ','')) MANUFACTURER_PK, -- V 1.02
           MD.DPID_PK,
           MD.PERCENTAGEDISCHARGE
    FROM MOUTRAN.MO_METER_DPIDXREF MD,
         DEL_DISCHARGE_POINT DP
    WHERE MD.DPID_PK = DP.DPID_PK);

  TYPE tab_meter_dp IS TABLE OF cur_meter_dp%ROWTYPE INDEX BY PLS_INTEGER;
  t_meter_dp  tab_meter_dp;

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
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_job.IND_STATUS := 'RUN';

   IF USER = 'FINDEL' THEN
      l_filepath := 'FINEXPORT';
   END IF;

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
--   l_filename := 'METER_DISCHARGE_POINT_SEVERN-W_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;


  -- start processing all records for range supplied
--  fHandle := UTL_FILE.FOPEN('DELEXPORT', l_filename, 'w');
  OPEN cur_meter_dp;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_meter_dp BULK COLLECT INTO t_meter_dp LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_meter_dp.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
      l_err.TXT_KEY := t_meter_dp(i).DPID_PK;
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into DEL_METER_DISCHARGE_POINT';
          INSERT INTO DEL_METER_DISCHARGE_POINT VALUES t_meter_dp(i);

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
                 CLOSE cur_meter_dp;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
              END IF;
        END;

        -- keep count of records written
        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
        END IF;
    END LOOP;

-- NOW WRITE THE FILE
   FOR w IN cb_cur
   LOOP
    CASE w.WHOLESALER_ID 
      WHEN 'ANGLIAN-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_ANW_V';
      WHEN 'DWRCYMRU-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_WEL_V';
      WHEN 'SEVERN-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_STW_V';
      WHEN 'SEVERN-A' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_STWA_V';
      WHEN 'SEVERN-B' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_STWB_V';
      WHEN 'THAMES-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_THW_V';
      WHEN 'WESSEX-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_WEW_V';
      WHEN 'YORKSHIRE-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_YOW_V';
      WHEN 'UNITED-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_UUW_V';
      WHEN 'SOUTHSTAFF-W' THEN
        l_sql := 'SELECT * FROM DEL_METER_DISCHARGE_SSW_V';
    END CASE;
    IF w.RUN_FLAG = 1 THEN
      l_filename := 'METER_DISCHARGE_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
      P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);
      l_no_row_written := l_no_row_written + l_rows_written; -- add rows written to total

      IF w.WHOLESALER_ID NOT LIKE 'SEVERN%' THEN
        l_filename := 'OWC_METER_DISCHARGE_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);
      END IF;
    ELSE
      l_sql := 'SELECT COUNT(*) FROM DEL_METER_DISCHARGE_POINT MDP, DEL_DISCHARGE_POINT DP, DEL_SUPPLY_POINT SP WHERE MDP.DPID_PK = DP.DPID_PK AND DP.SPID_PK = SP.SPID_PK AND SP.WHOLESALERID = :wholesaler';
      EXECUTE IMMEDIATE l_sql INTO l_count USING w.WHOLESALER_ID;
      l_no_row_dropped_cb := l_no_row_dropped_cb + l_count;
    END IF;
  END LOOP;
 
    IF t_meter_dp.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;

  CLOSE cur_meter_dp;

  -- archive the latest batch
  P_DEL_UTIL_ARCHIVE_TABLE(p_tablename => l_tablename,
                           p_batch_no => no_batch,
                           p_filename => l_filename);


  -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP43', 2350, l_no_row_read,    'Distinct Meter Discharge Point Mappings read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP43', 2360, l_no_row_dropped, 'Meter Discharge Point Mappings dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP43', 2365, l_no_row_dropped_cb, 'Cross Border Supply Points not written to any file');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP43', 2370, l_no_row_insert,  'Meter Discharge Point Mappings written to ' || l_tablename || ' from extract');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP43', 2380, l_no_row_written, 'Meter Discharge Point Mappings written to file(s) from extract');

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
END P_MOU_DEL_METER_DISCHARGE;
/
exit;