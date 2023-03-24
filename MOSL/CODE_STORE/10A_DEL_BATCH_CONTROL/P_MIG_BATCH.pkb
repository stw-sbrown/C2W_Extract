create or replace
PACKAGE BODY P_MIG_BATCH
IS
 ------------------------------------------------------------------------------------
-- PACKAGE BODY : Batch Migration
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MIG_BATCH.pkb
--
-- Subversion $Revision: 5333 $
--
-- CREATED        : 23/02/2016
--
-- DESCRIPTION    : Package containing common procedures for batch migration
--                  batch control, job control, error logging and reconciliation counts 
--
-- NOTES  :
-- 
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------        --------------------------------------
-- V 1.05      26/07/2016          S.Badhan       I-312 removed as cannot use in a transaction
-- V 1.04      20/07/2016          S.Badhan       I-312 - Enable parallel processing for session.
-- V 1.03      15/04/2016          S.Badhan       Removed setting of NO_RANGE_MAX, was 
--                                                being set twice.
-- V 1.00      22/03/2016          S.Badhan       Procedures to run added to schedule
-- V 0.01      23/02/2016          S.Badhan       Initial Draft
----------------------------------------------------------------------------------------


/*------------------------------------------------------------------------------
|| PROCEDURE  : P_STARTBATCH
|| DESCRIPTION: Start Batch schedule
||----------------------------------------------------------------------------*/
PROCEDURE P_STARTBATCH
IS

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_STARTBATCH';
  l_batch                       MIG_BATCHSTATUS%ROWTYPE;
  l_ref                         MIG_JOBREF%ROWTYPE;
  l_job                         MIG_JOBREF.NO_JOB%TYPE;
  return_code                   NUMBER;  
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_key                         VARCHAR2(200);
  l_progress                    VARCHAR2(100);

  PROCEDURE FN_GETJOB_PARMS
  IS
      l_ref                         MIG_JOBREF%ROWTYPE;    
  
  BEGIN
   
     l_progress := 'SELECT MIG_JOBREF ';
  
     BEGIN   
       SELECT NO_JOB
       INTO   l_ref.NO_JOB
       FROM   MIG_JOBREF
       WHERE  NM_PROCESS = g_proc_name;
     EXCEPTION
     WHEN  NO_DATA_FOUND THEN
           l_ref.ERR_TOLERANCE := 99999;
           l_ref.EXP_TOLERANCE := 99999;
           l_ref.WAR_TOLERANCE := 99999;
           l_ref.NO_STREAM := 50;
           l_ref.NO_RANGE_MIN := 1;
           l_ref.NO_RANGE_MAX := 999999999;
           l_ref.NO_COMMIT := 999999;
           
           INSERT INTO MIG_JOBREF
           (NO_JOB, NM_PROCESS, ERR_TOLERANCE, EXP_TOLERANCE, WAR_TOLERANCE, NO_STREAM, NO_RANGE_MIN, NO_RANGE_MAX, NO_COMMIT)
           VALUES
           ( (SELECT nvl(MAX(NO_JOB),0) + 100
              FROM   MIG_JOBREF), 
              g_proc_name, l_ref.ERR_TOLERANCE, l_ref.EXP_TOLERANCE, l_ref.WAR_TOLERANCE, l_ref.NO_STREAM, l_ref.NO_RANGE_MIN, l_ref.NO_RANGE_MAX, l_ref.NO_COMMIT)
           RETURNING NO_JOB INTO l_ref.NO_JOB;     
     END;
          
     g_no_job := l_ref.NO_JOB; 
  
  END FN_GETJOB_PARMS;
  
BEGIN

   DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Started ');

   l_key := c_module_name ;
   l_progress := 'Add new batch run';
   g_no_batch := 0;
   g_no_job := 0;

   SELECT GREATEST(nvl(MAX(NO_BATCH),0) + 1,100)
   INTO   l_batch.NO_BATCH
   FROM   MIG_BATCHSTATUS;

   INSERT INTO MIG_BATCHSTATUS
   (NO_BATCH, TS_START, DT_PROCESS, TS_UPDATE, BATCH_STATUS)
   VALUES
   (l_batch.NO_BATCH, current_timestamp, current_date, current_timestamp, 'RUN');
   
   COMMIT;
   
   g_no_batch := l_batch.NO_BATCH;
   
   -- Call extracts
 
   -- 1. Tariff Export
   g_proc_name := 'P_DEL_TARIFF_EXPORT_MAIN';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling Tariff XML Export procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_TARIFF_EXPORT.P_DEL_TARIFF_EXPORT_MAIN(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF;
 
   -- 2. Supply Point
   g_proc_name := 'P_MOU_DEL_SUPPLY_POINT';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_SUPPLY_POINT(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF;
 
    -- 3. Service Component
   g_proc_name := 'P_MOU_DEL_SERVICE_COMPONENT';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_SERVICE_COMPONENT(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF;

    -- 4. Meter   
   g_proc_name := 'P_MOU_DEL_METER';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_METER(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF;   

    -- 5. Meter Supply Point  
   g_proc_name := 'P_MOU_DEL_METER_SUPPLY_POINT';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_METER_SUPPLY_POINT(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF; 

    -- 6. Meter Network
   g_proc_name := 'P_MOU_DEL_METER_NETWORK';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_METER_NETWORK(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF; 

    -- 7. Meter Reading
   g_proc_name := 'P_MOU_DEL_METER_READING';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling MO Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_METER_READING(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF; 

    -- 8. Discharge Point
   g_proc_name := 'P_MOU_DEL_DISCHARGE_POINT';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_DISCHARGE_POINT(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF; 

    -- 9. Meter Discharge Point
   g_proc_name := 'P_MOU_DEL_METER_DISCHARGE';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling MO Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_METER_DISCHARGE(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF; 

    -- 10. Calculated Discharge 
   g_proc_name := 'P_MOU_DEL_CALCULATED_DISCHARGE';
   l_progress := 'Get job parameters ' || g_proc_name;
   FN_GETJOB_PARMS; 
   return_code := 0;
   l_progress := 'Calling DEL Extract procedure - ' || g_proc_name ;
   DBMS_OUTPUT.PUT_LINE(l_progress);
   P_MOU_DEL_CALCULATED_DISCHARGE(g_no_batch, g_no_job, return_code);  
   
   IF return_code = -1 THEN
      FN_UPDATEBATCH('ERR');
      DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
      RETURN;
   END IF; 

   -- 11. Generate reference data
   P_DEL_UTIL_REF_DATA();
   
   FN_UPDATEBATCH('END');
   DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended ');

EXCEPTION
WHEN OTHERS THEN     
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     FN_ERRORLOG(g_no_batch, 0, 'E', substr(l_error_message,1,100), l_key, l_progress);
     FN_UPDATEBATCH('ERR');
     DBMS_OUTPUT.PUT_LINE(to_char(SYSDATE,'HH24:MI:SS') || ' -  MIGRATION_BATCH Ended With errors  !!!!!');
     RAISE; 
END P_STARTBATCH;


/*------------------------------------------------------------------------------
|| PROCEDURE  : FN_UPDATEBATCH
|| DESCRIPTION: Update Batch schedule
||----------------------------------------------------------------------------*/
PROCEDURE FN_UPDATEBATCH(ind_status       IN MIG_BATCHSTATUS.BATCH_STATUS%TYPE)
IS
  c_module_name                 CONSTANT VARCHAR2(30) := 'FN_UPDATEBATCH';
  l_batch                       MIG_BATCHSTATUS%ROWTYPE;
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_key                         VARCHAR2(200);
  l_progress                    VARCHAR2(100);

BEGIN
 
   l_key := c_module_name || ',' ||  ind_status ;
  
   l_progress := 'Update batch run';
   l_batch.BATCH_STATUS := ind_status;
   
   IF ind_status NOT IN ('END', 'ERR', 'RUN') THEN
      FN_ERRORLOG(g_no_batch, 0, 'X', 'Invalid Parameter', l_key, l_progress);
      UPDATE MIG_BATCHSTATUS
      SET    BATCH_STATUS = 'ERR',
             TS_UPDATE    = CURRENT_TIMESTAMP
      WHERE  NO_BATCH     = g_no_batch;    
   ELSE
      UPDATE MIG_BATCHSTATUS
      SET    BATCH_STATUS = l_batch.BATCH_STATUS,
             TS_UPDATE    = CURRENT_TIMESTAMP
      WHERE  NO_BATCH     = g_no_batch;
   END IF;

   COMMIT;
   
EXCEPTION
WHEN OTHERS THEN     
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     FN_ERRORLOG(g_no_batch, 0, 'E', substr(l_error_message,1,100), l_key, l_progress);
     RAISE;
END FN_UPDATEBATCH;


/*------------------------------------------------------------------------------
|| PROCEDURE  : FN_STARTJOB
|| DESCRIPTION: Adds entry for new job run
||----------------------------------------------------------------------------*/
PROCEDURE FN_STARTJOB(no_batch          IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                      no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                      nm_process        IN MIG_JOBREF.NM_PROCESS%TYPE,            
                      no_instance       OUT MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                      err_tolerance     OUT MIG_JOBSTATUS.ERR_TOLERANCE%TYPE,                      
                      exp_tolerance     OUT MIG_JOBSTATUS.EXP_TOLERANCE%TYPE,        
                      war_tolerance     OUT MIG_JOBSTATUS.WAR_TOLERANCE%TYPE,     
                      no_commit         OUT MIG_JOBSTATUS.NO_COMMIT%TYPE,   
                      no_stream         OUT MIG_JOBSTATUS.NO_STREAM%TYPE,   
                      no_range_min      OUT MIG_JOBSTATUS.NO_RANGE_MIN%TYPE,   
                      no_range_max      OUT MIG_JOBSTATUS.NO_RANGE_MAX%TYPE,
                      ind_status        IN OUT MIG_JOBSTATUS.IND_STATUS%TYPE)                      
IS

  c_module_name                 CONSTANT VARCHAR2(11) := 'FN_STARTJOB';
  l_job                         MIG_JOBSTATUS%ROWTYPE;  
  l_ref                         MIG_JOBREF%ROWTYPE;  
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_key                         VARCHAR2(200);
  l_progress                    VARCHAR2(100);  

BEGIN
 
   l_key := c_module_name ||  ',' || no_job;
   no_instance := 0;
  
   l_progress := 'Select Job from MIG_JOBREF';
   --  DBMS_OUTPUT.PUT_LINE('FN_STARTJOB 1');

   l_ref.NM_PROCESS := nm_process;
   
   -- check job is for correct batch no

   SELECT NO_JOB,
          ERR_TOLERANCE,
          EXP_TOLERANCE,
          WAR_TOLERANCE,
          NO_STREAM,
          NO_RANGE_MIN,
          NO_RANGE_MAX,
          NO_COMMIT            
   INTO   l_ref.NO_JOB,
          l_ref.ERR_TOLERANCE,
          l_ref.EXP_TOLERANCE,
          l_ref.WAR_TOLERANCE,
          l_ref.NO_STREAM,
          l_ref.NO_RANGE_MIN,          
          l_ref.NO_RANGE_MAX,
          l_ref.NO_COMMIT            
   FROM   MIG_JOBREF
   WHERE  NM_PROCESS = l_ref.NM_PROCESS;

   IF l_ref.NO_JOB <> no_job THEN
      FN_ERRORLOG(no_batch, 0, 'X', 'Incorrect Job Number', l_key, nm_process || ',' || l_progress );
      ind_status := 'ERR';
      RETURN;
   END IF;
   
   l_progress := 'Add new JOB to MIG_JOBSTATUS';
   
   no_instance :=  no_job + l_ref.NO_STREAM;
   err_tolerance := l_ref.ERR_TOLERANCE;     
   exp_tolerance := l_ref.EXP_TOLERANCE;     
   war_tolerance := l_ref.WAR_TOLERANCE;   
   no_commit     := l_ref.NO_COMMIT;
   no_stream     := l_ref.NO_STREAM; 
   no_range_min  := l_ref.NO_RANGE_MIN; 
   no_range_max  := l_ref.NO_RANGE_MAX;

   INSERT INTO MIG_JOBSTATUS
   (NO_BATCH, NO_INSTANCE, TS_START, DT_PROCESS, TS_UPDATE, IND_STATUS, TXT_ARG, ERR_TOLERANCE, EXP_TOLERANCE,
    WAR_TOLERANCE, NO_COMMIT, NO_STREAM, NO_RANGE_MIN, NO_RANGE_MAX)
   VALUES
   (no_batch, no_instance, current_timestamp, current_date, current_timestamp, 'RUN', nm_process, err_tolerance, exp_tolerance,
   war_tolerance, no_commit, no_stream, no_range_min, no_range_max); 

   ind_status := 'RUN';

   COMMIT;
     
EXCEPTION
WHEN OTHERS THEN     
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     FN_ERRORLOG(no_batch, no_instance, 'E', substr(l_error_message,1,100), l_key, l_progress || ',' || no_job);
     ind_status := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, no_instance, ind_status);   
END FN_STARTJOB;


/*------------------------------------------------------------------------------
|| PROCEDURE  : FN_UPDATEJOB
|| DESCRIPTION: Update JOB status
||----------------------------------------------------------------------------*/
PROCEDURE FN_UPDATEJOB(no_batch         IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                       no_instance      IN MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                       ind_status       IN OUT MIG_JOBSTATUS.IND_STATUS%TYPE)           

IS
  c_module_name                 CONSTANT VARCHAR2(30) := 'FN_UPDATEJOB';
  l_job                         MIG_JOBSTATUS%ROWTYPE;  
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_key                         VARCHAR2(200);
  l_progress                    VARCHAR2(100);  
 
BEGIN
 
   l_key := c_module_name || ',' || ind_status;
  
   l_progress := 'Update job run';
   --DBMS_OUTPUT.PUT_LINE('FN_UPDATEJOB - 0 ' || no_batch || ' ,' || no_instance || ' ,' || ind_status);
     
   l_job.NO_BATCH := no_batch;
   l_job.NO_INSTANCE := no_instance;
   l_job.IND_STATUS := ind_status;

   IF l_job.IND_STATUS NOT IN ('END', 'ERR', 'RUN') THEN
      FN_ERRORLOG(l_job.NO_BATCH, l_job.NO_INSTANCE, 'X', 'Invalid Parameter', l_key, l_progress);
      UPDATE MIG_JOBSTATUS
      SET    IND_STATUS   = 'ERR',
             TS_UPDATE    = current_timestamp
      WHERE  NO_BATCH     = l_job.NO_BATCH
      AND    NO_INSTANCE  = l_job.NO_INSTANCE;
      ind_status := 'ERR';
   ELSE
      UPDATE MIG_JOBSTATUS
      SET    IND_STATUS   = l_job.IND_STATUS,
             TS_UPDATE    = current_timestamp
      WHERE  NO_BATCH     = l_job.NO_BATCH
      AND    NO_INSTANCE  = l_job.NO_INSTANCE;
   END IF;     
   
   COMMIT;
   
EXCEPTION
WHEN OTHERS THEN     
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     FN_ERRORLOG(no_batch, no_instance, 'E', substr(l_error_message,1,100), l_key, ind_status);
     UPDATE MIG_JOBSTATUS
     SET    IND_STATUS   = 'ERR',
            TS_UPDATE    = current_timestamp
     WHERE  NO_BATCH     = l_job.NO_BATCH
     AND    NO_INSTANCE  = l_job.NO_INSTANCE;
     ind_status := 'ERR';
END FN_UPDATEJOB;
   
/*------------------------------------------------------------------------------
|| PROCEDURE  : FN_ERRORLOG
|| DESCRIPTION: Inserts messages on the error log
||----------------------------------------------------------------------------*/
PROCEDURE FN_ERRORLOG(no_batch          IN MIG_JOBSTATUS.NO_BATCH%TYPE,
                      no_instance       IN MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                      ind_log           IN MIG_ERRORLOG.IND_LOG%TYPE,
                      txt_err           IN MIG_ERRREF.TXT_ERR%TYPE,
                      txt_key           IN MIG_ERRORLOG.TXT_KEY%TYPE,
                      txt_data          IN MIG_ERRORLOG.TXT_DATA%TYPE default null)                
IS

  c_module_name                 CONSTANT VARCHAR2(11) := 'FN_ERRORLOG';
  l_ref                         MIG_ERRREF%ROWTYPE;
  l_log                         MIG_ERRORLOG%ROWTYPE;  
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_key                         VARCHAR2(200);
  l_progress                    VARCHAR2(100);  
  
BEGIN

   l_key := c_module_name || ',' || ind_log;
   l_progress := 'Start of Error Logging';
   --DBMS_OUTPUT.PUT_LINE('FN_ERRORLOG ' || no_batch || ' ,' || no_instance || ' ,' || ind_log || ' ,' || txt_err);
   
   l_ref.IND_LOG := ind_log;
   l_ref.TXT_ERR := txt_err;
   
   IF ind_log NOT IN ('E', 'W', 'X') THEN
      FN_ERRORLOG(no_batch, no_instance, 'X', 'Invalid Parameter', l_key, l_progress);
      RETURN;
   END IF;

   BEGIN
      SELECT NO_ERR
      INTO   l_ref.NO_ERR
      FROM   MIG_ERRREF
      WHERE  IND_LOG = l_ref.IND_LOG
      AND    TXT_ERR = l_ref.TXT_ERR;
   EXCEPTION
   WHEN  NO_DATA_FOUND THEN
         INSERT INTO MIG_ERRREF
         (IND_LOG, NO_ERR, TXT_ERR)
         VALUES
         (l_ref.IND_LOG,
         (SELECT nvl(MAX(NO_ERR),0) + 1
          FROM   MIG_ERRREF),
          l_ref.TXT_ERR)
          RETURNING NO_ERR INTO l_ref.NO_ERR;
   END;
     
   l_progress := 'Add error to error log';

   l_log.NO_BATCH := no_batch;
   l_log.NO_INSTANCE := no_instance;
   l_log.TXT_KEY := txt_key;
   l_log.TXT_DATA := txt_data;
    
   INSERT INTO MIG_ERRORLOG
   (NO_BATCH, NO_INSTANCE, TS_CREATED, NO_SEQ, IND_LOG, NO_ERR, TXT_KEY, TXT_DATA)
   VALUES
   (no_batch, no_instance, CURRENT_TIMESTAMP, 
    (SELECT nvl(MAX(NO_SEQ),0) + 1
     FROM   MIG_ERRORLOG
     WHERE  NO_INSTANCE = l_log.NO_INSTANCE
     AND    NO_BATCH    = l_log.NO_BATCH),
     l_ref.IND_LOG, l_ref.NO_ERR, l_log.TXT_KEY, l_log.TXT_DATA);

EXCEPTION
WHEN OTHERS THEN          
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     DBMS_OUTPUT.PUT_LINE('Error in ' || c_module_name || ' at step ' || l_progress || ' -  ' || l_key || ' - ' || l_error_message);
     RAISE;
END FN_ERRORLOG;
  
/*------------------------------------------------------------------------------
|| PROCEDURE  : FN_RECONLOG
|| DESCRIPTION: Add processing counts to reconciliation tables
||----------------------------------------------------------------------------*/
PROCEDURE FN_RECONLOG(no_batch            IN MIG_CPLOG.NO_BATCH%TYPE,
                      no_instance         IN MIG_CPLOG.NO_INSTANCE%TYPE,
                      recon_control_point IN MIG_CPREF.RECON_CONTROL_POINT%TYPE,
                      recon_measure       IN MIG_CPREF.RECON_MEASURE%TYPE,
                      recon_measure_tot   IN MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE,
                      recon_measure_desc  IN MIG_CPREF.RECON_MEASURE_DESC%TYPE)        
IS

  c_module_name                 CONSTANT VARCHAR2(30) := 'FN_RECONLOG';
  l_ref                         MIG_CPREF%ROWTYPE;
  l_log                         MIG_CPLOG%ROWTYPE;
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_key                         VARCHAR2(200);
  l_progress                    VARCHAR2(100);  
  
BEGIN

   l_key := c_module_name || ',' || recon_measure_tot;
   l_progress := 'Start of reconciliation counts';
   --DBMS_OUTPUT.PUT_LINE('FN_RECONLOG 0 ' || recon_control_point || ', ' || recon_measure || ',' || RECON_MEASURE_DESC);

   l_ref.RECON_CONTROL_POINT := recon_control_point;
   l_ref.RECON_MEASURE := recon_measure;
   l_ref.RECON_MEASURE_DESC := recon_measure_desc;

   BEGIN
     SELECT NO_RECON_CP
     INTO   l_ref.NO_RECON_CP
     FROM   MIG_CPREF
     WHERE  RECON_CONTROL_POINT = l_ref.RECON_CONTROL_POINT
     AND    RECON_MEASURE       = l_ref.RECON_MEASURE
     AND    RECON_MEASURE_DESC  = l_ref.RECON_MEASURE_DESC;
   EXCEPTION
   WHEN NO_DATA_FOUND THEN
        INSERT INTO MIG_CPREF
        (NO_RECON_CP, RECON_CONTROL_POINT, RECON_MEASURE, RECON_MEASURE_DESC)
        VALUES
        ( (SELECT nvl(MAX(NO_RECON_CP),0) + 1
           FROM   MIG_CPREF),
           l_ref.RECON_CONTROL_POINT, l_ref.RECON_MEASURE, l_ref.RECON_MEASURE_DESC)
        RETURNING NO_RECON_CP INTO l_ref.NO_RECON_CP;
   END; 
   
   l_progress := 'Add to reconciliation log';
   
   l_log.RECON_MEASURE_TOTAL := recon_measure_tot;
   l_log.NO_BATCH := no_batch;
   l_log.NO_INSTANCE := no_instance;
   
   INSERT INTO MIG_CPLOG
   (NO_BATCH, NO_INSTANCE, TS_CREATED, NO_RECON_CP, RECON_MEASURE_TOTAL)
   VALUES
   (l_log.NO_BATCH, l_log.NO_INSTANCE, current_timestamp, l_ref.NO_RECON_CP, l_log.RECON_MEASURE_TOTAL);

     
EXCEPTION
WHEN OTHERS THEN       
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     FN_ERRORLOG(no_batch, no_instance, 'E', substr(l_error_message,1,100), l_key, recon_measure_desc);
     RAISE;
END FN_RECONLOG;
         

END P_MIG_BATCH;
/
exit;