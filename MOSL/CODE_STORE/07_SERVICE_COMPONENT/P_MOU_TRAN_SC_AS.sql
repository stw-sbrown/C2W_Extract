CREATE OR REPLACE PROCEDURE P_MOU_TRAN_SC_AS(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                             no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                             return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_SC_AS.sql
--
-- Subversion $Revision: 4621 $
--
-- CREATED        : 15/03/2016
--
-- DESCRIPTION    : Procedure to hold details of Assessed Sewage
--                  Will write to temporary table BT_SC_AS.
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History --------------------------------------   
--
-- Version     Date           Author     Description
-- ---------   ------------   -------    -----------------------------------------------
-- V 0.01      29/06/2016     S.Badhan   I-260. Initial Draft
----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_SC_AS';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_uw                          BT_SC_UW%ROWTYPE; 
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
                     
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
    IS 

      SELECT spr.NO_PROPERTY, spr.NO_SERV_PROV, spr.NO_COMBINE_054, spr.CD_SERV_PROV, spr.NO_TARIFF_GROUP, spr.NO_TARIFF_SET,
             spr.CD_TARIFF, spr.NO_VALUE
      FROM   BT_SPR_TARIFF_ALGITEM  spr,
             LU_SERVICE_CATEGORY    cat   
      WHERE  spr.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end   
      AND    spr.CD_BILL_ALG_ITEM      = 'FSR'
      AND    cat.TARGET_SERV_PROV_CODE = trim(spr.CD_SERV_PROV)
      AND    cat.SERVICECOMPONENTTYPE  = 'AS'
      ORDER BY 1, 2;

TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;
  
BEGIN
 
   -- initial variables 
   
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_prev_prp := 0;
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
     
   l_progress := 'processing ';

   -- any errors set return code and exit out
   
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS); 
      return_code := -1;
      RETURN;
   END IF;
      
  -- start processing all records for range supplied
  
  DELETE FROM BT_SC_AS;
  
  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing ';

  LOOP
  
    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop(i).NO_PROPERTY;
      l_uw := NULL;
  
      l_no_row_read := l_no_row_read + 1;

        l_progress := 'INSERT  BT_SC_UW';  
   
          l_rec_written := TRUE;
          BEGIN 
           INSERT INTO BT_SC_AS
           (NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, 
            CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET, NO_VALUE)
            VALUES
            (t_prop(i).NO_PROPERTY, t_prop(i).NO_SERV_PROV, t_prop(i).NO_COMBINE_054, t_prop(i).CD_SERV_PROV, 
             t_prop(i).CD_TARIFF, t_prop(i).NO_TARIFF_GROUP, t_prop(i).NO_TARIFF_SET, t_prop(i).NO_VALUE);
           EXCEPTION  
          WHEN OTHERS THEN 
               l_no_row_dropped := l_no_row_dropped + 1;
               l_rec_written := FALSE;
               l_error_number := SQLCODE;
               l_error_message := SQLERRM;
               
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
               l_no_row_err := l_no_row_err + 1;
               
               -- if tolearance limit has een exceeded, set error message and exit out
               IF l_no_row_err > l_job.ERR_TOLERANCE THEN
                   CLOSE cur_prop; 
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
        
        l_prev_prp := t_prop(i).NO_PROPERTY;
        
    END LOOP;
    
    IF t_prop.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  
  CLOSE cur_prop;  

  -- write counts 
  l_progress := 'Writing Counts';  
  
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 730, l_no_row_insert, 'Distinct Service Component Type during KEY_GEN stage 2');

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
END P_MOU_TRAN_SC_AS;
/
show error;

--CREATE OR REPLACE PUBLIC SYNONYM P_MOU_TRAN_SC_AS FOR P_MOU_TRAN_SC_AS;

exit;
