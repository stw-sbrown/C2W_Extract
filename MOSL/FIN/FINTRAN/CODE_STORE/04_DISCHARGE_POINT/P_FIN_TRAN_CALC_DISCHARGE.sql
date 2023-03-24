CREATE OR REPLACE PROCEDURE P_FIN_TRAN_CALC_DISCHARGE(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                      no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                      return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Calculated Discharge Point MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_FIN_TRAN_CALC_DISCHARGE.sql
--
-- Subversion $Revision: 5345 $
--
-- CREATED        : 30/08/2016
--
-- DESCRIPTION    : Procedure to populate MO_CALCULATED_DISCHARGE from SAP and OWC supplied data 
--               - OWC_CALCULATED_DISCHARGE and SAP_CALCULATED_DISCHARGE.
--
-- DESCRIPTION    : Procedure to create the Calculated Discharge Points MO Extract 
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      30/08/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_FIN_TRAN_CALC_DISCHARGE';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_CALCULATED_DISCHARGE%ROWTYPE; 
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_no_dp                       NUMBER(9) := 0;

CURSOR cur_prop (p_property_start   MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE,
                 p_property_end     MO_ELIGIBLE_PREMISES.STWPROPERTYNUMBER_PK%TYPE)                 
    IS 
    SELECT  cd.DPID_PK,
            cd.CALCDISCHARGEID_PK,
            cd.DISCHARGETYPE,
            cd.SUBMISSIONFREQ,
            cd.TEYEARLYVOLESTIMATE,
            cd.OWC,
            dp.SPID_PK,
            dp.NO_IWCS,
            dp.NO_SAMPLE_POINT, 
            dp.CONSENT_NO,
            dp.DPEFFECTFROMDATE,
            dp.EFFECTFROMDATE,
            dp.EFFECTTODATE,
            dp.STWPROPERTYNUMBER_PK 
    FROM    RECEPTION.SAP_CALCULATED_DISCHARGE cd 
            JOIN MO_DISCHARGE_POINT dp ON dp.DPID_PK = cd.DPID_PK
    WHERE   dp.STWPROPERTYNUMBER_PK BETWEEN p_property_start AND p_property_end  --AND pr.STWPROPERTYNUMBER_PK = 958002009
    ORDER BY dp.STWPROPERTYNUMBER_PK; 

TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;
  
BEGIN
 
   -- initialise variables 
   
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
     
   l_progress := 'processing';

   -- any errors set return code and exit out
   
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS); 
      return_code := -1;
      RETURN;
   END IF;
      
  -- start processing all records for range supplied
  
  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing';

  LOOP
  
    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := substr(t_prop(i).STWPROPERTYNUMBER_PK || ',' || t_prop(i).SPID_PK || ',' || t_prop(i).DPID_PK,1,30);
          
      l_mo := NULL;
      l_rec_written := TRUE;

      -- keep count of distinct property
      l_no_row_read := l_no_row_read + 1;

      l_mo.CALCDISCHARGEID_PK := t_prop(i).CALCDISCHARGEID_PK;  
      l_mo.DPID_PK := t_prop(i).DPID_PK;  
      l_mo.DPEFFECTFROMDATE := t_prop(i).DPEFFECTFROMDATE;   
      l_mo.EFFECTFROMDATE := t_prop(i).EFFECTFROMDATE;   
      l_mo.EFFECTTODATE := t_prop(i).EFFECTTODATE;   
      l_mo.DISCHARGETYPE := t_prop(i).DISCHARGETYPE;   
      l_mo.SUBMISSIONFREQ := t_prop(i).SUBMISSIONFREQ;         
      l_mo.TEYEARLYVOLESTIMATE := t_prop(i).TEYEARLYVOLESTIMATE;   
      l_mo.STWPROPERTYNUMBER_PK := t_prop(i).STWPROPERTYNUMBER_PK;      
      l_mo.STWACCOUNTNUMBER := null;   
      l_mo.STWIWCS := t_prop(i).NO_IWCS;   
      l_mo.REFDESC := null;                              ---*** FIX NULL 
      l_mo.TETARIFFBAND := NULL;   
      l_mo.TECATEGORY := 'CALCULATED';         
    
      l_progress := 'INSERT MO_CALCULATED_DISCHARGE ';           
    
      IF l_rec_written THEN
         BEGIN 
           INSERT INTO MO_CALCULATED_DISCHARGE
           (CALCDISCHARGEID_PK, DPID_PK, DPEFFECTFROMDATE, EFFECTFROMDATE, EFFECTTODATE,
            DISCHARGETYPE, SUBMISSIONFREQ, TEYEARLYVOLESTIMATE, STWPROPERTYNUMBER_PK, STWACCOUNTNUMBER,
            STWIWCS, REFDESC, TETARIFFBAND, TECATEGORY)
           VALUES
           (l_mo.CALCDISCHARGEID_PK, l_mo.DPID_PK, l_mo.DPEFFECTFROMDATE, l_mo.EFFECTFROMDATE, l_mo.EFFECTTODATE,
            l_mo.DISCHARGETYPE, l_mo.SUBMISSIONFREQ, l_mo.TEYEARLYVOLESTIMATE, l_mo.STWPROPERTYNUMBER_PK, l_mo.STWACCOUNTNUMBER,
            l_mo.STWIWCS, l_mo.REFDESC, l_mo.TETARIFFBAND, l_mo.TECATEGORY);
         EXCEPTION 
         WHEN OTHERS THEN 
              l_no_row_dropped := l_no_row_dropped + 1;
              l_rec_written := FALSE;
              l_error_number := SQLCODE;
              l_error_message := SQLERRM;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK,1,100));      -- LSmith V0.03
              l_no_row_err := l_no_row_err + 1;
         END;
      ELSE
        l_no_row_dropped := l_no_row_dropped + 1;
      END IF;  

      IF l_rec_written THEN
         l_no_row_insert := l_no_row_insert + 1;
      ELSE 
         -- if tolearance limit has een exceeded, set error message and exit out
         IF (   l_no_row_exp > l_job.EXP_TOLERANCE
             OR l_no_row_err > l_job.ERR_TOLERANCE)   
         THEN
            CLOSE cur_prop; 
            l_job.IND_STATUS := 'ERR';
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
            P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
            COMMIT;
            return_code := -1;
            RETURN; 
         END IF;
       
      END IF;

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
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP47', 2480, l_no_row_read,    'Read calc_discharges in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP47', 2490, l_no_row_dropped, 'Dropped calc_discharges during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP47', 2500, l_no_row_insert,  'Written calc_discharges to Table ');    

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
END P_FIN_TRAN_CALC_DISCHARGE;
/
show error;

exit;