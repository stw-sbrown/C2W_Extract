CREATE OR REPLACE PROCEDURE P_MOU_TRAN_SC_MPW(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                              no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                              return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_SC_MPW.sql
--
-- Subversion $Revision: 4591 $
--
-- CREATED        : 15/03/2016
--
-- DESCRIPTION    : Procedure to hold details of all metered potable water services (MPW)
--                  read to each property. Will write to temporary table BT_SC_MPW.
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History --------------------------------------   
--
-- Version   Date        Author    Description
-- -------- ----------  --------  ------------------------------------------------------
-- V 0.04   24/06/2016  D.Cheung  I-257 - Add PCHG algorithm type to cursor filter
-- V 0.03   22/06/2016  L.Smith   Remove CP25
-- V 0.02   26/05/2016  S.Badhan  Choose the latest tariff version.
-- V 0.01   15/03/2016  S.Badhan  Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_SC_MPW';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE; 
  l_prev_sp                     BT_TVP054.NO_SERV_PROV%TYPE;   
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mpw                         BT_SC_MPW%ROWTYPE; 
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

    SELECT NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, NO_TARIFF_GROUP, NO_TARIFF_SET,
            CD_TARIFF, DS_EXT_REFERENCE, sum(CD_EXT_REF) AS SUM_CD_EXT_REF, max(CD_EXT_REF) AS MAX_CD_EXT_REF
     FROM 
     (SELECT spr.NO_PROPERTY, spr.NO_SERV_PROV, spr.NO_COMBINE_054, spr.CD_SERV_PROV, spr.NO_TARIFF_GROUP, spr.NO_TARIFF_SET,
            spr.CD_TARIFF, substr(spr.DS_EXT_REFERENCE,1,9) as DS_EXT_REFERENCE, spr.CD_EXT_REF,
            ROW_NUMBER() OVER ( PARTITION BY spr.NO_PROPERTY, spr.NO_SERV_PROV, spr.CD_TARIFF, spr.CD_EXT_REF ORDER BY spr.NO_PROPERTY, spr.NO_SERV_PROV, spr.CD_TARIFF, spr.CD_EXT_REF DESC ) AS Record_Nr
      FROM  BT_SPR_TARIFF_EXTREF   spr, 
            BT_SP_TARIFF_EXTREF    spv,
            LU_SERVICE_CATEGORY    cat   
      WHERE  spr.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end   
      AND    spv.CD_ALGORITHM IN ('BCAP','PCHG')
      AND    spv.CD_TARIFF    = spr.CD_TARIFF
      AND    spr.TP_ENTITY_332  = 'S'
      AND    spr.DS_EXT_REFERENCE IN ('2016-2017 Peak','2016-2017 Off Peak', '2015-2016 Peak','2015-2016 Off Peak')
      AND    spv.TP_ENTITY_332  = 'S'
      AND    spv.DS_EXT_REFERENCE IN ('2016-2017 Peak','2016-2017 Off Peak', '2015-2016 Peak','2015-2016 Off Peak')
   --   AND    trf.DT_END       IS NULL
      AND    cat.TARGET_SERV_PROV_CODE = trim(spr.CD_SERV_PROV)
      AND    cat.SERVICECOMPONENTTYPE  = 'MPW'          ) x
      WHERE  Record_Nr = 1
      GROUP BY NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, NO_TARIFF_GROUP, NO_TARIFF_SET, CD_TARIFF, DS_EXT_REFERENCE
      ORDER BY NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, DS_EXT_REFERENCE DESC ;  
      
      
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
   l_prev_sp := 0;
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
  
  DELETE FROM BT_SC_MPW;
  
  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing ';

  LOOP
  
    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop(i).NO_PROPERTY;
      l_mpw := NULL;
      l_mpw.D2079_MAXDAILYDMD := 0;
      l_mpw.D2080_DLYRESVDCAP := 0;
      
      IF l_prev_prp || l_prev_sp <> t_prop(i).NO_PROPERTY || t_prop(i).NO_SERV_PROV THEN
      
         l_no_row_read := l_no_row_read + 1;

         l_mpw.D2079_MAXDAILYDMD := t_prop(i).MAX_CD_EXT_REF;
         l_mpw.D2080_DLYRESVDCAP := t_prop(i).SUM_CD_EXT_REF / 365;
      
        l_progress := 'INSERT  BT_SC_MPW';
        l_rec_written := TRUE;
        BEGIN 
         INSERT INTO BT_SC_MPW
          (NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, 
           D2079_MAXDAILYDMD, D2080_DLYRESVDCAP, D2056_TARIFFCODE)
          VALUES
          (t_prop(i).NO_PROPERTY, t_prop(i).NO_SERV_PROV, t_prop(i).NO_COMBINE_054, t_prop(i).CD_SERV_PROV, 
          l_mpw.D2079_MAXDAILYDMD, l_mpw.D2080_DLYRESVDCAP, t_prop(i).CD_TARIFF);
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
           IF substr(t_prop(i).DS_EXT_REFERENCE,1,9) = '2015-2016' THEN
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'old tariff version used 2015-2016',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_war := l_no_row_war + 1;

              IF l_no_row_war > l_job.WAR_TOLERANCE THEN
                 CLOSE cur_prop; 
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Warning tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN; 
              END IF;
           END IF;
              
        END IF;
         
        l_prev_prp := t_prop(i).NO_PROPERTY;
        l_prev_sp := t_prop(i).NO_SERV_PROV; 
      
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
  
  
--  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 700, l_no_row_insert, 'Distinct Service Component Type during KEY_GEN stage 2');

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
END P_MOU_TRAN_SC_MPW;
/
show error;

--CREATE OR REPLACE PUBLIC SYNONYM P_MOU_TRAN_SC_MPW FOR P_MOU_TRAN_SC_MPW;

exit;
