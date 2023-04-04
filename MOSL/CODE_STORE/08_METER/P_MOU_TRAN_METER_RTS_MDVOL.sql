CREATE OR REPLACE PROCEDURE P_MOU_TRAN_METER_RTS_MDVOL (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                               no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                               return_code IN OUT NUMBER) 
IS 
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Update MDVOL and RTS meter data
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_RTS_MDVOL.sql
--
-- Subversion $Revision: 6253 $
--
-- CREATED        : 28/10/2016
--
-- DESCRIPTION    : Procedure to update the RTS and MDVOL values on the meters with the correct association calculation values
--
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.07      16/11/2016  D.Cheung   Add another 2 Materialized Views
-- V 0.06      08/11/2016  D.Cheung   Compile Materialized Views
-- V 0.05      03/11/2016  D.Cheung   Refresh Materialized Views
-- V 0.04      02/11/2016  D.Cheung   RTS get ABS value
-- V 0.03      01/11/2016  D.Cheung   Add section for updating YearlyVolumeEstimates
-- V 0.02      01/11/2016  D.Cheung   MDVOL get ABS value
-- V 0.01      28/10/2016  D.Cheung   Initial Draft
-----------------------------------------------------------------------------------------
  
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_RTS_MDVOL';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read_rts             MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_update_rts           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_rts          MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_mdvol           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_update_mdvol         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_mdvol        MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_read_yrvolest        MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_update_yrvolest      MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped_yrvolest     MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;  
  
  CURSOR cur_rts IS
    SELECT MM.METERREF, MM.MANUFACTURER_PK, MM.MANUFACTURERSERIALNUM_PK, ABS(TMTV.RTS) * 100 AS RTS, MM.RETURNTOSEWER 
    FROM TE_METERS_TAB_V TMTV
        , MO_METER MM
    WHERE MM.MANUFACTURER_PK = TMTV.MANUFACTURER_PK
    AND MM.MANUFACTURERSERIALNUM_PK = TMTV.MANUFACTURERSERIALNUM_PK
    AND MM.RETURNTOSEWER IS NOT NULL
    ORDER BY MM.METERREF;
    
TYPE tab_rts IS TABLE OF cur_rts%ROWTYPE INDEX BY PLS_INTEGER;
t_rts  tab_rts;    
    
  CURSOR cur_mdvol IS
    SELECT MD.METERDPIDXREF_PK, MD.MANUFACTURER_PK, MD.MANUFACTURERSERIALNUM_PK, NVL(ABS(TMTV.PERCENTAGEDISCHARGE),0) AS MDVOL, MD.PERCENTAGEDISCHARGE
    FROM TE_METERS_TAB_V TMTV
        , MO_METER_DPIDXREF MD
    WHERE MD.MANUFACTURER_PK = TMTV.MANUFACTURER_PK
    AND MD.MANUFACTURERSERIALNUM_PK = TMTV.MANUFACTURERSERIALNUM_PK
    ORDER BY MD.METERDPIDXREF_PK;
    
TYPE tab_mdvol IS TABLE OF cur_mdvol%ROWTYPE INDEX BY PLS_INTEGER;
t_mdvol  tab_mdvol;        

  CURSOR cur_yrvolest IS
    SELECT MM.METERREF, MM.MANUFACTURER_PK, MM.MANUFACTURERSERIALNUM_PK, MYV.YEARLYVOLEST, MM.YEARLYVOLESTIMATE 
    FROM METER_YEARLYVOLEST_V MYV
        , MO_METER MM
    WHERE MM.METERREF = MYV.METERREF
    ORDER BY MM.METERREF;
    
TYPE tab_yrvolest IS TABLE OF cur_yrvolest%ROWTYPE INDEX BY PLS_INTEGER;
t_yrvolest  tab_yrvolest; 

BEGIN
   -- initial variables
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read_rts := 0;
   l_no_row_update_rts := 0;
   l_no_row_dropped_rts := 0;
   l_no_row_read_mdvol := 0;
   l_no_row_update_mdvol := 0;
   l_no_row_dropped_mdvol := 0;
   l_no_row_read_yrvolest := 0;
   l_no_row_update_yrvolest := 0;
   l_no_row_dropped_yrvolest := 0;   
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

   COMMIT;

   l_progress := 'processing ';

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;
   
   l_progress := 'Compile Materialized Views';
   EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW TE_MATCHED_WATER_METERS1_MV COMPILE';
   EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW TE_MATCHED_WATER_METERS2_MV COMPILE';
   EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW TE_MATCHED_WATER_METERS3_MV COMPILE';
   EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW TE_MATCHED_WATER_METERS4_MV COMPILE';
   EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW TE_MATCHED_WATER_METERS5_MV COMPILE';
   EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW TE_MATCHED_WATER_METERS6_MV COMPILE';
   
   l_progress := 'Refresh Materialized Views';
   DBMS_MVIEW.REFRESH('TE_MATCHED_WATER_METERS1_MV');
   DBMS_MVIEW.REFRESH('TE_MATCHED_WATER_METERS2_MV');
   DBMS_MVIEW.REFRESH('TE_MATCHED_WATER_METERS3_MV');
   DBMS_MVIEW.REFRESH('TE_MATCHED_WATER_METERS4_MV');
   DBMS_MVIEW.REFRESH('TE_MATCHED_WATER_METERS5_MV');
   DBMS_MVIEW.REFRESH('TE_MATCHED_WATER_METERS6_MV');
  
  OPEN cur_rts ();

  l_progress := 'loop processing rts';

  LOOP
  
      FETCH cur_rts BULK COLLECT INTO t_rts LIMIT l_job.NO_COMMIT;
  
      FOR i IN 1..t_rts.COUNT
      LOOP
      
          l_err.TXT_KEY := t_rts(i).METERREF;
          l_rec_written := TRUE;    -- set default record status to write
          
          -- keep count of distinct meters
          l_no_row_read_rts := l_no_row_read_rts + 1;
          
          IF l_rec_written THEN
              BEGIN
                  l_progress := 'loop processing ABOUT TO UPDATE METER WITH RTS VALUE';
--                  UPDATE TEMP_METER_RTS 
                  UPDATE MO_METER 
                  SET RETURNTOSEWER = t_rts(i).RTS
                  WHERE METERREF = t_rts(i).METERREF;
              EXCEPTION
              WHEN OTHERS THEN
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
                       CLOSE cur_rts;
                       l_job.IND_STATUS := 'ERR';
                       P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                       P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                       commit;
                       return_code := -1;
                       RETURN;
                    END IF;
              END;
          END IF;
          
          -- keep count of records written
          IF l_rec_written THEN
              l_no_row_update_rts := l_no_row_update_rts + 1;
          ELSE
              l_no_row_dropped_rts := l_no_row_dropped_rts + 1;
          END IF;
          
      END LOOP;

      IF t_rts.COUNT < l_job.NO_COMMIT THEN
          EXIT;
      ELSE
          commit;
      END IF;

  END LOOP;

  CLOSE cur_rts;     
  
  -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1121, l_no_row_read_rts,    'Distinct Meters read during RTS update Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1131, l_no_row_dropped_rts, 'Distinct Meters dropped during RTS update Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1141, l_no_row_update_rts,  'Distinct Meters updated with RTS during Transform');

  commit;

  l_progress := 'END loop processing RTS';  
  

  OPEN cur_mdvol ();

  l_progress := 'loop processing MDVOL';
  
  LOOP
  
      FETCH cur_mdvol BULK COLLECT INTO t_mdvol LIMIT l_job.NO_COMMIT;
  
      FOR i IN 1..t_mdvol.COUNT
      LOOP
      
          l_err.TXT_KEY := t_mdvol(i).METERDPIDXREF_PK;
          l_rec_written := TRUE;    -- set default record status to write
          
          -- keep count of distinct meters
          l_no_row_read_mdvol := l_no_row_read_mdvol + 1;
          
          IF l_rec_written THEN
              BEGIN
                  l_progress := 'loop processing ABOUT TO UPDATE METER_DPIDXREF WITH MDVOL VALUE';
--                  UPDATE TEMP_METERDPIDXREF_MDVOL
                  UPDATE MO_METER_DPIDXREF 
                  SET PERCENTAGEDISCHARGE = t_mdvol(i).MDVOL
                  WHERE METERDPIDXREF_PK = t_mdvol(i).METERDPIDXREF_PK;
              EXCEPTION
              WHEN OTHERS THEN
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
                       CLOSE cur_mdvol;
                       l_job.IND_STATUS := 'ERR';
                       P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                       P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                       commit;
                       return_code := -1;
                       RETURN;
                    END IF;
              END;
          END IF;
          
          -- keep count of records written
          IF l_rec_written THEN
              l_no_row_update_mdvol := l_no_row_update_mdvol + 1;
          ELSE
              l_no_row_dropped_mdvol := l_no_row_dropped_mdvol + 1;
          END IF;
          
      END LOOP;

      IF t_mdvol.COUNT < l_job.NO_COMMIT THEN
          EXIT;
      ELSE
          commit;
      END IF;

  END LOOP;

  CLOSE cur_mdvol;       
  
    -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2631, l_no_row_read_mdvol,    'Distinct MeterDPIDXRefs read during MDVOL update Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2641, l_no_row_dropped_mdvol, 'Distinct MeterDPIDXRefs dropped during MDVOL update Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP53', 2651, l_no_row_update_mdvol,  'Distinct MeterDPIDXRefs updated with MDVOL during Transform');

  commit;
  
  l_progress := 'END loop processing MDVOL';  
    

  OPEN cur_yrvolest ();

  l_progress := 'loop processing YearlyVolumeEstimate';

  LOOP
  
      FETCH cur_yrvolest BULK COLLECT INTO t_yrvolest LIMIT l_job.NO_COMMIT;
  
      FOR i IN 1..t_yrvolest.COUNT
      LOOP
      
          l_err.TXT_KEY := t_yrvolest(i).METERREF;
          l_rec_written := TRUE;    -- set default record status to write
          
          -- keep count of distinct meters
          l_no_row_read_yrvolest := l_no_row_read_yrvolest + 1;
          
          IF l_rec_written THEN
              BEGIN
                  l_progress := 'loop processing ABOUT TO UPDATE METER WITH YEARLYVOLESTIMATE VALUE';
--                  UPDATE TEMP_METER_RTS 
                  UPDATE MO_METER 
                  SET YEARLYVOLESTIMATE = t_yrvolest(i).YEARLYVOLEST
                  WHERE METERREF = t_yrvolest(i).METERREF;
              EXCEPTION
              WHEN OTHERS THEN
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
                       CLOSE cur_yrvolest;
                       l_job.IND_STATUS := 'ERR';
                       P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                       P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                       commit;
                       return_code := -1;
                       RETURN;
                    END IF;
              END;
          END IF;
          
          -- keep count of records written
          IF l_rec_written THEN
              l_no_row_update_yrvolest := l_no_row_update_yrvolest + 1;
          ELSE
              l_no_row_dropped_yrvolest := l_no_row_dropped_yrvolest + 1;
          END IF;
          
      END LOOP;

      IF t_yrvolest.COUNT < l_job.NO_COMMIT THEN
          EXIT;
      ELSE
          commit;
      END IF;

  END LOOP;

  CLOSE cur_yrvolest;     
  
  -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1122, l_no_row_read_yrvolest,    'Distinct Meters read during YearlyVolEst update Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1132, l_no_row_dropped_yrvolest, 'Distinct Meters dropped during YearlyVolEst update Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1142, l_no_row_update_yrvolest,  'Distinct Meters updated with YearlyVolEst during Transform');

  commit;

  l_progress := 'END loop processing YearlyVolEst';  
   
    
  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';
  
  commit;
  
EXCEPTION
WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     commit;
     return_code := -1;  
END P_MOU_TRAN_METER_RTS_MDVOL;
/
/
show errors;
exit;