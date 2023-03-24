CREATE OR REPLACE PROCEDURE P_MOU_TRAN_CALC_DISCHARGE  (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                       no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                       return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Calculated Discharge MO Extract 
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_CALC_DISCHARGE.sql
--
-- Subversion $Revision: 5194 $
--
-- CREATED        : 26/05/2016
--
-- DESCRIPTION    : Procedure to create the Calculated Discharge Points MO Extract 
--                  Will read from key gen and target tables, apply any transformationn
--                  rules and write to normalised tables.
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      26/05/2016  D.Cheung   Initial Draft
-- V 0.02      15/07/2016  D.Cheung   I-303 - Updated TE_CATEGORY and DISCHARGE TYPE to correct issue
-- v 0.03      18/07/2016  M.Marron   Removed amp ampsesand from comment abve and replace with and for auto build
-- v 0.04      19/07/2016  D.Cheung   I-306 - Remove NO_SERV_PROV to solve duplicates exceptions
-- V 0.05      27/07/2016  D.Cheung   I-321 - Only write latest record for same DPID and category type
-- V 0.06      15/08/2016  D.Cheung   I-324, I338 - not counting dropped rows correctly for reconciliations
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_CALC_DISCHARGE';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_srt_order                   VARCHAR2(1);
  l_prev_prp                    MO_CALCULATED_DISCHARGE.CALCDISCHARGEID_PK%TYPE; 
  l_t314                        CIS.TVP314TARACCLSAPPL%ROWTYPE; 
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
  l_mo_stw_balanced_yn          VARCHAR2(1);
  l_prev_dpid                   MO_CALCULATED_DISCHARGE.DPID_PK%TYPE;
  l_prev_dischargetype          MO_CALCULATED_DISCHARGE.DISCHARGETYPE%TYPE;
    
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
    IS 
        SELECT /*+ PARALLEL(BTW,12) PARALLEL(MD,12) PARALLEL(TV054,12) */
        DISTINCT
              MD.DPID_PK || BTW.MET_REF AS CALCDISCHARGEID_PK
              , MD.DPID_PK
              , TV054.NO_PROPERTY AS NO_PROPERTY
              , TV054.NO_ACCOUNT AS NO_ACCOUNT
--              , TV054.NO_SERV_PROV AS NO_SERV_PROV    --V0.04
              , BTW.NO_IWCS
              --, T703.CD_EXT_REF AS NO_IWCS
              , TRIM(UPPER(TE_REVISED_NAME)) AS DISCHARGETYPE   --v0.02
              , 'B' AS SUBMISSIONFREQ
              , MAX(BTW.CALC_DISCHARGE_VOL) AS TEYEARLYVOLESTIMATE
              , MAX(BTW.REFDESC) AS REFDESC
              , MD.DPEFFECTFROMDATE
              , MD.EFFECTFROMDATE
              , MD.EFFECTTODATE
              , 'CALCULATED' AS TECATEGORY    --v0.02
        FROM BT_TE_WORKING BTW
        JOIN MO_DISCHARGE_POINT MD ON BTW.NO_IWCS = MD.NO_IWCS
            AND BTW.NO_ACCOUNT = MD.NO_ACCOUNT
        JOIN BT_TVP054 TV054 ON TV054.NO_PROPERTY = MD.STWPROPERTYNUMBER_PK
            AND TV054.NO_ACCOUNT = MD.NO_ACCOUNT
        WHERE 1=1 --BTW.NO_IWCS || BTW.MET_REF BETWEEN p_no_property_start AND p_no_property_end 
--            AND BTW.TE_CATEGORY IN ('Private TE Meter', 'Private Water Meter')    --v0.02
              AND TRIM(UPPER(BTW.TE_CATEGORY)) IN ('CALCULATED')      --v0.02
        GROUP BY MD.DPID_PK || BTW.MET_REF, MD.DPID_PK, TV054.NO_PROPERTY, TV054.NO_ACCOUNT, BTW.NO_IWCS, TRIM(UPPER(TE_REVISED_NAME)), 'B', MD.DPEFFECTFROMDATE, MD.EFFECTFROMDATE, MD.EFFECTTODATE, 'CALCULATED'
        ORDER BY MD.DPID_PK, TRIM(UPPER(TE_REVISED_NAME)), MD.EFFECTFROMDATE DESC;
                          
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
    l_prev_prp := 0;
    l_job.IND_STATUS := 'RUN';
    l_prev_dpid := NULL;
    l_prev_dischargetype := NULL;

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
  
    OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

    l_progress := 'loop processing ';
    LOOP
  
        FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;
    
        l_no_row_read := l_no_row_read + t_prop.COUNT;
 
        FOR i IN 1..t_prop.COUNT
        LOOP
    
            l_err.TXT_KEY := t_prop(i).CALCDISCHARGEID_PK;
            l_mo := NULL;
            l_rec_written := TRUE;
            
            L_PROGRESS := 'CHECK IF VALIDATED TARIFF';
            BEGIN
                SELECT MAX(MO_STW_BALANCED_YN)
                INTO l_mo_stw_balanced_yn
                FROM BT_TE_SUMMARY
                WHERE NO_IWCS = t_prop(i).NO_IWCS
                ;
            END;
            IF (l_mo_stw_balanced_yn <> 'Y') THEN
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('TE TARIFF NOT VALIDATED - CALCULATION MISMATCH',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                l_no_row_exp := l_no_row_exp + 1;
                l_rec_written := FALSE;
            ELSE
                L_PROGRESS := 'PROCESSING TRANSFORMS';
                
                --Trade Effluent Tariff Band  D6024 - Tariff band for Trade Effluent banded charge        
                --CONDITION: Must be populated if the Discharge Point Tariff has a Banded charging element.  
                --    Value must be a valid Band as defined in the Discharge Point Tariff
                --STW HAS NO BANDED CHARGE FOR TE
                l_mo.TETARIFFBAND := NULL;
                
                --v0.02
                l_progress := 'TRANSFORM DISCHARGETYPE';
                l_mo.DISCHARGETYPE := CASE t_prop(i).DISCHARGETYPE
                    WHEN 'RAINFALL' THEN 'CONTAMINATED'
                    WHEN 'TANKER' THEN 'TANKERED'
                    ELSE 'ESTIMATED'
                END;
                
                --v0.05
                l_progress := 'CHECK DUPLICATE DPID AND DISCHARGETYPE';
                IF (l_prev_dpid = t_prop(i).DPID_PK AND l_prev_dischargetype = l_mo.DISCHARGETYPE ) THEN
                  P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('INFO ONLY - DROP DUPLICATE DPID AND DISCHARGETYPE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                  L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                  l_rec_written := FALSE;
                END IF;
            END IF;
        
            l_progress := 'INSERT MO_CALCULATED_DISCHARGE';
            IF l_rec_written THEN
                BEGIN 
                    INSERT INTO MO_CALCULATED_DISCHARGE
                        (CALCDISCHARGEID_PK, DPID_PK
                        , DPEFFECTFROMDATE, EFFECTFROMDATE, EFFECTTODATE
                        , DISCHARGETYPE, SUBMISSIONFREQ, TEYEARLYVOLESTIMATE
                        , STWPROPERTYNUMBER_PK, STWACCOUNTNUMBER, STWIWCS, REFDESC, TETARIFFBAND
                        , TECATEGORY    --v0.02
                        )
                    VALUES
                        (t_prop(i).CALCDISCHARGEID_PK, t_prop(i).DPID_PK
                        , t_prop(i).DPEFFECTFROMDATE, t_prop(i).EFFECTFROMDATE, t_prop(i).EFFECTTODATE
                        , l_mo.DISCHARGETYPE, t_prop(i).SUBMISSIONFREQ, t_prop(i).TEYEARLYVOLESTIMATE
                        , t_prop(i).NO_PROPERTY, t_prop(i).NO_ACCOUNT, t_prop(i).NO_IWCS, t_prop(i).REFDESC, l_mo.TETARIFFBAND
                        , t_prop(i).TECATEGORY    --v0.02
                        );
                EXCEPTION 
                    WHEN OTHERS THEN 
--                        l_no_row_dropped := l_no_row_dropped + 1;
                        l_rec_written := FALSE;
                        l_error_number := SQLCODE;
                        l_error_message := SQLERRM;
                        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));      -- LSmith V0.03
                        l_no_row_exp := l_no_row_exp + 1;
                END;
            END IF;  

            IF l_rec_written THEN
                l_no_row_insert := l_no_row_insert + 1;
                l_prev_dpid := t_prop(i).DPID_PK;
                l_prev_dischargetype := l_mo.DISCHARGETYPE;
            ELSE 
                l_no_row_dropped := l_no_row_dropped + 1;
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
            l_prev_prp := t_prop(i).CALCDISCHARGEID_PK;
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
END P_MOU_TRAN_CALC_DISCHARGE;
/
/
show error;

exit;