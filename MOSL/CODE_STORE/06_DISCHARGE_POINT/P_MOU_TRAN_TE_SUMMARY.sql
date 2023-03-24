create or replace
PROCEDURE P_MOU_TRAN_TE_SUMMARY(no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                no_job      IN MIG_JOBREF.NO_JOB%TYPE,
                                return_code IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Eligibility Key Generation 
--
-- AUTHOR         : Lee Smith
--
-- FILENAME       : P_MOU_TRAN_TE_SUMMARY.sql
--
-- Subversion $Revision: 5458 $
--
-- CREATED        : 10/05/2016
--
-- DESCRIPTION    : Procedure to populate table BT_TV_WORKING 
--                  Migrated working data from the Trade Effluent Customer Database.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      13/05/2016  L.Smith    Initial version
--
-- V 0.02      25/05/2016  L. Smith   Added new columns to cross balance MO and STW calculations
-- V 0.03      01/06/2016  L. Smith   Do not process teaccess.cus_data rows for no_iwcs where an 
--                                    error has already been written to mig_errorlog.
-- V 0.04      07/06/2016  L. Smith   Column rounding
-- V 0.05      01/07/2016  L. Smith   Populate new column SEWERAGEVOLUMEADJMENTHOD
-- V 0.06      07/07/2106  L. Smith   Use parameter no_batch in cursor
-- V 0.07      28/07/2016  L. Smith   I322 
-- V 0.08      09/09/2016  L. Smith   Process period 16 working data
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TE_SUMMARY';
  l_key                         VARCHAR2(30);
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_jobref                      MIG_JOBREF%ROWTYPE;  
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE:=0;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE:=0;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE:=0;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE:=0;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE:=0;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE:=0;
  l_water_sp                    NUMBER(9);
  l_sewage_sp                   NUMBER(9);
  l_le                          NUMBER(9);
  l_mo_stw_balanced_yn          VARCHAR2(1);
  l_rec_written                 BOOLEAN;
  l_svam                        VARCHAR2(20);
  l_no_batch                    mig_errorlog.no_batch%TYPE := no_batch;

-- SELECT Trade Effluent working data migrated from the customer database
CURSOR cur_te_summary
    IS
SELECT 
       CASE WHEN INSTR(
                       TRANSLATE(
                                 SUBSTR(cd.no_account,
                                        1, 
                                        DECODE(INSTR(cd.no_account,'-'),0,LENGTH(cd.no_account),INSTR(cd.no_account,'-')-1)
                                       ),
                                 'ABCDEFGHIJKLMNOPQRSTUVWXYZ*',
                                 '                           '
                                ),
                       ' ') > 0 THEN
              -- Set to NULL as no_account still contains invalid characters after the hyphen and subsequent data has been removed
              NULL
            ELSE
              -- Removes the hyphen and any subsequent data
              SUBSTR(cd.no_account,
                     1, 
                     DECODE(INSTR(cd.no_account,'-'),0,LENGTH(cd.no_account),INSTR(cd.no_account,'-')-1)
                    )
       END AS no_account,
       cd.no_iwcs,
       NVL(working_data.working_rows,0) no_working_rows,
       cd.district,
       cd.sewage,
       cd.sitecode,
       cd.disno,
       cd.site_name,
       cd.site_add_1,
       cd.site_add_2,
       cd.site_add_3,
       cd.site_add_4,
       cd.site_pc,
       cd.bill_name,
       cd.bill_add_1,
       cd.bill_add_2,
       cd.bill_add_3,
       cd.bill_add_4,
       cd.bill_pc,
       cd.xref,
       cd.sp_code,
       cd.no_account no_account_ref,
       cd.charge_code,
       cd.cw_adv,
       cd.other_used_water,
       cd.dis_desc,
       cd.ammonia,
       cd.cod,
       cd.ss,
       cd.status,
       cd.ceased_date,
       cd.bill_cycle,
       cd.start_cypher,
       cd.data_provide_method,
       cd.dis_start_date,
       cd.tame_area,
       sitedata.col_calc,
       extref.no_property,
       extref.supply_point_code,
       extref.no_legal_entity,
       ROUND(working_data.te_vol,0)                  te_vol,
       ROUND(working_data.te_days,0)                 te_days,
       ROUND(working_data.ms_vol,0)                  ouw_vol,
       ROUND(working_data.ouw_days,0)                ouw_days,
       ROUND(working_data.fa_vol,0)                  fa_vol,
       ROUND(working_data.da_vol,0)                  da_vol,
       ROUND(working_data.pa_perc,2)                 pa_perc,
       ROUND(working_data.mdvol_for_ws_meter_perc,2) mdvol_for_ws_meter_perc,
       ROUND(working_data.mdvol_for_te_meter_perc,2) mdvol_for_te_meter_perc,
       ROUND(working_data.calc_discharge_vol,0)      calc_discharge_vol,
       ROUND(working_data.sub_meter,0)               sub_meter,
       ROUND(working_data.ws_vol,0)                  ws_vol,
       ROUND(working_data.mo_calc,0)                 mo_calc,
       ROUND(working_data.stw_calc,0)                stw_calc,
       da_only,
       da_exists,
       te_contributing,
       ms_contributing
  FROM teaccess.cus_data cd
  LEFT OUTER JOIN (
     SELECT *
       FROM (SELECT sd.no_iwcs,
                    sd.col_calc,
                    sd.cus_reader,
                    ROW_NUMBER() OVER (PARTITION BY sd.no_iwcs
                                       ORDER BY sd.no_iwcs, sd.cus_reader NULLS LAST) iwcs_row
               FROM teaccess.site_data sd
            )
      WHERE iwcs_row = 1
  ) sitedata
    ON cd.no_iwcs = sitedata.no_iwcs
  LEFT OUTER JOIN (
    SELECT -- Calculates a yearly Summary from the period summaries
           no_iwcs,
           SUM(working_rows)             working_rows,
           SUM(te_vol)                   te_vol,
           MAX(te_days)                  te_days,
           SUM(ms_vol)                   ms_vol,
           MAX(ouw_days)                 ouw_days,
           SUM(te_vol_filtered)          te_vol_filtered,
           MAX(mdvol_for_ws_meter_perc)  mdvol_for_ws_meter_perc,
           MAX(mdvol_for_te_meter_perc)  mdvol_for_te_meter_perc,
           SUM(ws_vol)                   ws_vol,
           SUM(calc_discharge_vol)       calc_discharge_vol,
           SUM(sub_meter)                sub_meter,
           SUM(da_vol)                   da_vol,
           SUM(fa_vol)                   fa_vol,
           MAX(pa_perc)                  pa_perc,
           SUM(mo_calc)                  mo_calc,
           SUM(stw_calc)                 stw_calc,
           MIN(da_only)                  da_only,
           MAX(da_exists)                da_exists,
           MAX(te_contributing)          te_contributing,
           MAX(ms_contributing)          ms_contributing
      FROM (SELECT -- Performs a summary at Period level
                   no_iwcs,
                   period,
                   working_rows,
                   te_vol,
                   te_days,
                   ms_vol,
                   ouw_days,
                   te_vol_filtered,
                   mdvol_for_ws_meter_perc,
                   mdvol_for_te_meter_perc,
                   ws_vol,
                   calc_discharge_vol,
                   sub_meter,
                   da_vol,
                   fa_vol,
                   pa_perc,
                   (te_vol_filtered - (te_vol_filtered * pa_perc)) +
                     (ws_vol - (ws_vol * pa_perc)) +
                     (calc_discharge_vol - (calc_discharge_vol * pa_perc)) -
                     (sub_meter - (sub_meter * pa_perc)) -
                     (da_vol - (da_vol * pa_perc)) -
                     (fa_vol - (fa_vol * pa_perc))
                   stw_calc,
                   (
                    (
                     (1 - pa_perc) *
                     (((ws_vol - sub_meter) * GREATEST(NVL(mdvol_for_ws_meter_perc,0),nvl(mdvol_for_te_meter_perc,0),1)) -    --Coalesce to 1 not 0?
                      (da_vol*1) -
                      (fa_vol)
                     )
                    )
                    + te_vol_filtered
                    + calc_discharge_vol
                   )
                   mo_calc,
                   da_only,
                   da_exists,
                   te_contributing,
                   ms_contributing
              FROM (
                    SELECT no_iwcs,
                           period,
                           COUNT(*) working_rows,
                           SUM(te_vol)                  te_vol,
                           MAX(CASE
                                 WHEN period = latest_period THEN
                                    te_year
                                 ELSE
                                    0
                               END
                              ) te_days,
                           MAX(CASE
                                 WHEN period = latest_period THEN
                                    ouw_year
                                 ELSE
                                    0
                               END
                              ) ouw_days,
                           SUM(ms_vol)                  ms_vol,
                           SUM(te_vol_filtered)         te_vol_filtered,
                           MAX(mdvol_for_ws_meter_perc) mdvol_for_ws_meter_perc,
                           MAX(mdvol_for_te_meter_perc) mdvol_for_te_meter_perc,
                           SUM(ws_vol)                  ws_vol,
                           SUM(calc_discharge_vol)      calc_discharge_vol,
                           SUM(sub_meter)               sub_meter,
                           SUM(da_vol)                  da_vol,
                           SUM(fa_vol)                  fa_vol,
                           MAX(pa_perc)                 pa_perc,
                           MIN(da_yn)                   da_only,
                           MAX(da_yn)                   da_exists,
                           MAX(te_contributing)         te_contributing,
                           MAX(ms_contributing)         ms_contributing
                      FROM (SELECT no_iwcs,
                                   period,
                                   stage,
                                   ltbc_start,
                                   ltbc_finish,
                                   MAX(period) OVER (PARTITION BY no_iwcs ORDER BY no_iwcs) latest_period, -- the maximum period for no_iwcs
                                   met_ref,
                                   ROUND(te_vol,0) te_vol,
                                   te_year,
                                   ROUND(ms_vol,0) ms_vol,
                                   ouw_year,
                                   te_vol_filtered,
                                   mdvol_for_ws_meter_perc,
                                   mdvol_for_te_meter_perc,
                                   ws_vol,
                                   calc_discharge_vol,
                                   sub_meter,
                                   da_vol,
                                   fa_vol,
                                   pa_perc,
                                   da_yn,
                                   CASE
                                      WHEN te > 0 AND te_vol > 0 
                                      THEN
                                         'Y'
                                      ELSE
                                         'N'
                                   END AS te_contributing,
                                   CASE
                                      WHEN ms > 0 AND ms_vol > 0 
                                      THEN
                                         'Y'
                                      ELSE
                                         'N'
                                   END AS ms_contributing
                              FROM bt_te_working
                              JOIN lu_te_billing_cycle
                                ON period = ltbc_period
                                   AND stage = ltbc_cycle_number
--where no_iwcs in (14427001101, 14427001102, 12509002501)
--                           ) WHERE period + 1 >= latest_period
                           ) WHERE period = 16  -- latest_period
                    GROUP BY no_iwcs, period
            )
    )
    GROUP BY no_iwcs
  ) working_data
    ON cd.no_iwcs = working_data.no_iwcs
  LEFT OUTER JOIN ( -- Retrieves no_property, supply_point_code and no_legal_entity for Trade Effluent
     SELECT DISTINCT
            TO_NUMBER(LTRIM(TRIM(extref.cd_ext_ref),'0')) no_iwcs,
            extref.no_property,
            supply_point_code,
            t054.no_legal_entity
       FROM bt_spr_tariff_extref extref
       JOIN lu_service_category lu
         ON TRIM(extref.cd_serv_prov) = lu.target_serv_prov_code
            AND lu.servicecomponenttype = 'TE'
       LEFT OUTER JOIN bt_tvp054 t054
         ON extref.no_combine_054 = t054.no_combine_054
      WHERE ds_ext_reference = 'Industrial Waste Reference '
        AND REPLACE(TRANSLATE(cd_ext_ref,'0123456789',' '),' ','') IS NULL   -- Verify numeric Format
  ) extref
    ON cd.no_iwcs = extref.no_iwcs
    WHERE cd.no_iwcs NOT IN ( -- Ignore no_iwcs rows already reported in error by P_MOU_TRAN_TE_WORKING
                             SELECT SUBSTR(elog.txt_key,1,INSTR(elog.txt_key,',')-1) no_iwcs
                               FROM mig_errorlog elog
                               JOIN mig_errref   eref
                                 ON elog.no_err = eref.no_err
                               LEFT OUTER JOIN mig_jobstatus jstatus
                                 ON elog.no_batch = jstatus.no_batch
                                    AND elog.no_instance = jstatus.no_instance
                              WHERE jstatus.txt_arg = 'P_MOU_TRAN_TE_WORKING'
                                AND elog.no_batch = l_no_batch
                                AND elog.ind_log = 'E'
                              GROUP BY SUBSTR(elog.txt_key,1,INSTR(elog.txt_key,',')-1)
                            );

  TYPE tab_te_summary IS TABLE OF cur_te_summary%ROWTYPE INDEX BY PLS_INTEGER;
  t_te_summary  tab_te_summary;

  
BEGIN
 
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_job.IND_STATUS := 'RUN';

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
  
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS); 
      return_code := -1;
      RETURN;
   END IF;

   l_progress := 'truncating table';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_TE_SUMMARY';


  l_progress := 'Processing Cursor';
  OPEN cur_te_summary;

  LOOP
  
    FETCH cur_te_summary BULK COLLECT INTO t_te_summary LIMIT l_job.NO_COMMIT;  -- 999999
 
    FOR i IN 1..t_te_summary.COUNT
    LOOP
    
      l_err.TXT_KEY := t_te_summary(i).no_iwcs;
      l_rec_written := TRUE;
      l_no_row_read := l_no_row_read + 1;
      
      -- Check stw calculation equals the MOSL calculation 
      IF ROUND(t_te_summary(i).mo_calc,0) = ROUND(t_te_summary(i).stw_calc,0) THEN
         l_mo_stw_balanced_yn := 'Y';
      ELSE
         l_mo_stw_balanced_yn := 'N';
      END IF;
      
      -- Derive column SEWERAGEVOLUMEADJMENTHOD             -- I-322
      IF t_te_summary(i).da_exists = 'Y' THEN
            l_svam := 'DA';
      ELSIF t_te_summary(i).te_contributing = 'Y' THEN
            l_svam := 'SUBTRACT';
      ELSE
            l_svam := 'NONE';
      END IF;
      
      -- Generate a warning if no_account found
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH,                                                                                  -- Batch number
                              L_JOB.NO_INSTANCE,                                                                         -- Job number
                              'W',                                                                                       -- log
                              SUBSTR('Missing no_account on teaccess.cusdata',1,100),                                    -- error/warning
                              L_ERR.TXT_KEY,                                                                             -- key
                              SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100)                                          -- data
                             );
      
      -- Generate a warning if no_property found
            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH,                                                                                  -- Batch number
                              L_JOB.NO_INSTANCE,                                                                         -- Job number
                              'W',                                                                                       -- log
                              SUBSTR('Unable to find the no_property',1,100),                                            -- error/warning
                              L_ERR.TXT_KEY,                                                                             -- key
                              SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100)                                          -- data
                             );

      BEGIN
        INSERT INTO bt_te_summary (
        NO_ACCOUNT,              NO_IWCS,             NO_PROPERTY,             SUPPLY_POINT_CODE,       NO_LEGAL_ENTITY,
        NO_WORKING_ROWS,         DISTRICT,            SEWAGE,                  SITECODE,                DISNO,
        SITE_NAME,               SITE_ADD_1,          SITE_ADD_2,              SITE_ADD_3,              SITE_ADD_4,
        SITE_PC,                 BILL_NAME,           BILL_ADD_1,              BILL_ADD_2,              BILL_ADD_3,
        BILL_ADD_4,              BILL_PC,             XREF,                    SP_CODE,                 NO_ACCOUNT_REF,
        CHARGE_CODE,             CW_ADV,              OTHER_USED_WATER,        DIS_DESC,                AMMONIA,
        COD,                     SS,                  STATUS,                  CEASED_DATE,             BILL_CYCLE,
        START_CYPHER,            DATA_PROVIDE_METHOD, DIS_START_DATE,          TAME_AREA,               COL_CALC,
        TE_VOL,                  OUW_VOL,             TE_DAYS,                 OUW_DAYS,                FA_VOL,
        DA_VOL,                  PA_PERC,             MDVOL_FOR_WS_METER_PERC, MDVOL_FOR_TE_METER_PERC, CALC_DISCHARGE_VOL,
        SUB_METER,               WS_VOL,              MO_CALC,                 STW_CALC,                MO_STW_BALANCED_YN,
        SEWERAGEVOLUMEADJMENTHOD)
        VALUES (
           t_te_summary(i).no_account,      t_te_summary(i).no_iwcs,             t_te_summary(i).no_property,             t_te_summary(i).supply_point_code,       t_te_summary(i).no_legal_entity,
           t_te_summary(i).no_working_rows, t_te_summary(i).district,            t_te_summary(i).sewage,                  t_te_summary(i).sitecode,                t_te_summary(i).disno,
           t_te_summary(i).site_name,       t_te_summary(i).site_add_1,          t_te_summary(i).site_add_2,              t_te_summary(i).site_add_3,              t_te_summary(i).site_add_4,
           t_te_summary(i).site_pc,         t_te_summary(i).bill_name,           t_te_summary(i).bill_add_1,              t_te_summary(i).bill_add_2,              t_te_summary(i).bill_add_3,
           t_te_summary(i).bill_add_4,      t_te_summary(i).bill_pc,             t_te_summary(i).xref,                    t_te_summary(i).sp_code,                 t_te_summary(i).no_account_ref,
           t_te_summary(i).charge_code,     t_te_summary(i).cw_adv,              t_te_summary(i).other_used_water,        t_te_summary(i).dis_desc,                t_te_summary(i).ammonia,
           t_te_summary(i).cod,             t_te_summary(i).ss,                  t_te_summary(i).status,                  t_te_summary(i).ceased_date,             t_te_summary(i).bill_cycle,
           t_te_summary(i).start_cypher,    t_te_summary(i).data_provide_method, t_te_summary(i).dis_start_date,          t_te_summary(i).tame_area,               t_te_summary(i).col_calc,
           t_te_summary(i).te_vol,          t_te_summary(i).ouw_vol,             t_te_summary(i).te_days,                 t_te_summary(i).ouw_days,                t_te_summary(i).fa_vol,
           t_te_summary(i).da_vol,          t_te_summary(i).pa_perc,             t_te_summary(i).mdvol_for_ws_meter_perc, t_te_summary(i).mdvol_for_te_meter_perc, t_te_summary(i).calc_discharge_vol,
           t_te_summary(i).sub_meter,       t_te_summary(i).ws_vol,              t_te_summary(i).mo_calc,                 t_te_summary(i).stw_calc,                l_mo_stw_balanced_yn,
           l_svam);
      EXCEPTION
          WHEN OTHERS THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_err := l_no_row_err + 1;
      END;



      -- keep count of records written
      IF l_rec_written THEN
         l_no_row_insert := l_no_row_insert + 1;
      ELSIF (l_no_row_exp > l_job.EXP_TOLERANCE
             OR l_no_row_err > l_job.ERR_TOLERANCE) THEN
            -- tolearance limit has een exceeded, set error message and exit out
              CLOSE cur_te_summary; 
              l_job.IND_STATUS := 'ERR';
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
              COMMIT;
              return_code := -1;
              RETURN; 
      END IF;

    END LOOP;

    IF t_te_summary.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  CLOSE cur_te_summary;


   -- write counts 
  l_progress := 'Writing Counts';  
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP51', 2600, l_no_row_read,    'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP51', 2610, l_no_row_dropped, 'Dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP51', 2620, l_no_row_insert,  'Written to Table ');    

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

     
END P_MOU_TRAN_TE_SUMMARY;

/
show error;

exit;