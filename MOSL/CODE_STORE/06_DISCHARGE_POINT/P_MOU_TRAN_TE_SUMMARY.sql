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
-- Subversion $Revision: 4023 $
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
  l_rec_written                 BOOLEAN;

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
       NULL te_volume,
       NULL ouw_volume,
       NULL te_days,
       NULL ouw_days
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
      SELECT no_iwcs, 
      COUNT(*) working_rows
        FROM bt_te_working
        GROUP BY no_iwcs
  ) working_data
    ON cd.no_iwcs = working_data.no_iwcs
  LEFT OUTER JOIN (
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
    ON cd.no_iwcs = extref.no_iwcs;

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


      BEGIN
        INSERT INTO bt_te_summary (
           NO_ACCOUNT,       NO_IWCS,             NO_PROPERTY,      SUPPLY_POINT_CODE, NO_LEGAL_ENTITY,
           NO_WORKING_ROWS,  DISTRICT,            SEWAGE,           SITECODE,          DISNO,
           SITE_NAME,        SITE_ADD_1,          SITE_ADD_2,       SITE_ADD_3,        SITE_ADD_4,
           SITE_PC,          BILL_NAME,           BILL_ADD_1,       BILL_ADD_2,        BILL_ADD_3,
           BILL_ADD_4,       BILL_PC,             XREF,             SP_CODE,           NO_ACCOUNT_REF,
           CHARGE_CODE,      CW_ADV,              OTHER_USED_WATER, DIS_DESC,          AMMONIA,
           COD,              SS,                  STATUS,           CEASED_DATE,       BILL_CYCLE,
           START_CYPHER,     DATA_PROVIDE_METHOD, DIS_START_DATE,   TAME_AREA,         COL_CALC,
           TE_VOLUME,        OUW_VOLUME,          TE_DAYS,          OUW_DAYS)
        VALUES (
           t_te_summary(i).no_account,      t_te_summary(i).no_iwcs,             t_te_summary(i).no_property,      t_te_summary(i).supply_point_code, t_te_summary(i).no_legal_entity,
           t_te_summary(i).no_working_rows, t_te_summary(i).district,            t_te_summary(i).sewage,           t_te_summary(i).sitecode,          t_te_summary(i).disno,
           t_te_summary(i).site_name,       t_te_summary(i).site_add_1,          t_te_summary(i).site_add_2,       t_te_summary(i).site_add_3,        t_te_summary(i).site_add_4,
           t_te_summary(i).site_pc,         t_te_summary(i).bill_name,           t_te_summary(i).bill_add_1,       t_te_summary(i).bill_add_2,        t_te_summary(i).bill_add_3,
           t_te_summary(i).bill_add_4,      t_te_summary(i).bill_pc,             t_te_summary(i).xref,             t_te_summary(i).sp_code,           t_te_summary(i).no_account_ref,
           t_te_summary(i).charge_code,     t_te_summary(i).cw_adv,              t_te_summary(i).other_used_water, t_te_summary(i).dis_desc,          t_te_summary(i).ammonia,
           t_te_summary(i).cod,             t_te_summary(i).ss,                  t_te_summary(i).status,           t_te_summary(i).ceased_date,       t_te_summary(i).bill_cycle,
           t_te_summary(i).start_cypher,    t_te_summary(i).data_provide_method, t_te_summary(i).dis_start_date,   t_te_summary(i).tame_area,         t_te_summary(i).col_calc,
           t_te_summary(i).te_volume,       t_te_summary(i).ouw_volume,          t_te_summary(i).te_days,          t_te_summary(i).ouw_days);
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

