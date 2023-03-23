create or replace
PROCEDURE P_MOU_TRAN_TE_WORKING(no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                no_job      IN MIG_JOBREF.NO_JOB%TYPE,
                                return_code IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Eligibility Key Generation 
--
-- AUTHOR         : Lee Smith
--
-- FILENAME       : P_MOU_TRAN_TE_WORKING.sql
--
-- Subversion $Revision: 4026 $
--
-- CREATED        : 13/05/2016
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

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_TE_WORKING';
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
-- Note: end_read_calculated has been used in calculations due to missing data
CURSOR cur_te_working
    IS
    SELECT no_account,
           no_account_ref,
           fa_yn,
           CASE -- See spreadsheet working tab column (AD) Formula
              WHEN fa_yn = 'Y' AND te_revised_name IS NOT NULL THEN
                 CASE
                    WHEN te_revised_name != 'Domestic' AND te <= 0 AND end_read_calculated IS NOT NULL AND start_date IS NOT NULL AND units IS NOT NULL THEN
                       (end_read_calculated - start_read) * units
                 END
           END AS fa_vol,
           da_yn,
           CASE -- See spreadsheet working tab column (AF) Formula
              WHEN da_yn = 'Y' AND unit IS NOT NULL THEN
                 CASE 
                    WHEN unit = 'Head' AND start_read IS NOT NULL AND end_read_calculated IS NOT NULL AND units IS NOT NULL THEN
                       (end_read_calculated - start_read) * units
                    WHEN unit != 'Head' AND te_year IS NOT NULL AND end_read_calculated IS NOT NULL AND units IS NOT NULL THEN
                       (te_year / 2) * end_read_calculated * units
                 END
           END AS da_vol,
           pa_yn,
           CASE -- See spreadsheet working tab column (AH) Formula
              WHEN pa_yn = 'Y' AND te IS NOT NULL THEN
                 CASE 
                    WHEN te > 0 AND te < 1 THEN
                       1 - te
                    WHEN te > -1 AND te < 0 THEN
                       1- (-1 * te)
                 END
           END AS pa_perc,
           mdvol_for_ws_meter_yn,
           CASE -- See spreadsheet working tab column (AJ) Formula
              WHEN mdvol_for_ws_meter_yn = 'Y' AND NVL(te,-1) > 0 THEN
                 1
           END AS mdvol_for_ws_meter_perc,
           mdvol_for_te_meter_yn,
           CASE -- See spreadsheet working tab column (AL) Formula
              WHEN mdvol_for_te_meter_yn = 'Y' AND NVL(te,-1) > 0 THEN
                 1
           END AS mdvol_for_te_meter_perc,
           calc_discharge_yn,
           CASE -- See spreadsheet working tab column (AN) Formula
              WHEN end_read_calculated IS NULL OR start_read IS NULL OR UNITS IS NULL THEN
                 NULL
              WHEN calc_discharge_yn = 'Y' AND te_category = 'Calculated' THEN
                 (end_read_calculated - start_read) * units
           END AS calc_discharge_vol,
           te_revised_name,
           te_category,
           period,
           met_ref,
           serial_no,
           refdesc,
           target_ref,
           unit,
           units,
           start_date,
           start_read,
           code,
           end_date,
           end_read,
           codea,
           te,
           te_vol,
           ms,
           ms_vol,
           no_iwcs,
           stage,
           reason,
           ouw_year,
           te_year,
           end_read_calculated
      FROM
       (SELECT -- Removes the hyphen and any subsequent data
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
               cd.no_account   no_account_ref,
               CASE -- Derive FA_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN lu1.te_category = 'Fixed' AND te_working_data.te = -1 THEN
                     'Y'
                  ELSE
                     'N'
               END AS fa_yn,
               CASE  -- Derive DA_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN lu1.te_category = 'Domestic' THEN
                     'Y'
                  ELSE
                     'N'
               END AS da_yn,
               CASE  -- Derive PA_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN te_working_data.te BETWEEN 0 and 1 THEN
                     'Y'
                  ELSE
                     'N'
               END AS pa_yn,
               CASE  -- Derive MDVOL_FOR_WS_METER_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN lu1.te_category = 'Water Meter' AND te_working_data.te = 1 AND te_working_data.ms = 0 THEN
                     'Y'
                  ELSE
                     'N'
               END AS mdvol_for_ws_meter_yn,
               CASE  -- Derive MDVOL_FOR_TE_METER_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN lu1.te_category IN ('TE Meter', 'Private TE Meter') AND te_working_data.te = 1 AND te_working_data.ms = 0 THEN
                     'Y'
                  ELSE
                     'N'
               END AS mdvol_for_te_meter_yn,
               CASE  -- Derive CALC_DISCHARGE_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN lu1.te_category = 'Calculated' THEN
                     'Y'
                  ELSE
                     'N'
               END AS calc_discharge_yn,
               lu1.te_revised_name,
               lu1.te_category,
               te_working_data.period,                                                                                                                                                                                       
               te_working_data.met_ref,
               te_working_data.serial_no,
               te_working_data.refdesc,
               te_working_data.target_ref,
               te_working_data.unit,
               te_working_data.units,
               te_working_data.start_date,
               te_working_data.start_read,
               te_working_data.code,
               te_working_data.end_date,
               te_working_data.end_read,
               te_working_data.codea,
               te_working_data.te,
               te_working_data.te_vol,
               te_working_data.ms,
               te_working_data.ms_vol,
               te_working_data.no_iwcs,
               te_working_data.stage,
               te_working_data.reason,
               te_working_data.ouw_year,
               te_working_data.te_year,
               end_read_calculated
          FROM (SELECT md.period,
                       md.met_ref,
                       md.serial_no,
                       md.refdesc,
                       md.target_ref,
                       md.unit,
                       md.units,
                       md.start_date,
                       md.start_read,
                       md.code,
                       md.end_date,
                       md.end_read,
                       md.codea,
                       md.te,
                       md.te_vol,
                       md.ms,
                       md.ms_vol,
                       md.no_iwcs,
                       md.stage,
                       md.reason,
                       md.ouw_year,
                       md.te_year,
                       NVL(end_read,
                           NVL(start_read,0) + NVL(te_vol,0)
                          ) end_read_calculated,
                       ROW_NUMBER() OVER (PARTITION BY no_iwcs, met_ref
                                          ORDER BY no_iwcs, met_ref, period DESC, NVL(end_read,NVL(start_read,0) + NVL(te_vol,0)) DESC NULLS LAST) latest_iwcs  -- Amended to use end_read_calculated
                  FROM teaccess.meter_data md
                 WHERE period >= 15                                   -- Business rule
--                   AND start_date IS NOT NULL                       -- Filter inactive accounts
--                   AND NVL(end_date,sysdate) >= sysdate             -- Filter inactive accounts
               ) te_working_data
          -- Join to cus_data to derive the no_account
          LEFT OUTER JOIN teaccess.cus_data cd
            ON te_working_data.no_iwcs = cd.no_iwcs
          -- Join to lu_te_refdesc to obtain the te_revised_name and te_category
          LEFT OUTER JOIN lu_te_refdesc lu1
            ON te_working_data.refdesc = lu1.te_refdesc
         WHERE latest_iwcs = 1                                         -- Latest iwcs
--           AND cd.no_account IS NOT NULL;                             -- Filter rows without a no_account
       );

  TYPE tab_te_working IS TABLE OF cur_te_working%ROWTYPE INDEX BY PLS_INTEGER;
  t_te_working  tab_te_working;

 
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

   EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_TE_WORKING';


  l_progress := 'Processing Cursor';
  OPEN cur_te_working;

  LOOP
  
    FETCH cur_te_working BULK COLLECT INTO t_te_working LIMIT l_job.NO_COMMIT;  -- 999999
 
    FOR i IN 1..t_te_working.COUNT
    LOOP
    
      l_err.TXT_KEY := t_te_working(i).no_iwcs||','||t_te_working(i).met_ref;
      l_rec_written := TRUE;
      l_no_row_read := l_no_row_read + 1;

      BEGIN
        INSERT INTO bt_te_working (
           NO_ACCOUNT,              ACCOUNT_REF,       TE_REVISED_NAME,       TE_CATEGORY,             PERIOD,     
           MET_REF,                 SERIAL_NO,         REFDESC,               TARGET_REF,              UNIT,
           UNITS,                   START_DATE,        START_READ,            CODE,                    END_DATE,   
           END_READ,                CODEA,             TE,                    TE_VOL,                  MS,
           MS_VOL,                  NO_IWCS,           STAGE,                 REASON,                  OUW_YEAR,
           TE_YEAR,                 FA_YN,             FA_VOL,                DA_YN,                   DA_VOL,
           PA_YN,                   PA_PERC,           MDVOL_FOR_WS_METER_YN, MDVOL_FOR_WS_METER_PERC, MDVOL_FOR_TE_METER_YN,
           MDVOL_FOR_TE_METER_PERC, CALC_DISCHARGE_YN, CALC_DISCHARGE_VOL)
        VALUES (
           t_te_working(i).no_account,              t_te_working(i).no_account_ref,    t_te_working(i).te_revised_name,       t_te_working(i).te_category,              t_te_working(i).period,
           t_te_working(i).met_ref,                 t_te_working(i).serial_no,         t_te_working(i).refdesc,               t_te_working(i).target_ref,               t_te_working(i).unit,
           t_te_working(i).units,                   t_te_working(i).start_date,        t_te_working(i).start_read,            t_te_working(i).code,                     t_te_working(i).end_date,
           t_te_working(i).end_read_calculated,     t_te_working(i).codea,             t_te_working(i).te,                    t_te_working(i).te_vol,                   t_te_working(i).ms,
           t_te_working(i).ms_vol,                  t_te_working(i).no_iwcs,           t_te_working(i).stage,                 t_te_working(i).reason,                   t_te_working(i).ouw_year,
           t_te_working(i).te_year,                 t_te_working(i).fa_yn,             t_te_working(i).fa_vol,                t_te_working(i).da_yn,                    t_te_working(i).da_vol,
           t_te_working(i).pa_yn,                   t_te_working(i).pa_perc,           t_te_working(i).mdvol_for_ws_meter_yn, t_te_working(i).mdvol_for_ws_meter_perc,  t_te_working(i).mdvol_for_te_meter_yn,
           t_te_working(i).mdvol_for_te_meter_perc, t_te_working(i).calc_discharge_yn, t_te_working(i).calc_discharge_vol);
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
              CLOSE cur_te_working; 
              l_job.IND_STATUS := 'ERR';
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
              COMMIT;
              return_code := -1;
              RETURN; 
      END IF;

    END LOOP;

    IF t_te_working.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  CLOSE cur_te_working;


   -- write counts 
  l_progress := 'Writing Counts';  
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP50', 2570, l_no_row_read,    'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP50', 2580, l_no_row_dropped, 'Dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP50', 2590, l_no_row_insert,  'Written to Table ');    

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

     
END P_MOU_TRAN_TE_WORKING;
/
show error;

exit;

