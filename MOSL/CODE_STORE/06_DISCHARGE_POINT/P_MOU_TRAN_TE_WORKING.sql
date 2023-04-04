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
-- Subversion $Revision: 6368 $
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
-- V 0.02      07/06/2016  L.Smith    Column rounding
-- V 0.03      01/09/2016  L.Smith    Period 16 only
-- V 0.04      09/09/2016  L.Smith    Period 13 to present
-- V 0.05      27/09/2016  L.Smith    FAs may have TE usage
-- V 0.06      12/10/2016  L.Smith    TE must also exist as an added check for da_yn.
-- V 0.07      25/10/2016  S.Badhan   Check IWCS No exists in target
-- V 0.08      26/10/2016  S.Badhan   I-369. Set percentage value to zero if fixed charge.
-- V 0.09      31/10/2016  L.Smith    Ignore adjustments when setting PA marker.
--                                    PAs can't be 0 or 100 percent.
-- V 0.10      09/11/2016  L.Smith    Trim spaces from unit, code, codea columns.
--                                    da_vol calc use ouw_year not te_year
-- V 0.11      11/11/2016  L.Smith    New TE exclusion table lu_te_exclusion
-- V 0.12      17/11/2016  L.Smith    da_vol should be and absolute value
-- V 0.13      23/11/2016  L.Smith    Update no_iwcs 5193029700/1 to Water Meter (Shared refdesc)
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
  l_count                       NUMBER;  
  l_te_vol_calc                 BT_TE_WORKING.TE_VOL_CALC%TYPE;
  l_ouw_vol_calc                BT_TE_WORKING.OUW_VOL_CALC%TYPE;
  l_rec_written                 BOOLEAN;

-- SELECT Trade Effluent working data migrated from the customer database
-- Note: end_read_calculated has been used in calculations due to missing data
CURSOR cur_te_working
    IS
    SELECT no_iwcs,
           met_ref,
           period,
           stage,
           start_date,
           start_read,
           end_date,
           end_read,
           no_account,
           no_account_ref,
           fa_yn,
           CASE -- See spreadsheet working tab column (AD) Formula
              WHEN fa_yn = 'Y' AND te_revised_name IS NOT NULL THEN
                 CASE
--                    WHEN te_revised_name != 'Domestic' AND te <= 0 AND end_read_base IS NOT NULL AND start_read IS NOT NULL AND units IS NOT NULL THEN -- V0.05
                    WHEN UPPER(te_revised_name) != 'DOMESTIC' AND end_read_base IS NOT NULL AND start_read IS NOT NULL AND units IS NOT NULL THEN -- V0.05
                       CASE
                          WHEN te <= 0 THEN
                             ROUND((end_read_base + end_read_add2 - start_read) * units,2)
                          ELSE
                             ROUND((end_read_base + end_read_add2 - start_read) * units * -1,2)
                          END
                 END
           END AS fa_vol,
           da_yn,
           CASE -- See spreadsheet working tab column (AF) Formula
              WHEN da_yn = 'Y' AND unit IS NOT NULL THEN
                 CASE 
                    WHEN UPPER(unit) = 'HEAD' AND start_read IS NOT NULL AND end_read_base IS NOT NULL AND units IS NOT NULL THEN
                       ABS(ROUND((ouw_year / 2) * (end_read_base + end_read_add1) * units,0))
                    WHEN UPPER(unit) != 'HEAD' AND end_read_base IS NOT NULL AND units IS NOT NULL THEN
                       ABS(ROUND((end_read_base + end_read_add1 - start_read) * units,0))
                 END
           END AS da_vol,
           pa_yn,
           pa_perc,
--           CASE -- See spreadsheet working tab column (AH) Formula
--              WHEN pa_yn = 'Y' AND te IS NOT NULL THEN
--                 CASE 
--                    WHEN te > 0 AND te < 1 THEN
--                       ROUND(te,2)
--                    WHEN te > -1 AND te < 0 THEN
--                       ROUND((-1 * te),2)
--                 END
--           END AS pa_perc,
           mdvol_for_ws_meter_yn,
           CASE -- See spreadsheet working tab column (AJ) Formula
              WHEN mdvol_for_ws_meter_yn = 'Y' AND NVL(te,0) > 0 THEN
                 1
              WHEN mdvol_for_ws_meter_yn = 'Y' AND NVL(ms,1) > 0 THEN
                 0
           END AS mdvol_for_ws_meter_perc,
           mdvol_for_te_meter_yn,
           CASE -- See spreadsheet working tab column (AL) Formula
              WHEN mdvol_for_te_meter_yn = 'Y' AND NVL(te,0) > 0 THEN
                 1
              WHEN mdvol_for_te_meter_yn = 'Y' AND NVL(ms,1) > 0 THEN
                 0
           END AS mdvol_for_te_meter_perc,
           calc_discharge_yn,
           CASE -- See spreadsheet working tab column (AN) Formula
              WHEN end_read_base IS NULL OR start_read IS NULL OR UNITS IS NULL THEN
                 NULL
              WHEN calc_discharge_yn = 'Y' AND UPPER(te_category) = 'CALCULATED' THEN
                 ROUND((NVL(end_read,end_read_base + end_read_add2) - start_read) * units,2)
           END AS calc_discharge_vol,
           CASE -- See spreadsheet summary tab (P) Formula and (AM)
              WHEN start_read IS NULL AND end_read_base IS NULL THEN
                 NULL
              WHEN te_category LIKE '%Water Meter%' AND mdvol_for_ws_meter_yn = 'Y' AND NVL(te,0) > 0 THEN
                 ROUND((end_read_base - NVL(start_read,0)),0)
           END AS ws_vol,
           CASE -- See spreadsheet summary tab (Q) Formula
              WHEN start_read IS NULL AND end_read_base IS NULL THEN
                 NULL
              WHEN te_category IN ('Private TE Meter','Private Water Meter') THEN
                 ROUND(end_read_base - NVL(start_read,0),0)
           END AS sub_meter,
           te_revised_name,
           te_category,
           serial_no,
           refdesc,
           target_ref,
           unit,
           units,
           code,
           codea,
           te,
           te_vol,
           ms,
           ms_vol,
           reason,
           ouw_year,
           te_year,
           end_read_add1,
           end_read_add2,
           end_read_base,
           CASE -- end_read_calculation calculates the end_read column where no data exists in teaccess.meter_data
              WHEN end_read IS NOT NULL THEN
                 end_read
              WHEN te_category IN ('Private TE Meter','Private Water Meter') OR te_category LIKE '%Water Meter%' THEN
                 end_read_base + end_read_add3
              WHEN da_yn = 'Y' THEN
                 end_read_base + end_read_add1
              ELSE
                 end_read_base + end_read_add2
           END AS end_read_calculated,
           CASE  -- end_read_check is a recalculation of the end_read column for all no_iwcs rows to validate the results match existing end_read data (Proof test).
              WHEN te_category IN ('Private TE Meter','Private Water Meter') OR te_category LIKE '%Water Meter%' THEN
                 end_read_base + end_read_add3
              WHEN da_yn = 'Y' THEN
                 end_read_base + end_read_add1
              ELSE
                 end_read_base + end_read_add2
           END AS end_read_check,
           CASE -- See spreadsheet summary tab (R) Formula
              WHEN UPPER(te_category) = 'TE METER' THEN
                 NVL(te_vol,0)
              ELSE
                 0
           END AS te_vol_filtered,
           te_exclusion_yn
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
               CASE -- Derive FA_YN marker using excel rules (See Spreadsheet Calculations with MO Rules) **** Amended to match work sheet cell rules ADnn
                  WHEN UPPER(lu1.te_category) = 'FIXED' THEN -- V0.05 AND te_working_data.te <= 0 THEN
                     'Y'
                  ELSE
                     'N'
               END AS fa_yn,
               CASE  -- Derive DA_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN UPPER(lu1.te_category) = 'DOMESTIC'
                       AND NVL(te,0) != 0 THEN   -- V0.06
                     'Y'
                  ELSE
                     'N'
               END AS da_yn,
               CASE  -- Derive PA_YN marker using excel rules (See Spreadsheet Calculations with MO Rules) **** Amended to match work sheet cell rules AHnn
--                  WHEN (te_working_data.te > 0 and te_working_data.te < 1)
--                       OR (te_working_data.te > -1 and te_working_data.te < 0) THEN
                  WHEN NVL(te_cnt,0) > 1
                       AND NVL(te_min,0) = NVL(te_max,0) 
                       AND ABS(NVL(te_min,0)) NOT IN (0,1) THEN
                     'Y'
                  ELSE
                     'N'
               END AS pa_yn,
               CASE  -- Derive MDVOL_FOR_WS_METER_YN marker using excel rules (See Spreadsheet Calculations with MO Rules) **** Amended to match work sheet cell rules AJnn
                  WHEN UPPER(lu1.te_category) = 'WATER METER' AND (NVL(te_working_data.te,0) > 0 OR NVL(te_working_data.ms,1) > 0) THEN
                     'Y'
                  ELSE
                     'N'
               END AS mdvol_for_ws_meter_yn,
               CASE  -- Derive MDVOL_FOR_TE_METER_YN marker using excel rules (See Spreadsheet Calculations with MO Rules) **** Amended to match work sheet cell rules ALnn
                  WHEN UPPER(lu1.te_category) LIKE '%TE%' AND (NVL(te_working_data.te,0) > 0 OR NVL(te_working_data.ms,1) > 0) THEN
                     'Y'
                  ELSE
                     'N'
               END AS mdvol_for_te_meter_yn,
               CASE  -- Derive CALC_DISCHARGE_YN marker using excel rules (See Spreadsheet Calculations with MO Rules)
                  WHEN UPPER(lu1.te_category) = 'CALCULATED' THEN
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
               end_read_add1,
               end_read_add2,
               end_read_add3,
               end_read_base,
               CASE  
                  WHEN NVL(te_cnt,0) > 1
                       AND NVL(te_min,0) = NVL(te_max,0) 
                       AND ABS(NVL(te_min,0)) NOT IN (0,1) THEN
                     te_min
                  ELSE
                     0
               END AS pa_perc,
               DECODE(NVL(TO_CHAR(lte.no_iwcs),'N'),'N','N','Y') te_exclusion_yn
          FROM (SELECT md.no_iwcs,
                       md.period,
                       md.stage,
                       md.met_ref,
                       md.serial_no,
                       md.refdesc,
                       md.target_ref,
                       TRIM(md.unit) unit,
                       md.units,
                       md.start_date,
                       NVL(md.start_read,0) start_read,
                       TRIM(md.code) code,
                       md.end_date,
                       TO_NUMBER(md.end_read) end_read,
                       TRIM(md.codea) codea,
                       md.te,
                       md.te_vol,
                       md.ms,
                       md.ms_vol,
                       md.reason,
                       md.ouw_year,
                       md.te_year,
                       NVL(md.end_read,
                           NVL(md.start_read,0)
                          ) end_read_base,
                       CASE
                          WHEN md.end_read IS NULL THEN
                             -- translate negative values to positive
                             NVL(ABS(md.te_vol),0) * 10
                          ELSE
                             0   
                       END AS end_read_add1,
                       CASE
                          WHEN end_read IS NULL THEN
                             -- translate negative values to positive
                             DECODE(SIGN( NVL(md.te_vol/md.te,0)), 1,NVL(md.te_vol/md.te,0), -1,NVL(md.te_vol/md.te,0)*-1, 0)
                          ELSE
                             0
                       END AS end_read_add2,
                       CASE
                          WHEN md.end_read IS NULL THEN
                             -- translate negative values to positive
                             DECODE(  SIGN( NVL(md.ms_vol,0)), 1,NVL(md.ms_vol,0), -1,NVL(md.ms_vol,0)*-1, 0)
                          ELSE
                             0
                       END AS end_read_add3,
                       pa_ilv.te_min,
                       pa_ilv.te_max,
                       pa_ilv.te_cnt,
                       ROW_NUMBER() OVER (PARTITION BY md.no_iwcs, md.period, md.stage, md.met_ref
                                          ORDER BY md.no_iwcs, md.period DESC, md.stage, md.met_ref, 
                                                   NVL(md.end_read,0) DESC, NVL(md.start_read,0) DESC, NVL(md.te_vol,0) DESC ) latest_iwcs
                  FROM teaccess.meter_data md
                  LEFT OUTER JOIN 
                     (SELECT no_iwcs,
                             period,
                             MIN(ABS(NVL(te,0))) te_min,
                             MAX(ABS(NVL(te,0))) te_max,
                             COUNT(*) te_cnt
                        FROM teaccess.meter_data md
                        LEFT OUTER JOIN lu_te_refdesc ltr
                          ON md.refdesc = ltr.te_refdesc
                        WHERE period >= 13
                          AND UPPER(NVL(ltr.te_category,'NOT FOUND')) != 'ADJUSTMENT'  -- Ignore adjustments when setting PA marker
                        GROUP BY no_iwcs, period) pa_ilv
                    ON pa_ilv.no_iwcs = md.no_iwcs
                       AND pa_ilv.period = md.period
                 WHERE md.period >= 13                                    -- Business currently requires period 16 data only
                   AND REPLACE(TRANSLATE(md.end_read,'123456789.','0000000000'),'0','') IS NULL   -- Non numeric character check
                   AND REPLACE(TRANSLATE(md.start_read,'123456789.','0000000000'),'0','') IS NULL -- Non numeric characters check
               ) te_working_data
          -- Join to cus_data to derive the no_account
          LEFT OUTER JOIN teaccess.cus_data cd
            ON te_working_data.no_iwcs = cd.no_iwcs
          -- Join to lu_te_refdesc to obtain the te_revised_name and te_category
          LEFT OUTER JOIN lu_te_refdesc lu1
            ON te_working_data.refdesc = lu1.te_refdesc
          LEFT OUTER JOIN lu_te_exclusion lte
            ON te_working_data.no_iwcs = lte.no_iwcs
         WHERE latest_iwcs = 1                                         -- Latest iwcs
--           AND cd.no_account IS NOT NULL;                             -- Filter rows without a no_account
       )
 ORDER BY no_iwcs, period DESC, stage, met_ref;

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

      l_te_vol_calc := ROUND(NVL(t_te_working(i).fa_vol,0)/10 + 
                             NVL(t_te_working(i).calc_discharge_vol,0) +
                             NVL(t_te_working(i).ws_vol,0),
                             0
                            );

      l_ouw_vol_calc := ROUND(NVL(t_te_working(i).ms_vol,0),0);

      -- Error if NO_ICS does not exist on Target

      -- Exception NO_IWCS is marked for exclusion      
      IF t_te_working(i).te_exclusion_yn = 'Y' AND l_rec_written THEN
         l_no_row_dropped := l_no_row_dropped + 1;
         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'NO_IWCS has been excluded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
         l_no_row_exp := l_no_row_exp + 1;
      END IF;
      
      l_progress := 'SELECT BT_SPR_TARIFF_EXTREF'; 
      
      IF l_rec_written THEN
         SELECT COUNT(*)
         INTO   l_count
         FROM   BT_SPR_TARIFF_EXTREF
         WHERE  CD_EXT_REF     = TO_CHAR(lpad(t_te_working(i).no_iwcs,11,'0'))
         AND    TP_ENTITY_332    = 'S' 
         AND    NO_EXT_REFERENCE = 4
         AND    TRIM(CD_SERV_PROV) = 'TW';

         IF l_count = 0 THEN
            l_no_row_dropped := l_no_row_dropped + 1;
            l_rec_written := FALSE;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Teaccess NO_IWCS does not exist in Target',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
      END IF;
      
      IF l_rec_written THEN          
         l_progress := 'INSERT BT_TE_WORKING'; 
          BEGIN
            INSERT INTO bt_te_working (
               NO_ACCOUNT,            ACCOUNT_REF,             PERIOD,                 STAGE,
               TE_REVISED_NAME,       TE_CATEGORY,             MET_REF,                SERIAL_NO,
               REFDESC,               TARGET_REF,              UNIT,                   UNITS,
               START_DATE,            START_READ,              CODE,                   END_DATE,
               END_READ,              CODEA,                   TE,                     TE_VOL,
               MS,                    MS_VOL,                  NO_IWCS,                REASON,
               OUW_YEAR,              TE_YEAR,                 FA_YN,                  FA_VOL,
               DA_YN,                 DA_VOL,                  PA_YN,                  PA_PERC,
               MDVOL_FOR_WS_METER_YN, MDVOL_FOR_WS_METER_PERC, MDVOL_FOR_TE_METER_YN,  MDVOL_FOR_TE_METER_PERC,
               CALC_DISCHARGE_YN,     CALC_DISCHARGE_VOL,      WS_VOL,                 SUB_METER,
               TE_VOL_FILTERED,       TE_VOL_CALC,           OUW_VOL_CALC)
            VALUES (
               t_te_working(i).no_account,            t_te_working(i).no_account_ref,                  t_te_working(i).period,                t_te_working(i).stage,
               t_te_working(i).te_revised_name,       t_te_working(i).te_category,                     t_te_working(i).met_ref,               t_te_working(i).serial_no,
               t_te_working(i).refdesc,               t_te_working(i).target_ref,                      t_te_working(i).unit,                  t_te_working(i).units,
               t_te_working(i).start_date,            t_te_working(i).start_read,                      t_te_working(i).code,                  t_te_working(i).end_date,
               t_te_working(i).end_read_calculated,   t_te_working(i).codea,                           t_te_working(i).te,                    t_te_working(i).te_vol,
               t_te_working(i).ms,                    NVL(t_te_working(i).ms_vol,0),                   t_te_working(i).no_iwcs,               t_te_working(i).reason,
               t_te_working(i).ouw_year,              t_te_working(i).te_year,                         t_te_working(i).fa_yn,                 NVL(t_te_working(i).fa_vol,0),
               t_te_working(i).da_yn,                 NVL(t_te_working(i).da_vol,0),                   t_te_working(i).pa_yn,                 NVL(t_te_working(i).pa_perc,0),
               t_te_working(i).mdvol_for_ws_meter_yn, t_te_working(i).mdvol_for_ws_meter_perc,         t_te_working(i).mdvol_for_te_meter_yn, t_te_working(i).mdvol_for_te_meter_perc,
               t_te_working(i).calc_discharge_yn,     NVL(t_te_working(i).calc_discharge_vol,0),       NVL(t_te_working(i).ws_vol,0),         NVL(t_te_working(i).sub_meter,0),
               t_te_working(i).te_vol_filtered,       l_te_vol_calc,                                   l_ouw_vol_calc);
          EXCEPTION
              WHEN OTHERS THEN 
              IF t_te_working(i).period = 16 THEN
                 l_no_row_dropped := l_no_row_dropped + 1;
                 l_rec_written := FALSE;
                 l_error_number := SQLCODE;
                 l_error_message := SQLERRM;
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 l_no_row_err := l_no_row_err + 1;
              END IF;
          END;
    
          IF l_rec_written AND NVL(t_te_working(i).end_read_calculated,0) != NVL(t_te_working(i).end_read_check,0) THEN
             P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, 
                                     L_JOB.NO_INSTANCE, 
                                     'W', 
                                     SUBSTR('end_read imbalance Warning',1,100),  
                                     L_ERR.TXT_KEY, SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
             l_no_row_war := l_no_row_war + 1;
          END IF;

      END IF;

      -- keep count of records written
      IF l_rec_written THEN
         l_no_row_insert := l_no_row_insert + 1;
      ELSIF (l_no_row_exp > l_job.EXP_TOLERANCE
             OR l_no_row_err > l_job.ERR_TOLERANCE
             OR l_no_row_war > l_job.WAR_TOLERANCE) THEN
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

   -- Update no_iwcs 5193029700/1 to Water Meter (Shared refdesc).
   UPDATE bt_te_working
      SET te_revised_name = 'Water Supply Meter',
          te_category = 'Water Meter'
    WHERE no_iwcs = 5193029700 
      AND met_ref = 1
      AND refdesc = 'Domestic Meter';
   
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