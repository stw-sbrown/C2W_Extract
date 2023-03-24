--------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS CREATION 
--
-- AUTHOR         		: 	Lee Smith
--
-- FILENAME       		: 	07_DDL_MOSL_VIEWS_ALL.sql
--
-- CREATED        		: 	13/10/2016
--
-- Subversion $Revision: 6380 $
--	
--
-- DESCRIPTION 		   	: 	Creates views MOSL database
--
--
--
---------------------------- Modification History ------------------------------
--
-- Version     		Date                Author         		         Description
-- ---------      ---------------     -------------            	 ---------------
-- V0.01       	  13/10/2016    	    Lee Smith     	           Initial version
-- V0.02          01/11/2016          Lee Smith                  TE_SUB_METERS_TO_V Cross Border meters
-- V0.03          01/11/2016          D Cheung                   Add METER_READING_CDV_V and METER_YEARLYVOLEST_V
-- V0.04          02/11/2016          Lee Smith                  TE  changes (PA).
-- V0.05          02/11/2016          D Cheung                   METER_READING_CDV_V - fix to date processing in calc
-- V0.06          07/11/2016          Lee Smith                  TE_SUB_METERS_TO Number conversion
-- V0.07          09/11/2016          Lee Smith                  New view. te_read_tolerance.
-- V0.08          16/11/2016          Lee Smith                  New materialized views 5_MV and 6_MV.
-- V0.09          18/11/2016          Lee Smith                  Added revision and remove moutran from name
--                                                               Remove comment as it upsets Linux
-- V0.10          18/11/2016          Lee Smith                  Sub Meters can not be Calculated
-- V0.11          21/11/2016          Lee Smith                  TE_TRANSFORMED_TAB_V view. Negative calulated values.
-- V0.12          22/11/2016          Lee Smith                  Trim leading zeros mv6 manufacturerserialnum_pk
-- V0.13          22/11/2016          Lee Smith                  Allow all cross borders into view mv5

CREATE OR REPLACE VIEW te_working_v AS
  SELECT CASE
            WHEN btw.no_iwcs IN (
                 1211000101,2424005001,2424034201,2424034202,2427012801,3018002200,3018003301,3018003701,3018003801,3018004101,
                 3018004201,3018004301,3028001700,3028002100,3028004600,3028010600,3028010900,3028011700,3028012200,3028013100,
                 3028014800,3028019600,3028019900,3028020500,3028020700,3028020900,3028021200,3028021300,3028021601,3028165000,
                 3049001700,3049002100,3049002400,3049002500,3049002901,3049003101,3049021800,3049021901,3049022001,3051024301,
                 3064000500,3064000700,3064001900,3064001901,3064002400,3064002402,3064005500,3064005600,3064008700,3064009700,
                 3064010800,3064010801,3064013500,3064023901,3064024001,3064024101,3064024801,3064025001,3064025301,3064025401,
                 3064026001,3064026801,3064026901,3064027201,3064027301,3064028001,3064028301,3064028601,3064028801,3064029001,
                 3064029101,3064029201,3064029501,3064029601,3064029701,3064029801,3064030001,3079027201,3800005801,3800005901,
                 3807001100,3807001700,3807002300,3807002600,3807002700,3807004501,3807004901,3807004902,3807005001,3807005101,
                 3807050200,3807050301,3807050401,3807050501,3807050601,3807050701,3807050801,5009002700,5009002701,5009004400,
                 5009004700,5009005700,5009005801,5012000300,5012000400,5012002900,5012004600,5012004900,5012005800,5012006200,
                 5012008100,5012008600,5012008900,5012011500,5012012400,5012013500,5012013700,5012014400,5012016600,5012017200,
                 5012017300,5012017400,5012017600,5012018200,5012018900,5012019200,5012019400,5012019500,5012020301,5012020401,
                 5012020901,5012021301,5012021302,5012021601,5012021602,5012021701,5012021801,5012022101,5012022201,5012022301,
                 5026000300,5026000400,5032000900,5032002800,5032004700,5032004900,5032005000,5032005700,5032006000,5032006700,
                 5032007200,5032007300,5032008001,5032008101,5032008201,5032008301,5032021601,5032021701,5032021901,5032022201,
                 5032022301,5032022401,5062000800,5062000801,5062000802,5062000900,5062001402,5062004800,5062005602,5062006600,
                 5062006601,5062007100,5062007101,5062007800,5062008700,5062009400,5062011700,5062013500,5062013502,5062013503,
                 5062013600,5062014600,5062014601,5062015700,5062016400,5062016800,5062017200,5062017600,5062017900,5062018201,
                 5062018500,5062018700,5062019200,5062019500,5062019600,5062020000,5062020100,5062020400,5062020900,5062023201,
                 5062023202,5062023401,5062023501,5062023502,5062023801,5062024101,5062024301,5062024801,5062024901,5062025401,
                 5062025501,5062025801,5062025901,5062026001,5062026101,5062026202,5062026401,5062026701,5062026801,5062027101,
                 5062027201,5062027801,5062027901,5062028101,5062028201,5062028301,5062028401,5062028501,5062028601,5062028701,
                 5062028901,5062029001,5062029401,5062029501,5062029701,5062029801,5062029901,5062030001,5062030002,5062030101,
                 5062030201,5062030301,5062030302,5062030401,5062030501,5062030801,5062030901,5062031001,5062031101,5062031201,
                 5062031301,5062031601,5062031701,5062031801,5062031901,5065000300,5065000301,5065000303,5065000304,5065000900,
                 5065000903,5065004500,5065004600,5065004701,5065005300,5065005500,5065006100,5065007400,5065007800,5065008400,
                 5065008401,5065008500,5065023900,5065024101,5075001600,5075002200,5075002500,5076000100,5076000400,5076003300,
                 5076003800,5076003801,5076004000,5076007300,5076007800,5076008800,5076009100,5076009301,5076009501,5076009601,
                 5080000501,5080000800,5080001400,5080002200,5080002300,5080002501,5080002701,5090003600,5090003700,5090009000,
                 5090011300,5090027801,5090027901,5090028601,5090032101,5090032301,5090032401,5090032601,5191059301,5192066901,
                 5192068001,5193057800,5194064600,5194066501,5291062600,5291067300,5291068301,5292014500,5800002900,5800005801,
                 5800005901,5912002400,5912050100,5912050300,5912050401,5912050501,5914000100,5914000400,5920003000,5920003002,
                 5920003200,5920003800,5920004500,5920005600,5920006500,5920006600,5920007400,5920007500,5920007800,5920008000,
                 5920009101,5920009301,5920009401,5920009501,5920009601,5920009701,5920009801,5920010101,5920050100,5920050200,
                 5920050401,5920050601,5920051201,5920051301,5920051601,5920051801,5921000400,5921002000,5921002201,5921002301,
                 5921002302,5921002401,5921002501,5921002601,5923000301,5923000901,5925000701,6133301101,6133301701,6133308001,
                 6133314301,6133314401,6133314501,8026260301,8026260401,8065006701,8065656801,8065656901,11301005201,12504000301,
                 12507000701,12507001101,12508000301,12509000401,12509001101,12509001401,12509001601,12509001901,12509002101,12509002301,
                 12509002401,12509002501,12514000101,12514000201,12518002501,12518003001,12605002201,12605002401,12605002601,12605002701,
                 12605002801,12605003001,12605003101,12605003201,12605003301,12606000101,12606000501,12606000601,12615001001,12615001002,
                 12615001301,12615002701,12615002703,12615003101,12615003201,12615003202,12615003301,12615003701,12615003901,12615004101,
                 12615004201,12615004401,12615004501,12615004601,12615004901,12615005001,12616000501,12616000601,12616000602,12616002201,
                 12616002601,12616002701,12616002801,12616002901,12616003001,12616003002,12620000101,12620000201,12621000101,12621000501,
                 12629000101,12632000401,12632000501,12633000101,12635000101,12636000801,12636000901,12636001001,12636001101,12636001201,
                 14304000101,14401001501,14401001701,14401002501,14401003301,14401003801,14401004301,14401004501,14401006301,14401006401,
                 14401007201,14401008301,14401008302,14401008401,14401009101,14401009201,14401009301,14401009401,14401011301,14401012001,
                 14401012801,14401013101,14401013201,14401015201,14401015301,14401015401,14401015602,14401015901,14401016001,14401016201,
                 14401016501,14401016601,14401016701,14401016801,14401017201,14401017301,14401017401,14404000501,14404000601,14405000201,
                 14421000601,14421002301,14421002801,14427000501,14427000601,14427001001,14427001101,14427001102,14427001301,14427001901,
                 14427002001,14427002600,14427002901,14427003101,14427003201,14427003301,14427003302,14427003401,14427003402,14432000501,
                 14442000201,14999000301,15264001001,12616000501,5062009400) THEN
               'Y'
              ELSE
               'N'
         END AS has_cross_border_water_yn,
         CASE
            WHEN ilv.txt_data IS NOT NULL THEN
               ilv.txt_data
            ELSE
               TO_CHAR(dp.no_iwcs)
         END AS has_iwcs_dp,
         btw.*
    FROM bt_te_working btw
    LEFT OUTER JOIN mo_discharge_point dp
      ON btw.no_iwcs = dp.no_iwcs
    LEFT OUTER JOIN (
                      SELECT elog.txt_data,
                             TO_NUMBER(TRIM(LTRIM(SUBSTR(txt_data,-11),0))) no_iwcs,
                             SUBSTR(elog.txt_key,1,instr(txt_key,',')-1) stwpropertynumber_pk,
                             SUBSTR(elog.txt_key,instr(txt_key,',')+1) address_pk,
                          ROW_NUMBER() OVER (PARTITION BY SUBSTR(txt_data,-11)
                                             ORDER BY txt_data) dp_err_exc
                    FROM mig_errorlog elog
                    JOIN mig_errref   eref
                      ON elog.no_err = eref.no_err
                    LEFT OUTER JOIN mig_jobstatus jstatus
                      ON elog.no_batch = jstatus.no_batch
                         AND elog.no_instance = jstatus.no_instance
                   WHERE elog.no_batch = (SELECT MAX(no_batch)
                                            FROM mig_batchstatus)
                     AND elog.ind_log IN ('E','X')
                     AND txt_arg = 'P_MOU_TRAN_DISCHARGE_POINT'
                     AND TRIM(TRANSLATE(LTRIM(SUBSTR(txt_data,-11),0),'0123456789',' ')) IS NULL
                   ) ilv
     ON btw.no_iwcs = ilv.no_iwcs
        AND dp_err_exc = 1
  WHERE period = 16;

--
-- TE_IWCS_METERS
--
CREATE OR REPLACE VIEW te_iwcs_meters_v AS
SELECT no_iwcs,
       SUM(
       CASE
          WHEN UPPER(te_category) = 'WATER METER' AND NVL(te,0) != 0 THEN
             1
          ELSE
             0
       END) AS wsm_in_te_vol_calc_cnt,
       SUM(
       CASE
          WHEN UPPER(te_category) = 'PRIVATE TE METER' AND NVL(te,0) != 0  THEN
             1
          ELSE
             0
       END) AS ptem_in_te_vol_calc_cnt,
       SUM(
       CASE
          WHEN UPPER(te_category) = 'PRIVATE WATER METER' AND NVL(te,0) != 0  THEN
             1
          ELSE
             0
       END) AS pwm_in_te_vol_calc_cnt,
       SUM(
       CASE
          WHEN UPPER(te_category) = 'WATER METER' AND NVL(ms_vol,0) != 0 THEN
             1
          ELSE
             0
       END) AS wsm_in_ouw_vol_calc_cnt,
       SUM(
       CASE
          WHEN UPPER(te_category) = 'PRIVATE TE METER' AND NVL(ms_vol,0) != 0  THEN
             1
          ELSE
             0
       END) AS ptem_in_ouw_vol_calc_cnt,
       SUM(
       CASE
          WHEN UPPER(te_category) = 'PRIVATE WATER METER' AND NVL(ms_vol,0) != 0  THEN
             1
          ELSE
             0
       END) AS pwm_in_ouw_vol_calc_cnt       
  FROM te_working_v
 WHERE UPPER(te_category) IN ('WATER METER', 'PRIVATE TE METER', 'PRIVATE WATER METER')
 GROUP BY no_iwcs;

--
-- TE_MATCHED_WATER_METERS1_MV
--
CREATE MATERIALIZED VIEW TE_MATCHED_WATER_METERS1_MV REFRESH COMPLETE AS
  SELECT /*+REWRITE(TE_MATCHED_WATER_METERS1_MV)*/-- MATCHED ON CORESPID, SERIAL_NO
    DISTINCT btw.te_category,
    SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
    m.spid_pk,
    m.manufacturer_pk,
    btw.serial_no,
    btw.met_ref,
    m.manufacturerserialnum_pk,
    'N' matched_cross_border,
    CASE
      WHEN m.spid_pk LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
        ||'%'
      THEN 'Y'
      ELSE 'N'
    END AS matched_corespid_yn,
    CASE
      WHEN RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X') = RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X')
      THEN 'Y'
      ELSE 'N'
    END AS matched_serialno_yn,
    CASE
      WHEN readings_ilv.meterread IS NOT NULL
      THEN 'Y'
      ELSE 'N'
    END AS matched_endread_yn,
    CASE
      WHEN
        CASE
          WHEN btw.start_read = readings_ilv.startread_previous1
          THEN readings_ilv.startread_previous1
          WHEN btw.start_read = readings_ilv.startread_previous2
          THEN readings_ilv.startread_previous2
          WHEN btw.start_read = readings_ilv.startread_previous3
          THEN readings_ilv.startread_previous3
          WHEN btw.start_read = readings_ilv.startread_previous4
          THEN readings_ilv.startread_previous4
          WHEN btw.start_read = readings_ilv.startread_previous5
          THEN readings_ilv.startread_previous5
          WHEN btw.start_read = readings_ilv.startread_previous6
          THEN readings_ilv.startread_previous6
        END IS NOT NULL
      THEN 'Y'
      ELSE 'N'
    END AS matched_usage_readings_yn,
    CASE
       WHEN btw.pa_yn = 'Y' THEN
          100
       ELSE
          ABS(btw.te)*100
    END AS mdvol,
--    DECODE((1    -ABS(NVL(btw.ms,0))), 0, 1, 1,0, (1-ABS(NVL(btw.ms,0)))) returntosewer,
    ABS(NVL(btw.ms,0)) returntosewer,
    m.numberofdigits,
    m.metertreatment,
    m.measureunitfreedescriptor,
    m.measureunitatmeter,
    m.meterreadfrequency,
    m.nonmarketmeterflag,
    m.installedpropertynumber,
    dp.dpid_pk,
    btw.no_iwcs no_iwcs_working,
    btw.met_ref met_ref_working,
    btw.period period_working,
    btw.stage stage_working,
    btw.start_date start_date_working,
    btw.end_date end_date_working,
    btw.start_read start_read_working,
    btw.end_read end_read_working,
    btw.end_read - btw.start_read period_usage_working,
    btw.te te_working,
    btw.te_vol te_vol_working,
    btw.ms ms_working,
    btw.ms_vol ms_vol_working,
    readings_ilv.meterreaddate meterreaddate_readings,
    readings_ilv.meterread meterread_readings,
    CASE
      WHEN btw.start_read = readings_ilv.startread_previous1
      THEN readings_ilv.startread_previous1
      WHEN btw.start_read = readings_ilv.startread_previous2
      THEN readings_ilv.startread_previous2
      WHEN btw.start_read = readings_ilv.startread_previous3
      THEN readings_ilv.startread_previous3
      WHEN btw.start_read = readings_ilv.startread_previous4
      THEN readings_ilv.startread_previous4
      WHEN btw.start_read = readings_ilv.startread_previous5
      THEN readings_ilv.startread_previous5
      WHEN btw.start_read = readings_ilv.startread_previous6
      THEN readings_ilv.startread_previous6
    END AS startread_readings,
    CASE
      WHEN btw.start_read = readings_ilv.startread_previous1
      THEN readings_ilv.meterread - readings_ilv.startread_previous1
      WHEN btw.start_read = readings_ilv.startread_previous2
      THEN readings_ilv.meterread - readings_ilv.startread_previous2
      WHEN btw.start_read = readings_ilv.startread_previous3
      THEN readings_ilv.meterread - readings_ilv.startread_previous3
      WHEN btw.start_read = readings_ilv.startread_previous4
      THEN readings_ilv.meterread - readings_ilv.startread_previous4
      WHEN btw.start_read = readings_ilv.startread_previous5
      THEN readings_ilv.meterread - readings_ilv.startread_previous5
      WHEN btw.start_read = readings_ilv.startread_previous6
      THEN readings_ilv.meterread - readings_ilv.startread_previous6
    END AS periodusage_readings
  FROM te_working_v btw
  LEFT OUTER JOIN lu_te_meter_pairing ltmp
    ON ltmp.no_iwcs_working = btw.no_iwcs
       AND ltmp.met_ref_working = btw.met_ref
  JOIN mo_discharge_point dp
  ON dp.no_iwcs = btw.no_iwcs
  JOIN mo_meter m
  ON m.spid_pk LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
    ||'W%'
  LEFT OUTER JOIN
    (SELECT manufacturer_pk,
      manufacturerserialnum_pk,
      meterreaddate,
      meterread,
      NVL(startread_previous1,0) startread_previous1,
      NVL(startread_previous2,0) startread_previous2,
      NVL(startread_previous3,0) startread_previous3,
      NVL(startread_previous4,0) startread_previous4,
      NVL(startread_previous5,0) startread_previous5,
      NVL(startread_previous6,0) startread_previous6
    FROM
      (SELECT manufacturer_pk,
        manufacturerserialnum_pk,
        TO_DATE(meterreaddate,'dd-mon-yy') meterreaddate,
        meterread,
        LAG(meterread,1) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous1,
        LAG(meterread,2) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous2,
        LAG(meterread,3) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous3,
        LAG(meterread,4) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous4,
        LAG(meterread,5) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous5,
        LAG(meterread,6) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous6
      FROM mo_meter_reading mr
        --                WHERE manufacturer_pk = 'SCHLUMBERGER'
        --                 AND manufacturerserialnum_pk = '081021933'
        -- USE PERIOD 16 CYCLE 1
      WHERE meterreaddate <
        (SELECT MAX(ADD_MONTHS(TO_DATE(ltbc_finish,'dd-mon-yy'),1))
        FROM lu_te_billing_cycle
        WHERE ltbc_period     = 16
        )
      )
    WHERE meterread > startread_previous1
      -- and manufacturerserialnum_pk = '04243104'
      -- and manufacturerserialnum_pk = '12M114460'
    ) readings_ilv
  ON m.manufacturer_pk                                                          = readings_ilv.manufacturer_pk
  AND m.manufacturerserialnum_pk                                                = readings_ilv.manufacturerserialnum_pk
  AND btw.end_read                                                              = readings_ilv.meterread
  WHERE RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X') IS NOT NULL
  AND RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X')    = RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X')
  AND NVL(te_pairing_override_yn,'N') != 'Y';
  
--
-- TE_MATCHED_WATER_METERS2_MV
--
CREATE MATERIALIZED VIEW TE_MATCHED_WATER_METERS2_MV REFRESH COMPLETE AS
  SELECT /*+REWRITE(TE_MATCHED_WATER_METERS2_MV)*/ -- MATCHED ON CORESPID, METERREAD
    -- UNMATCHED SERIAL_NO
    DISTINCT btw.te_category,
    SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
    m.spid_pk,
    m.manufacturer_pk,
    btw.serial_no,
    btw.met_ref,
    m.manufacturerserialnum_pk,
    'N' matched_cross_border,
    CASE
      WHEN m.spid_pk LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
        ||'%'
      THEN 'Y'
      ELSE 'N'
    END AS matched_corespid_yn,
    CASE
      WHEN RTRIM(RTRIM(LTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X') = RTRIM(RTRIM(LTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X')
      THEN 'Y'
      ELSE 'N'
    END AS matched_serialno_yn,
    CASE
      WHEN readings_ilv.meterread IS NOT NULL
      THEN 'Y'
      ELSE 'N'
    END AS matched_endread_yn,
    CASE
      WHEN
        CASE
          WHEN btw.start_read = readings_ilv.startread_previous1
          THEN readings_ilv.startread_previous1
          WHEN btw.start_read = readings_ilv.startread_previous2
          THEN readings_ilv.startread_previous2
          WHEN btw.start_read = readings_ilv.startread_previous3
          THEN readings_ilv.startread_previous3
          WHEN btw.start_read = readings_ilv.startread_previous4
          THEN readings_ilv.startread_previous4
          WHEN btw.start_read = readings_ilv.startread_previous5
          THEN readings_ilv.startread_previous5
          WHEN btw.start_read = readings_ilv.startread_previous6
          THEN readings_ilv.startread_previous6
        END IS NOT NULL
      THEN 'Y'
      ELSE 'N'
    END AS matched_usage_readings_yn,
    CASE
       WHEN btw.pa_yn = 'Y' THEN
          100
       ELSE
          ABS(btw.te)*100
    END AS mdvol,
--    DECODE((1    -ABS(NVL(btw.ms,0))), 0, 1, 1,0, (1-ABS(NVL(btw.ms,0)))) returntosewer,
    ABS(NVL(btw.ms,0)) returntosewer,
    m.numberofdigits,
    m.metertreatment,
    m.measureunitfreedescriptor,
    m.measureunitatmeter,
    m.meterreadfrequency,
    m.nonmarketmeterflag,
    m.installedpropertynumber,
    dp.dpid_pk,
    btw.no_iwcs no_iwcs_working,
    btw.met_ref met_ref_working,
    btw.period period_working,
    btw.stage stage_working,
    btw.start_date start_date_working,
    btw.end_date end_date_working,
    btw.start_read start_read_working,
    btw.end_read end_read_working,
    btw.end_read - btw.start_read period_usage_working,
    btw.te te_working,
    btw.te_vol te_vol_working,
    btw.ms ms_working,
    btw.ms_vol ms_vol_working,
    readings_ilv.meterreaddate meterreaddate_readings,
    readings_ilv.meterread meterread_readings,
    CASE
      WHEN btw.start_read = readings_ilv.startread_previous1
      THEN readings_ilv.startread_previous1
      WHEN btw.start_read = readings_ilv.startread_previous2
      THEN readings_ilv.startread_previous2
      WHEN btw.start_read = readings_ilv.startread_previous3
      THEN readings_ilv.startread_previous3
      WHEN btw.start_read = readings_ilv.startread_previous4
      THEN readings_ilv.startread_previous4
      WHEN btw.start_read = readings_ilv.startread_previous5
      THEN readings_ilv.startread_previous5
      WHEN btw.start_read = readings_ilv.startread_previous6
      THEN readings_ilv.startread_previous6
    END AS startread_readings,
    CASE
      WHEN btw.start_read = readings_ilv.startread_previous1
      THEN readings_ilv.meterread - readings_ilv.startread_previous1
      WHEN btw.start_read = readings_ilv.startread_previous2
      THEN readings_ilv.meterread - readings_ilv.startread_previous2
      WHEN btw.start_read = readings_ilv.startread_previous3
      THEN readings_ilv.meterread - readings_ilv.startread_previous3
      WHEN btw.start_read = readings_ilv.startread_previous4
      THEN readings_ilv.meterread - readings_ilv.startread_previous4
      WHEN btw.start_read = readings_ilv.startread_previous5
      THEN readings_ilv.meterread - readings_ilv.startread_previous5
      WHEN btw.start_read = readings_ilv.startread_previous6
      THEN readings_ilv.meterread - readings_ilv.startread_previous6
    END AS periodusage_readings
  FROM te_working_v btw
  LEFT OUTER JOIN lu_te_meter_pairing ltmp
    ON ltmp.no_iwcs_working = btw.no_iwcs
       AND ltmp.met_ref_working = btw.met_ref
  JOIN mo_discharge_point dp
  ON dp.no_iwcs = btw.no_iwcs
  JOIN mo_meter m
  ON m.spid_pk LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
    ||'W%'
  LEFT OUTER JOIN
    (SELECT manufacturer_pk,
      manufacturerserialnum_pk,
      meterreaddate,
      meterread,
      NVL(startread_previous1,0) startread_previous1,
      NVL(startread_previous2,0) startread_previous2,
      NVL(startread_previous3,0) startread_previous3,
      NVL(startread_previous4,0) startread_previous4,
      NVL(startread_previous5,0) startread_previous5,
      NVL(startread_previous6,0) startread_previous6
    FROM
      (SELECT manufacturer_pk,
        manufacturerserialnum_pk,
        TO_DATE(meterreaddate,'dd-mon-yy') meterreaddate,
        meterread,
        LAG(meterread,1) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous1,
        LAG(meterread,2) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous2,
        LAG(meterread,3) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous3,
        LAG(meterread,4) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous4,
        LAG(meterread,5) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous5,
        LAG(meterread,6) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread_previous6
      FROM mo_meter_reading mr
        -- USE PERIOD 16 CYCLE 1
      WHERE meterreaddate <
        (SELECT MAX(ADD_MONTHS(TO_DATE(ltbc_finish,'dd-mon-yy'),1))
        FROM lu_te_billing_cycle
        WHERE ltbc_period     = 16
        )
      )
    WHERE meterread > startread_previous1
    ) readings_ilv
  ON m.manufacturer_pk                                                                                 = readings_ilv.manufacturer_pk
  AND m.manufacturerserialnum_pk                                                                       = readings_ilv.manufacturerserialnum_pk
  WHERE btw.serial_no                                                                                 IS NOT NULL
  AND btw.end_read                                                                                     = readings_ilv.meterread
  AND NVL(RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X'),'NOWORKINGSERIALNO') != NVL(RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X'),'NOMETERSERIALNO')
  AND NVL(te_pairing_override_yn,'N') != 'Y'
  AND NOT EXISTS
                            -- WATER METERS ALREADY FOUND
                            (SELECT 'Y' 
                               FROM te_working_v tw2
                               JOIN mo_discharge_point dp
                                 ON dp.no_iwcs = tw2.no_iwcs
                               JOIN mo_meter_spid_assoc msa
                                 ON SUBSTR(msa.spid,1,LENGTH(msa.spid)-2) = SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)||'W'
                               JOIN mo_meter m
                                 ON msa.manufacturer_pk = m.manufacturer_pk
                                    AND msa.manufacturerserialnum_pk = m.manufacturerserialnum_pk
                              WHERE RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X') =
                                    RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X'));

--
-- TE_MATCHED_WATER_METERS3_MV
--
CREATE MATERIALIZED VIEW TE_MATCHED_WATER_METERS3_MV REFRESH COMPLETE AS
  SELECT /*+REWRITE(TE_MATCHED_WATER_METERS3_MV)*/ -- MATCHED ON CORESPID
    -- UNMATCHED SERIAL_NO AND METERREAD
    /*+ leading (btw,dp,msa,m) full(msa)*/
    btw.te_category,
    SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
    m.spid_pk,
    m.manufacturer_pk,
    btw.serial_no,
    btw.met_ref,
    m.manufacturerserialnum_pk,
    'N' matched_cross_border,
    CASE
      WHEN m.spid_pk LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
        ||'%'
      THEN 'Y'
      ELSE 'N'
    END AS matched_corespid_yn,
    'N' matched_serialno_yn,
    'N' matched_endread_yn,
    'N' matched_usage_readings_yn,
--      NVL(btw.te,0)*100 mdvol,                                                             -- mo.mdvol
    CASE
       WHEN btw.pa_yn = 'Y' THEN
          100
       ELSE
          ABS(btw.te)*100
    END AS mdvol,
--    DECODE((1    -ABS(NVL(btw.ms,0))), 0, 1, 1,0, (1-ABS(NVL(btw.ms,0)))) returntosewer,
    ABS(NVL(btw.ms,0)) returntosewer,
    m.numberofdigits,
    m.metertreatment,
    m.measureunitfreedescriptor,
    m.measureunitatmeter,
    m.meterreadfrequency,
    m.nonmarketmeterflag,
    m.installedpropertynumber,
    dp.dpid_pk,
    btw.no_iwcs no_iwcs_working,
    btw.met_ref met_ref_working,
    btw.period period_working,
    btw.stage stage_working,
    btw.start_date start_date_working,
    btw.end_date end_date_working,
    btw.start_read start_read_working,
    btw.end_read end_read_working,
    btw.end_read - btw.start_read period_usage_working,
    btw.te te_working,
    btw.te_vol te_vol_working,
    btw.ms ms_working,
    btw.ms_vol ms_vol_working,
    CAST(NULL AS DATE) meterreaddate_readings,
    CAST(NULL AS NUMBER(12,0)) meterread_readings,
    CAST(NULL AS NUMBER(12,0)) startread_readings,
    CAST(NULL AS NUMBER(12,0)) periodusage_readings
  FROM
    (SELECT tw.no_iwcs,
      tw.met_ref,
      tw.period,
      tw.stage,
      tw.serial_no,
      tw.te_category,
      tw.start_date,
      tw.end_date,
      tw.start_read,
      tw.end_read,
      tw.te,
      tw.te_vol,
      tw.ms,
      tw.ms_vol,
      tw.pa_yn,
      ROW_NUMBER() OVER (PARTITION BY tw.no_iwcs ORDER BY tw.ms DESC, tw.met_ref ) iwcs_rows
    FROM te_working_v tw
    LEFT OUTER JOIN lu_te_meter_pairing ltmp
    ON ltmp.no_iwcs_working = tw.no_iwcs
       AND ltmp.met_ref_working = tw.met_ref
    WHERE UPPER(tw.unit) = 'M3'
    AND UPPER(tw.te_category) = 'WATER METER'
    AND NVL(te_pairing_override_yn,'N') != 'Y'
    AND NOT EXISTS
                            -- WATER METERS ALREADY FOUND
                            (SELECT 'Y' 
                               FROM te_working_v tw2
                               JOIN mo_discharge_point dp
                                 ON dp.no_iwcs = tw2.no_iwcs
                               JOIN mo_meter_spid_assoc msa
                                 ON SUBSTR(msa.spid,1,LENGTH(msa.spid)-2) = SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)||'W'
                               JOIN mo_meter m
                                 ON msa.manufacturer_pk = m.manufacturer_pk
                                    AND msa.manufacturerserialnum_pk = m.manufacturerserialnum_pk
                              WHERE RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X') =
                                    RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(tw.serial_no)),'0'),'.'),'A'),'X'))
    ) btw
  JOIN mo_discharge_point dp
  ON dp.no_iwcs = btw.no_iwcs
  JOIN 
    (SELECT msa.spid,
            manufacturer_pk,
            manufacturerserialnum_pk,
            ROW_NUMBER() OVER (PARTITION BY SUBSTR(msa.spid,1,LENGTH(msa.spid)-2)
                               ORDER BY SUBSTR(msa.spid,1,LENGTH(msa.spid)-2), manufacturerserialnum_pk) iwcs_rows
       FROM mo_meter_spid_assoc msa
      WHERE msa.spid LIKE '%W%' ) msa_ilv
  ON SUBSTR(msa_ilv.spid,1,LENGTH(msa_ilv.spid)-2) = SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)||'W'
     AND btw.iwcs_rows = msa_ilv.iwcs_rows
  JOIN mo_meter m
  ON msa_ilv.manufacturer_pk                                                                  = m.manufacturer_pk
  AND msa_ilv.manufacturerserialnum_pk                                                        = m.manufacturerserialnum_pk
  WHERE NVL(RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(btw.serial_no)),'0'),'.'),'A'),'X'),'-1') != NVL(RTRIM(RTRIM(RTRIM(LTRIM(TRIM(UPPER(m.manufacturerserialnum_pk)),'0'),'.'),'A'),'X'),'-2')
  AND NOT EXISTS
    (SELECT 'Y'
    FROM mo_meter_reading mr
    WHERE mr.manufacturer_pk        = m.manufacturer_pk
    AND mr.manufacturerserialnum_pk = m.manufacturerserialnum_pk
    AND mr.meterread                = btw.end_read
        AND mr.meterreaddate <
        (SELECT MAX(ADD_MONTHS(TO_DATE(ltbc_finish,'dd-mon-yy'),1))
        FROM lu_te_billing_cycle
        WHERE ltbc_period     = 16)
    );
                     


--
-- TE_MATCHED_WATER_METERS4_MV
--
CREATE MATERIALIZED VIEW TE_MATCHED_WATER_METERS4_MV REFRESH COMPLETE AS
  SELECT /*+REWRITE(TE_MATCHED_WATER_METERS4_MV)*/ -- CROSS BORDER EXCLUDING SOUTHSTAFFS
    -- UNMATCHED SERIAL_NO AND METERREAD
    /*+ leading (btw,dp,msa,m) */
    DISTINCT btw.te_category,
    SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
    CAST(NULL AS VARCHAR2(13)) spid_pk,
    mmd_ilv.manufacturer_pk,
    btw.serial_no,
    btw.met_ref,
    mmd_ilv.manufacturerserialnum_pk,
    'Y' matched_cross_border,
    CASE
      WHEN mmd_ilv.spid LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
        ||'%'
      THEN 'Y'
      ELSE 'N'
    END AS matched_corespid_yn,
    'N' matched_serialno_yn,
    'N' matched_endread_yn,
    'N' matched_usage_readings_yn,
    CASE
       WHEN btw.pa_yn = 'Y' THEN
          100
       ELSE
          ABS(btw.te)*100
    END AS mdvol,
--    DECODE((1    -ABS(NVL(btw.ms,0))), 0, 1, 1,0, (1-ABS(NVL(btw.ms,0)))) returntosewer,
    ABS(NVL(btw.ms,0)) returntosewer,
    CAST(NULL AS NUMBER(1,0)) numberofdigits,
    CAST(NULL AS VARCHAR2(32)) metertreatment,
    CAST(NULL AS VARCHAR2(255)) measureunitfreedescriptor,
    CAST(NULL AS VARCHAR2(12)) measureunitatmeter,
    CAST(NULL AS VARCHAR2(1)) meterreadfrequency,
    CAST(NULL AS NUMBER(1,0)) nonmarketmeterflag,
    CAST(NULL AS NUMBER(9,0)) installedpropertynumber,
    dp.dpid_pk,
    btw.no_iwcs no_iwcs_working,
    btw.met_ref met_ref_working,
    btw.period period_working,
    btw.stage stage_working,
    btw.start_date start_date_working,
    btw.end_date end_date_working,
    btw.start_read start_read_working,
    btw.end_read end_read_working,
    btw.end_read - btw.start_read period_usage_working,
    btw.te te_working,
    btw.te_vol te_vol_working,
    btw.ms ms_working,
    btw.ms_vol ms_vol_working,
    CAST(NULL AS DATE) meterreaddate_readings,
    CAST(NULL AS NUMBER(12,0)) meterread_readings,
    CAST(NULL AS NUMBER(12,0)) startread_readings,
    CAST(NULL AS NUMBER(12,0)) periodusage_readings
  FROM
    (SELECT tw.no_iwcs,
      tw.met_ref,
      tw.period,
      tw.stage,
      tw.serial_no,
      tw.te_category,
      tw.start_date,
      tw.end_date,
      tw.start_read,
      tw.end_read,
      tw.te,
      tw.te_vol,
      tw.ms,
      tw.ms_vol,
      tw.pa_yn,
      ROW_NUMBER() OVER (PARTITION BY tw.no_iwcs ORDER BY tw.serial_no, tw.met_ref ) iwcs_rows
    FROM te_working_v tw
    LEFT OUTER JOIN lu_te_meter_pairing ltmp
    ON ltmp.no_iwcs_working = tw.no_iwcs
       AND ltmp.met_ref_working = tw.met_ref
    WHERE UPPER(tw.unit) = 'M3'
    AND UPPER(tw.te_category) = 'WATER METER'
    AND NVL(te_pairing_override_yn,'N') != 'Y'
    ) btw
  JOIN mo_discharge_point dp
  ON dp.no_iwcs = btw.no_iwcs
  JOIN 
    (SELECT mmd.dpid_pk,
            mmd.spid,
            mmd.manufacturerserialnum_pk,
            mmd.manufacturer_pk,
            ROW_NUMBER() OVER (PARTITION BY dpid_pk
                               ORDER BY mmd.manufacturerserialnum_pk) xref_rows
       FROM mo_meter_dpidxref mmd
      WHERE mmd.owc IS NOT NULL
        AND mmd.owc != 'SOUTHSTAFF-W'
    ) mmd_ilv
  ON mmd_ilv.dpid_pk = dp.dpid_pk
     AND mmd_ilv.xref_rows = btw.iwcs_rows
 WHERE SUBSTR(mmd_ilv.spid,1,LENGTH(mmd_ilv.spid)-3) = SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3);
 
 
--
-- TE_MATCHED_WATER_METERS5_MV
--
CREATE MATERIALIZED VIEW TE_MATCHED_WATER_METERS5_MV REFRESH COMPLETE AS
  SELECT /*+REWRITE(TE_MATCHED_WATER_METERS5_MV)*/ -- CROSS BORDER SOUTHSTAFFS with a single Water Meters
    -- UNMATCHED SERIAL_NO AND METERREAD
    /*+ leading (btw,dp,msa,m) */
    DISTINCT btw.te_category,
    SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
    CAST(NULL AS VARCHAR2(13)) spid_pk,
    mmd_ilv.manufacturer_pk,
    btw.serial_no,
    btw.met_ref,
    mmd_ilv.manufacturerserialnum_pk,
    'Y' matched_cross_border,
    CASE
      WHEN mmd_ilv.spid LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
        ||'%'
      THEN 'Y'
      ELSE 'N'
    END AS matched_corespid_yn,
    'N' matched_serialno_yn,
    'N' matched_endread_yn,
    'N' matched_usage_readings_yn,
    CASE
       WHEN btw.pa_yn = 'Y' THEN
          100
       ELSE
          ABS(btw.te)*100
    END AS mdvol,
--    DECODE((1    -ABS(NVL(btw.ms,0))), 0, 1, 1,0, (1-ABS(NVL(btw.ms,0)))) returntosewer,
    ABS(NVL(btw.ms,0)) returntosewer,
    CAST(NULL AS NUMBER(1,0)) numberofdigits,
    CAST(NULL AS VARCHAR2(32)) metertreatment,
    CAST(NULL AS VARCHAR2(255)) measureunitfreedescriptor,
    CAST(NULL AS VARCHAR2(12)) measureunitatmeter,
    CAST(NULL AS VARCHAR2(1)) meterreadfrequency,
    CAST(NULL AS NUMBER(1,0)) nonmarketmeterflag,
    CAST(NULL AS NUMBER(9,0)) installedpropertynumber,
    dp.dpid_pk,
    btw.no_iwcs no_iwcs_working,
    btw.met_ref met_ref_working,
    btw.period period_working,
    btw.stage stage_working,
    btw.start_date start_date_working,
    btw.end_date end_date_working,
    btw.start_read start_read_working,
    btw.end_read end_read_working,
    btw.end_read - btw.start_read period_usage_working,
    btw.te te_working,
    btw.te_vol te_vol_working,
    btw.ms ms_working,
    btw.ms_vol ms_vol_working,
    CAST(NULL AS DATE) meterreaddate_readings,
    CAST(NULL AS NUMBER(12,0)) meterread_readings,
    CAST(NULL AS NUMBER(12,0)) startread_readings,
    CAST(NULL AS NUMBER(12,0)) periodusage_readings
  FROM
    (SELECT tw.no_iwcs,
      tw.met_ref,
      tw.period,
      tw.stage,
      tw.serial_no,
      tw.te_category,
      tw.start_date,
      tw.end_date,
      tw.start_read,
      tw.end_read,
      tw.te,
      tw.te_vol,
      tw.ms,
      tw.ms_vol,
      tw.pa_yn,
      ROW_NUMBER() OVER (PARTITION BY tw.no_iwcs ORDER BY tw.serial_no, tw.met_ref ) iwcs_rows,
      COUNT(*) OVER (PARTITION BY tw.no_iwcs) iwcs_rows_count
    FROM te_working_v tw
    LEFT OUTER JOIN lu_te_meter_pairing ltmp
    ON ltmp.no_iwcs_working = tw.no_iwcs
       AND ltmp.met_ref_working = tw.met_ref
    WHERE UPPER(tw.unit) = 'M3'
    AND UPPER(tw.te_category) = 'WATER METER'
    AND NVL(te_pairing_override_yn,'N') != 'Y'
    ) btw
  JOIN mo_discharge_point dp
  ON dp.no_iwcs = btw.no_iwcs
  JOIN 
    (SELECT mmd.dpid_pk,
            mmd.spid,
            mmd.manufacturerserialnum_pk,
            mmd.manufacturer_pk,
            ROW_NUMBER() OVER (PARTITION BY dpid_pk
                               ORDER BY mmd.manufacturerserialnum_pk) xref_rows,
            COUNT(*) OVER (PARTITION BY dpid_pk) dpid_pk_rows_count
       FROM mo_meter_dpidxref mmd
      WHERE mmd.owc IS NOT NULL
--        AND mmd.owc = 'SOUTHSTAFF-W'
    ) mmd_ilv
  ON mmd_ilv.dpid_pk = dp.dpid_pk
     AND mmd_ilv.xref_rows = btw.iwcs_rows
     AND mmd_ilv.dpid_pk_rows_count = btw.iwcs_rows_count
 WHERE SUBSTR(mmd_ilv.spid,1,LENGTH(mmd_ilv.spid)-3) = SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
   AND btw.iwcs_rows_count = 1; 


--
-- TE_MATCHED_WATER_METERS6_MV
--
CREATE MATERIALIZED VIEW TE_MATCHED_WATER_METERS6_MV REFRESH COMPLETE AS
  SELECT /*+REWRITE(TE_MATCHED_WATER_METERS6_MV)*/ -- CROSS BORDER with an override
    -- UNMATCHED SERIAL_NO AND METERREAD
    /*+ leading (btw,dp,msa,m) */
    DISTINCT btw.te_category,
    SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
    CAST(NULL AS VARCHAR2(13)) spid_pk,
    mmd_ilv.manufacturer_pk,
    btw.serial_no,
    btw.met_ref,
    mmd_ilv.manufacturerserialnum_pk,
    'Y' matched_cross_border,
    CASE
      WHEN mmd_ilv.spid LIKE SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3)
        ||'%'
      THEN 'Y'
      ELSE 'N'
    END AS matched_corespid_yn,
    'N' matched_serialno_yn,
    'N' matched_endread_yn,
    'N' matched_usage_readings_yn,
    CASE
       WHEN btw.pa_yn = 'Y' THEN
          100
       ELSE
          ABS(btw.te)*100
    END AS mdvol,
    ABS(NVL(btw.ms,0)) returntosewer,
    CAST(NULL AS NUMBER(1,0)) numberofdigits,
    CAST(NULL AS VARCHAR2(32)) metertreatment,
    CAST(NULL AS VARCHAR2(255)) measureunitfreedescriptor,
    CAST(NULL AS VARCHAR2(12)) measureunitatmeter,
    CAST(NULL AS VARCHAR2(1)) meterreadfrequency,
    CAST(NULL AS NUMBER(1,0)) nonmarketmeterflag,
    CAST(NULL AS NUMBER(9,0)) installedpropertynumber,
    dp.dpid_pk,
    btw.no_iwcs no_iwcs_working,
    btw.met_ref met_ref_working,
    btw.period period_working,
    btw.stage stage_working,
    btw.start_date start_date_working,
    btw.end_date end_date_working,
    btw.start_read start_read_working,
    btw.end_read end_read_working,
    btw.end_read - btw.start_read period_usage_working,
    btw.te te_working,
    btw.te_vol te_vol_working,
    btw.ms ms_working,
    btw.ms_vol ms_vol_working,
    CAST(NULL AS DATE) meterreaddate_readings,
    CAST(NULL AS NUMBER(12,0)) meterread_readings,
    CAST(NULL AS NUMBER(12,0)) startread_readings,
    CAST(NULL AS NUMBER(12,0)) periodusage_readings
  FROM
    (SELECT tw.no_iwcs,
      tw.met_ref,
      tw.period,
      tw.stage,
      tw.serial_no,
      tw.te_category,
      tw.start_date,
      tw.end_date,
      tw.start_read,
      tw.end_read,
      tw.te,
      tw.te_vol,
      tw.ms,
      tw.ms_vol,
      tw.pa_yn
    FROM te_working_v tw
    LEFT OUTER JOIN lu_te_meter_pairing ltmp2
    ON ltmp2.no_iwcs_working = tw.no_iwcs
       AND ltmp2.met_ref_working = tw.met_ref
    WHERE UPPER(tw.unit) = 'M3'
    AND UPPER(tw.te_category) = 'WATER METER'
    AND NVL(ltmp2.te_pairing_override_yn,'N') = 'Y'
    ) btw
  JOIN mo_discharge_point dp
  ON dp.no_iwcs = btw.no_iwcs
  JOIN lu_te_meter_pairing ltmp
  ON ltmp.no_iwcs_working = dp.no_iwcs
     AND ltmp.met_ref_working = btw.met_ref
  JOIN
    (SELECT mmd.dpid_pk,
            mmd.spid,
            mmd.manufacturerserialnum_pk,
            mmd.manufacturer_pk
       FROM mo_meter_dpidxref mmd
--      WHERE mmd.owc IS NOT NULL
    ) mmd_ilv
  ON mmd_ilv.dpid_pk = dp.dpid_pk
     AND ltmp.manufacturer_pk = mmd_ilv.manufacturer_pk
     AND ltmp.manufacturerserialnum_pk = mmd_ilv.manufacturerserialnum_pk;

--
-- TE_MATCHED_WATER_METERS_V
--
CREATE OR REPLACE VIEW TE_MATCHED_WATER_METERS_V AS (
SELECT te_category,
       corespid,
       spid_pk,
       manufacturer_pk,
       serial_no,
       met_ref,
       manufacturerserialnum_pk,
       matched_cross_border,
       matched_corespid_yn,
       matched_serialno_yn,
       matched_endread_yn,
       matched_usage_readings_yn,
       mdvol,
       returntosewer,
       numberofdigits,
       metertreatment,
       measureunitfreedescriptor,
       measureunitatmeter,
       meterreadfrequency,
       nonmarketmeterflag,
       installedpropertynumber,
       dpid_pk,
       no_iwcs_working,
       met_ref_working,
       period_working,
       stage_working,
       start_date_working,
       end_date_working,
       start_read_working,
       end_read_working,
       period_usage_working,
       te_working,
       te_vol_working,
       ms_working,
       ms_vol_working,
       meterreaddate_readings,
       meterread_readings,
       startread_readings,
       periodusage_readings
  FROM (SELECT ilv.*,
               ROW_NUMBER() OVER (PARTITION BY no_iwcs_working, met_ref
                                  ORDER BY no_iwcs_working, 
                                   met_ref, 
                                   matched_serialno_yn DESC, 
                                   matched_usage_readings_yn DESC, 
                                   matched_endread_yn, 
                                   matched_cross_border DESC) matched_metref 
              FROM (SELECT * FROM TE_MATCHED_WATER_METERS1_MV
                    UNION
                    SELECT * FROM TE_MATCHED_WATER_METERS2_MV
                    UNION
                    SELECT * FROM TE_MATCHED_WATER_METERS3_MV
                    UNION
                    SELECT * FROM TE_MATCHED_WATER_METERS4_MV
                    UNION
                    SELECT * FROM TE_MATCHED_WATER_METERS5_MV
                    UNION
                    SELECT * FROM TE_MATCHED_WATER_METERS6_MV
                   ) ilv
      )
 WHERE matched_metref = 1);

--
-- TE_ALL_METERS_V
--
CREATE OR REPLACE VIEW te_all_meters_v
AS
  SELECT DISTINCT te_category,
    no_iwcs,
    related_dpid,
    manufacturer_pk,
    manufacturerserialnum_pk,
    serial_no_working,
    met_ref_working,
    spid_pk,
    corespid,
    matched_cross_border,
    matched_corespid_yn,
    matched_serialno_yn,
    matched_endread_yn,
    matched_usage_readings_yn,
    numberofdigits,
    metertreatment,
    measureunitfreedescriptor,
    measureunitatmeter,
    meterreadfrequency,
    mdvol,
    returntosewer,
    nonmarketmeterflag,
    installedpropertynumber,
    periodusage
  FROM
    (
    -- TE Meters 3145
    SELECT btw.te_category,
      dp.no_iwcs,
      dp.dpid_pk related_dpid,
      mo.manufacturer_pk,
      mo.manufacturerserialnum_pk,
      btw.serial_no serial_no_working,
      btw.met_ref met_ref_working,
      mo.spid_pk,
      SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)-3) corespid,
      'N' matched_cross_border,
      'Y' matched_corespid_yn,
      CASE
        WHEN TRIM(NVL(btw.serial_no,'EMPTY')) = TRIM(NVL(mo.manufacturerserialnum_pk,'EMPTY'))
        THEN 'Y'
        ELSE 'N'
      END AS matched_serialno_yn,
      CASE
        WHEN btw.end_read = mr.meterread
        THEN 'Y'
        ELSE 'N'
      END AS matched_endread_yn,
      CASE
        WHEN btw.start_read = mr.startread
        AND btw.end_read    = mr.meterread
        THEN 'Y'
        ELSE 'N'
      END AS matched_usage_readings_yn,
      mo.numberofdigits,
      mo.metertreatment,
      mo.measureunitfreedescriptor,
      mo.measureunitatmeter,
      mo.meterreadfrequency,
      btw.te*100 mdvol,
      CASE
         WHEN btw.te_category = 'Private TE Meter' THEN
            0
         ELSE
--            DECODE((1    -ABS(NVL(btw.ms,0))), 0, 1, 1,0, (1-ABS(NVL(btw.ms,0))))
            ABS(NVL(btw.ms,0))
      END AS returntosewer, 
      mo.nonmarketmeterflag,
      mo.installedpropertynumber,
      (btw.end_read - btw.start_read) periodusage
    FROM mo_discharge_point dp
    JOIN mo_meter_spid_assoc msa
    ON SUBSTR(dp.spid_pk,1,LENGTH(dp.spid_pk)   -3)
      ||'S' = SUBSTR(msa.spid,1,LENGTH(msa.spid)-2)
    JOIN mo_meter mo
    ON msa.manufacturer_pk           = mo.manufacturer_pk
    AND msa.manufacturerserialnum_pk = mo.manufacturerserialnum_pk
    JOIN bt_te_working btw
    ON mo.manufacturerserialnum_pk = btw.no_iwcs
      ||btw.met_ref
    LEFT OUTER JOIN
      (SELECT manufacturer_pk,
        manufacturerserialnum_pk,
        meterread,
        NVL(startread,0) startread,
        (meterread - NVL(startread,0)) periodusage
      FROM
        (SELECT manufacturer_pk,
          manufacturerserialnum_pk,
          meterreaddate,
          meterread,
          startread,
          ROW_NUMBER() OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate DESC) meter_reads
        FROM
          (SELECT manufacturer_pk,
            manufacturerserialnum_pk,
            meterreaddate,
            meterread,
            LAG(meterread,1) OVER (PARTITION BY manufacturer_pk, manufacturerserialnum_pk ORDER BY meterreaddate) startread
          FROM mo_meter_reading mr
          WHERE meterreadtype = 'P'
          AND meterreaddate  <=
            (SELECT ADD_MONTHS(TO_DATE(ltbc_finish,'dd-mon-yy'),1)
            FROM lu_te_billing_cycle
            WHERE ltbc_period     = 16
            AND ltbc_cycle_number = 1
            )
          )
        WHERE meterread != startread
        )
      WHERE meter_reads = 1
      ) mr
    ON mr.manufacturer_pk           = msa.manufacturer_pk
    AND mr.manufacturerserialnum_pk = msa.manufacturerserialnum_pk
    WHERE mo.dpid_pk                = dp.dpid_pk
    AND btw.period                  = 16
    AND btw.te_category            IN ('Private TE Meter','Private Water Meter')
    UNION ALL
    -- Associated Water Meters 1226
    SELECT te_category,
      no_iwcs_working,
      dpid_pk related_dpid,
      manufacturer_pk,
      manufacturerserialnum_pk,
      serial_no serial_no_working,
      met_ref met_ref_working,
      spid_pk,
      corespid,
      matched_cross_border,
      matched_corespid_yn,
      matched_serialno_yn,
      matched_endread_yn,
      matched_usage_readings_yn,
      numberofdigits,
      metertreatment,
      measureunitfreedescriptor,
      measureunitatmeter,
      meterreadfrequency,
      mdvol,
      returntosewer,
      nonmarketmeterflag,
      installedpropertynumber,
      period_usage_working
    FROM te_matched_water_meters_v
    WHERE UPPER(te_category) = 'WATER METER'
    );

--
-- TE_SUB_METERS_TO
--
CREATE OR REPLACE VIEW TE_SUB_METERS_TO_V AS
  SELECT tw."HAS_CROSS_BORDER_WATER_YN",
    tw."HAS_IWCS_DP",
    tw."NO_IWCS",
    tw."PERIOD",
    tw."STAGE",
    tw."MET_REF",
    tw."NO_ACCOUNT",
    tw."ACCOUNT_REF",
    tw."TE_REVISED_NAME",
    tw."TE_CATEGORY",
    tw."SERIAL_NO",
    tw."REFDESC",
    tw."TARGET_REF",
    tw."UNIT",
    tw."UNITS",
    tw."START_DATE",
    tw."START_READ",
    tw."CODE",
    tw."END_DATE",
    tw."END_READ",
    tw."CODEA",
    tw."TE",
    tw."TE_VOL",
    tw."MS",
    tw."MS_VOL",
    tw."REASON",
    tw."OUW_YEAR",
    tw."TE_YEAR",
    tw."FA_YN",
    tw."FA_VOL",
    tw."DA_YN",
    tw."DA_VOL",
    tw."PA_YN",
    tw."PA_PERC",
    tw."MDVOL_FOR_WS_METER_YN",
    tw."MDVOL_FOR_WS_METER_PERC",
    tw."MDVOL_FOR_TE_METER_YN",
    tw."MDVOL_FOR_TE_METER_PERC",
    tw."CALC_DISCHARGE_YN",
    tw."CALC_DISCHARGE_VOL",
    tw."WS_VOL",
    tw."SUB_METER",
    tw."TE_VOL_FILTERED",
    tw."TE_VOL_CALC",
    tw."OUW_VOL_CALC",
    tam.manufacturerserialnum_pk submeterto
  FROM te_working_v tw
  JOIN te_all_meters_v tam
  ON tw.no_iwcs                  = tam.no_iwcs
  WHERE te                       = -1 --< 0
  AND UPPER(UNIT)                = 'M3'
  AND UPPER(tw.te_category) NOT IN ('FIXED','WATER METER','ADJUSTMENT','CALCULATED')
  AND NVL(tam.spid_pk,'W') LIKE '%W%'
  AND TO_NUMBER(tw.end_read) > TO_NUMBER(tw.start_read);


--
-- TE_METERS_TAB_V
--
CREATE OR REPLACE VIEW TE_METERS_TAB_V AS
  SELECT spid,                -- INNER 5                    -- Set inner1
    spid_core,                -- Set inner1
    related_dpid,             -- Set inner1
    manufacturer_pk,          -- Set inner1
    manufacturerserialnum_pk, -- Set inner1
    serial_no_working,        -- Set inner1
    matched_cross_border,     -- Set inner1
    matched_corespid_yn,      -- Set inner1
    matched_serialno_yn,      -- Set inner1
    matched_endread_yn,       -- Set inner1
    matched_usage_readings_yn,-- Set inner1
    metertreatment,           -- Set inner1
    te_category,              -- Set inner1
    te_revised_name,          -- Set inner1
    no_iwcs,                  -- Set inner1
    met_ref,                  -- Set inner1
    pa_perc,                  -- Set inner1
    serial_no,                -- Set inner1
    percentagedischarge,      -- Calculated inner1
    RTS,                      -- Calculated inner1
    mdvol,                    -- Calculated inner1
    mdassoc,                  -- Calculated inner1
    ptem,                     -- Calculated inner1
    Mdvol_1minusPtem,         -- Calculated inner3
    sub_meter_to,             -- Calculated inner1
    sma,                      -- Calculated inner1
    mdassoc*sma*(1-svneta) MdassocxSmax1less_Svneta,
    dasplit,           -- Calculated inner1
    isSub_svam,        -- Calculated inner1
    mdassoc_isSubsvam, -- Calculated inner3
    svneta,            -- Calculated inner4
    vol_per_period,    -- Set inner1
    sub_meter_to_vol,  -- Set inner1
    ddv,               -- Calculated inner3
    mdvol_ddv,         -- Calculated inner4
    mdassoc_dasplit,   -- Calculated inner3
    CASE
      WHEN svneta = 1 THEN 
        CASE
          WHEN pa_yn = 'Y' THEN -- Set to 100% to prevent PA being used twice
            mdvol_ddv
          ELSE
            (-1 * mdvol_ddv)
        END
      ELSE
        CASE
          WHEN mdassoc_dasplit > 0 THEN -- ????? Requires linked dpids
            ROUND((sma    * da_mdassoc * mdassoc),4)
          ELSE 
            ROUND((ddv * rts),4)
        END
    END AS sddv,
    da,        -- Set inner1
    da_mdassoc -- Calculated inner3
  FROM
    (SELECT spid,               -- INNER 4              -- Set inner1
      spid_core,                -- Set inner1
      related_dpid,             -- Set inner1
      manufacturer_pk,          -- Set inner1
      manufacturerserialnum_pk, -- Set inner1
      serial_no_working,        -- Set inner1
      matched_cross_border,     -- Set inner1
      matched_corespid_yn,      -- Set inner1
      matched_serialno_yn,      -- Set inner1
      matched_endread_yn,       -- Set inner1
      matched_usage_readings_yn,-- Set inner1
      metertreatment,           -- Set inner1
      te_category,              -- Set inner1
      te_revised_name,          -- Set inner1
      no_iwcs,                  -- Set inner1
      met_ref,                  -- Set inner1
      serial_no,                -- Set inner1
      percentagedischarge,      -- Calculated inner1
      RTS,                      -- Calculated inner1
      mdvol,                    -- Calculated inner1
      mdassoc,                  -- Calculated inner2
      ptem,                     -- Calculated inner1
      Mdvol_1minusPtem,         -- Calculated inner3
      sub_meter_to,             -- Calculated inner1
      sma,                      -- Calculated inner1
      dasplit,                  -- Calculated inner1
      isSub_svam,               -- Calculated inner1
      mdassoc_isSubsvam,        -- Calculated inner3
      CASE
         WHEN NVL(ouw_vol_calc,0) != 0 THEN
            mdassoc_isSubsvam
         ELSE
            0
      END AS svneta,
      vol_per_period,   -- Set inner1
      sub_meter_to_vol, -- Set inner1
      ddv,              -- Calculated inner3
      DECODE(NVL(mdvol,0),-1,0,NVL(mdvol,0))*ddv mdvol_ddv,
      mdassoc_dasplit, -- Calculated inner3
      da,              -- Set inner1
      da_mdassoc,      -- Calculated inner3
      pa_yn,
      pa_perc,
      ouw_vol_calc
    FROM
      (SELECT spid,               -- INNER 3          -- Set inner1
        spid_core,                -- Set inner1
        related_dpid,             -- Set inner1
        manufacturer_pk,          -- Set inner1
        manufacturerserialnum_pk, -- Set inner1
        serial_no_working,        -- Set inner1
        matched_cross_border,     -- Set inner1
        matched_corespid_yn,      -- Set inner1
        matched_serialno_yn,      -- Set inner1
        matched_endread_yn,       -- Set inner1
        matched_usage_readings_yn,-- Set inner1
        metertreatment,           -- Set inner1
        te_category,              -- Set inner1
        te_revised_name,          -- Set inner1
        no_iwcs,                  -- Set inner1
        met_ref,                  -- Set inner1
        serial_no,                -- Set inner1
        percentagedischarge,      -- Calculated inner1
        RTS,                      -- Calculated inner1
        mdvol,                    -- Calculated inner1
        mdassoc,                  -- Calculated inner2
        ptem,                     -- Calculated inner1
        CASE
           WHEN NVL(mdvol,0) <= 0 THEN
              0
           WHEN NVL(mdvol,0) = 1 THEN
              mdvol*(1-ptem)
           WHEN NVL(mdvol,0) > 0 AND NVL(mdvol,0) < 1 THEN
              TRUNC(mdvol+1)*(1-ptem)
           ELSE
              1-ptem
        END AS Mdvol_1minusPtem,
-- DECODE(NVL(mdvol,0),-1,0,NVL(mdvol,0))*(1-ptem) Mdvol_1minusPtem,
        sub_meter_to, -- Calculated inner1
        sma,          -- Calculated inner1
        dasplit,      -- Calculated inner1
        isSub_svam,   -- Calculated inner1
        mdassoc*issub_svam mdassoc_isSubsvam,
        vol_per_period,   -- Set inner1
        sub_meter_to_vol, -- Set inner1
        ROUND((NVL(vol_per_period,0) - NVL(sub_meter_to_vol,0)),4) ddv,
        mdassoc                *dasplit mdassoc_dasplit,
        da,                   -- Set inner1
        da*mdassoc da_mdassoc,-- Calculated inner3
        pa_yn,
        pa_perc,
        ouw_vol_calc
      FROM
        (SELECT spid,               -- INNER 2        -- Set inner1
          spid_core,                -- Set inner1
          related_dpid,             -- Set inner1
          manufacturer_pk,          -- Set inner1
          manufacturerserialnum_pk, -- Set inner1
          serial_no_working,        -- Set inner1
          matched_cross_border,     -- Set inner1
          matched_corespid_yn,      -- Set inner1
          matched_serialno_yn,      -- Set inner1
          matched_endread_yn,       -- Set inner1
          matched_usage_readings_yn,-- Set inner1
          metertreatment,           -- Set inner1
          te_category,              -- Set inner1
          te_revised_name,          -- Set inner1
          no_iwcs,                  -- Set inner1
          met_ref,                  -- Set inner1
          serial_no,                -- Set inner1
          percentagedischarge,      -- Calculated inner1
          RTS,                      -- Calculated inner1
          mdvol,                    -- Calculated inner1
          CASE
             WHEN DECODE(NVL(mdvol,0),-1,0,NVL(mdvol,0)) != 0 THEN 
                1
             ELSE 
                0
          END AS mdassoc,
          ptem,             -- Calculated inner1
          sub_meter_to,     -- Calculated inner1
          sma,              -- Calculated inner1
          dasplit,          -- Calculated inner1
          isSub_svam,       -- Calculated inner1
          vol_per_period,   -- Set inner1
          sub_meter_to_vol, -- Set inner1 ?????????? LINK SUB METERS TO METER
          da,               -- Set inner1
          pa_yn,
          pa_perc,
          ouw_vol_calc
        FROM
          (SELECT am.spid_pk spid, -- INNER 1
            am.corespid spid_core,
            am.related_dpid,
            am.manufacturer_pk,
            am.manufacturerserialnum_pk,
            am.serial_no_working,
            am.matched_cross_border,
            am.matched_corespid_yn,
            am.matched_serialno_yn,
            am.matched_endread_yn,
            am.matched_usage_readings_yn,
            am.metertreatment,
            working_rows.te_category,
            working_rows.te_revised_name,
            am.no_iwcs,
            working_rows.met_ref,
            working_rows.serial_no,
            CASE
              WHEN working_rows.pa_yn             = 'Y'
              AND UPPER(working_rows.te_category) = 'WATER METER'
              THEN 100 -- Set to 100% to prevent PA being used twice
              ELSE am.mdvol
            END AS percentagedischarge,
            am.returntosewer RTS, -- Calculated inner1
            CASE
              WHEN working_rows.pa_yn             = 'Y'
              AND UPPER(working_rows.te_category) = 'WATER METER'
              THEN 1 -- Set to 1 to prevent PA being used twice
              ELSE (am.mdvol/100)
            END AS mdvol,
            CASE
              WHEN UPPER(working_rows.te_category) = 'PRIVATE TE METER'
              THEN 1
              ELSE 0
            END AS ptem,
            CASE
                --               WHEN am.manufacturerserialnum_pk != submeters.sub_meter_to THEN  -- ********* DATA REQUIRES TIDYING *********
              WHEN am.metertreatment != 'POTABLE'
              THEN -- USE potable as problem with serial numbers (see above)
                submeters.sub_meter_to
            END AS sub_meter_to,
            CASE
              WHEN returntosewer>0
              THEN
                CASE
                  WHEN UPPER(NVL(working_rows.te_category,'EMPTY')) != 'PRIVATE TE METER'
                  THEN 1
                  ELSE 0
                END
              ELSE 0
            END AS sma,
            CASE
                --              WHEN working_rows2.da_vol_period > 0
              WHEN working_rows2.da_rows_cnt > 0
              THEN 1 -- ****** COUNT OF DPIDs ?
              ELSE 0
            END AS dasplit,
            CASE
              WHEN seweragevolumeadjmenthod = 'SUBTRACT'
              THEN 1
              ELSE 0
            END AS isSub_svam,
            CASE
              WHEN UPPER(am.metertreatment) = 'POTABLE'
              THEN
                 --                 NVL(am.periodusage,0)  -- There is a problem with these meter readings
                 CASE
                    WHEN working_rows.pa_perc NOT IN (0,1) THEN
                       ROUND(
                          NVL(working_rows.te_vol/(working_rows.pa_perc),(NVL(working_rows.end_read,0)-NVL(working_rows.start_read,0)))
                          ,4)
                    WHEN working_rows.units != 1 THEN
                       ROUND(NVL(working_rows.te_vol,(NVL(working_rows.end_read,0)-NVL(working_rows.start_read,0))* working_rows.units),4)
                    ELSE
                       ROUND(NVL((working_rows.te_vol/(mdvol/100)),NVL(working_rows.end_read,0)-NVL(working_rows.start_read,0)),4)
                 END
              ELSE
                 CASE
                    WHEN working_rows.units != 1 THEN
                       ROUND(NVL(am.periodusage,0)* working_rows.units,4)
                    ELSE
                       ROUND(NVL(am.periodusage,0),4)
                 END
            END AS vol_per_period,  -- RECHECK THIS 20161020
            CASE
              WHEN working_rows2.da_rows_cnt > 0
              THEN 'Y'
              ELSE 'N'
            END AS da_yn,
            CASE
                --               WHEN am.manufacturerserialnum_pk = submeters.sub_meter_to THEN  -- ********* DATA REQUIRES TIDYING *********
              WHEN am.metertreatment = 'POTABLE'
              THEN -- USE potable as problem with serial numbers (see above)
                submeters.sub_meter_to_vol
            END AS sub_meter_to_vol,
            working_rows2.da_vol_period da,
            bts.te_vol calc_dis_vol,
            bts.ouw_vol,
            bts.ws_vol,
            bts.seweragevolumeadjmenthod,
            working_rows.pa_yn pa_yn,
            working_rows.pa_perc,
            working_rows.ouw_vol_calc
          FROM te_all_meters_v am
          LEFT OUTER JOIN te_working_v working_rows
          ON am.no_iwcs          = working_rows.no_iwcs
          AND am.met_ref_working = working_rows.met_ref
          LEFT OUTER JOIN bt_te_summary bts
          ON bts.no_iwcs = am.no_iwcs
          LEFT OUTER JOIN
            (SELECT no_iwcs,
              COUNT(te_category) da_rows_cnt,
              SUM(ABS(NVL(te_vol,0))) da_vol_period,
              SUM(NVL(ms_vol,0)) da_ouw
            FROM te_working_v
            WHERE UPPER(te_category) = 'DOMESTIC'
              AND NVL(te,0)          != 0
            GROUP BY no_iwcs
            ) working_rows2
          ON working_rows2.no_iwcs = am.no_iwcs
          LEFT OUTER JOIN
            (SELECT no_iwcs,
              MAX(submeterto) sub_meter_to,
              SUM(ABS(te_vol)) sub_meter_to_vol
            FROM te_sub_meters_to_v
            GROUP BY no_iwcs
            ) submeters
          ON submeters.no_iwcs = am.no_iwcs
          )
        )
      )
    );
--where no_iwcs = '5291006200';

-- calculated fix for TE_VOL_MOCALC
--
-- TE_TRANSFORMED_TAB_V
--
CREATE OR REPLACE VIEW TE_TRANSFORMED_TAB_V AS
  SELECT 
    ilv.spid,
    ilv.spid_core,
    ilv.no_iwcs,
    ilv.related_dpid,
    ilv.calculation,
    CASE
       WHEN ilv.no_iwcs IN (
            '1211000101','2424005001','2424034201','2424034202','2427012801','3018002200','3018003301','3018003701','3018003801','3018004101',
            '3018004201','3018004301','3028001700','3028002100','3028004600','3028010600','3028010900','3028011700','3028012200','3028013100',
            '3028014800','3028019600','3028019900','3028020500','3028020700','3028020900','3028021200','3028021300','3028021601','3028165000',
            '3049001700','3049002100','3049002400','3049002500','3049002901','3049003101','3049021800','3049021901','3049022001','3051024301',
            '3064000500','3064000700','3064001900','3064001901','3064002400','3064002402','3064005500','3064005600','3064008700','3064009700',
            '3064010800','3064010801','3064013500','3064023901','3064024001','3064024101','3064024801','3064025001','3064025301','3064025401',
            '3064026001','3064026801','3064026901','3064027201','3064027301','3064028001','3064028301','3064028601','3064028801','3064029001',
            '3064029101','3064029201','3064029501','3064029601','3064029701','3064029801','3064030001','3079027201','3800005801','3800005901',
            '3807001100','3807001700','3807002300','3807002600','3807002700','3807004501','3807004901','3807004902','3807005001','3807005101',
            '3807050200','3807050301','3807050401','3807050501','3807050601','3807050701','3807050801','5009002700','5009002701','5009004400',
            '5009004700','5009005700','5009005801','5012000300','5012000400','5012002900','5012004600','5012004900','5012005800','5012006200',
            '5012008100','5012008600','5012008900','5012011500','5012012400','5012013500','5012013700','5012014400','5012016600','5012017200',
            '5012017300','5012017400','5012017600','5012018200','5012018900','5012019200','5012019400','5012019500','5012020301','5012020401',
            '5012020901','5012021301','5012021302','5012021601','5012021602','5012021701','5012021801','5012022101','5012022201','5012022301',
            '5026000300','5026000400','5032000900','5032002800','5032004700','5032004900','5032005000','5032005700','5032006000','5032006700',
            '5032007200','5032007300','5032008001','5032008101','5032008201','5032008301','5032021601','5032021701','5032021901','5032022201',
            '5032022301','5032022401','5062000800','5062000801','5062000802','5062000900','5062001402','5062004800','5062005602','5062006600',
            '5062006601','5062007100','5062007101','5062007800','5062008700','5062009400','5062011700','5062013500','5062013502','5062013503',
            '5062013600','5062014600','5062014601','5062015700','5062016400','5062016800','5062017200','5062017600','5062017900','5062018201',
            '5062018500','5062018700','5062019200','5062019500','5062019600','5062020000','5062020100','5062020400','5062020900','5062023201',
            '5062023202','5062023401','5062023501','5062023502','5062023801','5062024101','5062024301','5062024801','5062024901','5062025401',
            '5062025501','5062025801','5062025901','5062026001','5062026101','5062026202','5062026401','5062026701','5062026801','5062027101',
            '5062027201','5062027801','5062027901','5062028101','5062028201','5062028301','5062028401','5062028501','5062028601','5062028701',
            '5062028901','5062029001','5062029401','5062029501','5062029701','5062029801','5062029901','5062030001','5062030002','5062030101',
            '5062030201','5062030301','5062030302','5062030401','5062030501','5062030801','5062030901','5062031001','5062031101','5062031201',
            '5062031301','5062031601','5062031701','5062031801','5062031901','5065000300','5065000301','5065000303','5065000304','5065000900',
            '5065000903','5065004500','5065004600','5065004701','5065005300','5065005500','5065006100','5065007400','5065007800','5065008400',
            '5065008401','5065008500','5065023900','5065024101','5075001600','5075002200','5075002500','5076000100','5076000400','5076003300',
            '5076003800','5076003801','5076004000','5076007300','5076007800','5076008800','5076009100','5076009301','5076009501','5076009601',
            '5080000501','5080000800','5080001400','5080002200','5080002300','5080002501','5080002701','5090003600','5090003700','5090009000',
            '5090011300','5090027801','5090027901','5090028601','5090032101','5090032301','5090032401','5090032601','5191059301','5192066901',
            '5192068001','5193057800','5194064600','5194066501','5291062600','5291067300','5291068301','5292014500','5800002900','5800005801',
            '5800005901','5912002400','5912050100','5912050300','5912050401','5912050501','5914000100','5914000400','5920003000','5920003002',
            '5920003200','5920003800','5920004500','5920005600','5920006500','5920006600','5920007400','5920007500','5920007800','5920008000',
            '5920009101','5920009301','5920009401','5920009501','5920009601','5920009701','5920009801','5920010101','5920050100','5920050200',
            '5920050401','5920050601','5920051201','5920051301','5920051601','5920051801','5921000400','5921002000','5921002201','5921002301',
            '5921002302','5921002401','5921002501','5921002601','5923000301','5923000901','5925000701','6133301101','6133301701','6133308001',
            '6133314301','6133314401','6133314501','8026260301','8026260401','8065006701','8065656801','8065656901','11301005201','12504000301',
            '12507000701','12507001101','12508000301','12509000401','12509001101','12509001401','12509001601','12509001901','12509002101','12509002301',
            '12509002401','12509002501','12514000101','12514000201','12518002501','12518003001','12605002201','12605002401','12605002601','12605002701',
            '12605002801','12605003001','12605003101','12605003201','12605003301','12606000101','12606000501','12606000601','12615001001','12615001002',
            '12615001301','12615002701','12615002703','12615003101','12615003201','12615003202','12615003301','12615003701','12615003901','12615004101',
            '12615004201','12615004401','12615004501','12615004601','12615004901','12615005001','12616000501','12616000601','12616000602','12616002201',
            '12616002601','12616002701','12616002801','12616002901','12616003001','12616003002','12620000101','12620000201','12621000101','12621000501',
            '12629000101','12632000401','12632000501','12633000101','12635000101','12636000801','12636000901','12636001001','12636001101','12636001201',
            '14304000101','14401001501','14401001701','14401002501','14401003301','14401003801','14401004301','14401004501','14401006301','14401006401',
            '14401007201','14401008301','14401008302','14401008401','14401009101','14401009201','14401009301','14401009401','14401011301','14401012001',
            '14401012801','14401013101','14401013201','14401015201','14401015301','14401015401','14401015602','14401015901','14401016001','14401016201',
            '14401016501','14401016601','14401016701','14401016801','14401017201','14401017301','14401017401','14404000501','14404000601','14405000201',
            '14421000601','14421002301','14421002801','14427000501','14427000601','14427001001','14427001101','14427001102','14427001301','14427001901',
            '14427002001','14427002600','14427002901','14427003101','14427003201','14427003301','14427003302','14427003401','14427003402','14432000501',
            '14442000201','14999000301','15264001001','12616000501','5062009400') THEN
          'Y'
         ELSE
          'N'
    END AS has_cross_border_water_yn,
    NVL(te_vol_working,0)                              te_vol_working, 
    NVL(ms_vol_working,0)                              ms_vol_working,
    NVL(tim.wsm_in_te_vol_calc_cnt,0)                  wsm_in_te_vol_calc_cnt,
    NVL(tab_meters_ilv.wsm_tab_te_vol_calc_cnt,0)      wsm_tab_te_vol_calc_cnt,
    NVL(tim.ptem_in_te_vol_calc_cnt,0)                 ptem_in_te_vol_calc_cnt,
    NVL(tab_meters_ilv.ptem_tab_te_vol_calc_cnt,0)     ptem_tab_te_vol_calc_cnt,
    NVL(tim.pwm_in_te_vol_calc_cnt,0)                  pwm_in_te_vol_calc_cnt,
    NVL(tab_meters_ilv.pwm_tab_te_vol_calc_cnt,0)      pwm_tab_te_vol_calc_cnt,
    NVL(tim.wsm_in_ouw_vol_calc_cnt,0)                 wsm_in_ouw_vol_calc_cnt,
    NVL(tab_meters_ilv.wsm_tab_ouw_vol_calc_cnt,0)     wsm_tab_ouw_vol_calc_cnt,
    NVL(tim.ptem_in_ouw_vol_calc_cnt,0)                ptem_in_ouw_vol_calc_cnt,
    NVL(tab_meters_ilv.ptem_tab_ouw_vol_calc_cnt,0)    ptem_tab_ouw_vol_calc_cnt,
    NVL(tim.pwm_in_ouw_vol_calc_cnt,0)                 pwm_in_ouw_vol_calc_cnt,
    NVL(tab_meters_ilv.pwm_tab_ouw_vol_calc_cnt,0)     pwm_tab_ouw_vol_calc_cnt,
    ilv.complexity,
    ilv.calc_disc_flag,
    ilv.rts_change_flag,
    ilv.da_per_period, -- Set inner1
    ilv.fa_flag,       -- Set inner1
    ilv.fa_per_period,   -- Set inner1
    ilv.pa,            -- Set inner1
    ilv.sub_ind,
    ilv.da_inc,           -- Set inner1
    ilv.no_da,            -- Set inner1
    ilv.dasplit,          -- Set inner1
    ilv.meter_vol_period, -- Set inner1
    ilv.water_meter_vol_tedb,
    ilv.mdvol_wsm,
    ilv.te_meter_vol_tedb,
    ilv.ptem_mdvol,
    ilv.private_water_meter_tedb,
    ilv.mdvol_pwm,
    ilv.rts_wsm,
    ilv.rts_pwm,
    ilv.svam,          -- Set inner1
    ilv.no_svam,       -- Set inner1
    ilv.calc_disc_vol, -- Set inner1
    ilv.te_vol_mocalc,
    ilv.sewerage_vol_mocalc,   -- Set inner1
    ilv.te_vol_tedbcalc,       -- Set inner1
    ilv.sewerage_vol_tedbcalc, -- Set inner1
    CASE
      WHEN NVL(te_vol_mocalc,0) = NVL(te_vol_tedbcalc,0)
      THEN 'TRUE'
      ELSE 'FALSE'
    END AS te_vol_match,
    CASE
      WHEN NVL(sewerage_vol_mocalc,0) = NVL(sewerage_vol_tedbcalc,0)
      THEN 'TRUE'
      ELSE 'FALSE'
    END AS sewerage_vol_match
--               ,ilv.meter_vol_period, pa, ilv.da_inc, ilv.da_per_period, ilv.fa_per_period, ilv.calc_disc_vol, ilv.da_ouw, calc_perc_period
--,sddv_period, da_ouw, fa_ouw_vol_period
  FROM
    (SELECT spid,   -- Set inner1
      spid_core,    -- Set inner1
      no_iwcs,      -- Set inner1
      related_dpid, -- Set inner1
      calculation,  -- Set inner1
      complexity,
      calc_disc_flag,
      rts_change_flag,
      da_per_period, -- Set inner1
      fa_flag,       -- Set inner1
      fa_per_period,   -- Set inner1
      pa,            -- Set inner1
      sub_ind,
      da_inc,           -- Set inner1
      no_da,            -- Set inner1
      dasplit,          -- Set inner1
      meter_vol_period, -- Set inner1
      water_meter_vol_tedb,
      mdvol_wsm,
      te_meter_vol_tedb,
      ptem_mdvol,
      private_water_meter_tedb,
      mdvol_pwm,
      rts_wsm,
      rts_pwm,
      svam,          -- Set inner1
      no_svam,       -- Set inner1
      calc_disc_vol, -- Set inner1
--      ROUND(meter_vol_period - (da_inc * da_per_period) - (fa_per_period *(1-pa)) + (calc_disc_vol * calc_perc_period),0) te_vol_mocalc,
--      ROUND(sddv_period      + da_ouw + (fa_ouw_vol_period),0) sewerage_vol_mocalc, --  ******************************
      ROUND((meter_vol_period)
            - (da_inc * da_per_period) 
            - fa_per_period
            + (calc_disc_vol * calc_perc_period),0) te_vol_mocalc,
      ROUND((sddv_period) 
            + da_ouw 
            + (fa_ouw_vol_period),0) sewerage_vol_mocalc, --  ******************************
      te_vol_tedbcalc,                                                              -- Set inner1
      sewerage_vol_tedbcalc,                                                        -- Set inner1
      da_ouw,
      calc_perc_period
,sddv_period, fa_ouw_vol_period
    FROM
      (SELECT meters_ilv.spid,    -- Set inner1
        meters_ilv.spid_core,     -- Set inner1
        tes.no_iwcs,              -- Set inner1
        meters_ilv.related_dpid,  -- Set inner1
        tes.col_calc calculation, -- Set inner1
        NULL complexity,
        NULL calc_disc_flag,
        NULL rts_change_flag,
        NVL(meters_ilv.da_period,0) da_per_period,    -- Set inner1
        working_ilv.fa_exists_yn fa_flag,             -- Set inner1
        NVL(working_ilv.fa_vol_period,0) fa_per_period, -- Set inner1
        NVL(meters_ilv.pa_perc_period,0) pa,          -- Set inner1
        NULL sub_ind,
        CASE -- DAINC is set where there is a meter discharge volume (Non TE Meter) for the discharge point
          WHEN NVL(working_ilv.da_included,'N') = 'Y'
          THEN 1
          ELSE 0
        END AS da_inc, -- Set inner1
        CASE
            --          WHEN NVL(meters_ilv.da_vol_period,0) > 0 THEN
          WHEN NVL(working_ilv.da_included,'N') = 'Y'
          THEN 0
          ELSE 1
        END AS no_da, -- Set inner1
        CASE
            --          WHEN NVL(meters_ilv.da_vol_period,0) > 0 THEN
          WHEN NVL(working_ilv.da_included,'N') = 'Y'
          THEN MdassocxSmax1less_Svneta -- ??????????? See te_meters_tab_v assumed if DA 1*MdassocxSmax1less_Svneta
          ELSE 0
        END AS dasplit,                                      -- Set inner1
        NVL(meters_ilv.meter_vol_period,0) meter_vol_period, -- Set inner1
        NULL water_meter_vol_tedb,
        NULL mdvol_wsm,
        NULL te_meter_vol_tedb,
        NULL ptem_mdvol,
        NULL private_water_meter_tedb,
        NULL mdvol_pwm,
        NULL rts_wsm,
        NULL rts_pwm,
        tes.seweragevolumeadjmenthod svam, -- Set inner1
        CASE
          WHEN tes.seweragevolumeadjmenthod = 'SUBTRACT'
          THEN 1
          ELSE 0
        END AS no_svam,                              -- Set inner1
        ABS(NVL(tes.calc_discharge_vol,0)) calc_disc_vol, -- Set inner1              -- Calculated discharge should be positive *******
        NVL(meters_ilv.sddv_period,0) sddv_period,
        working_ilv.fa_ouw_vol_period fa_ouw_vol_period,
        ROUND(NVL(working_ilv.te_vol_period,0),0) te_vol_tedbcalc,             -- Set inner1
        ROUND(NVL(working_ilv.sewerage_vol_period,0),0) sewerage_vol_tedbcalc, -- Set inner1
        da_ouw,
        DECODE(working_ilv.calc_perc_period,-1000,0,working_ilv.calc_perc_period)   calc_perc_period  -- Reset -1000 to 0  *******
      FROM bt_te_summary tes
      LEFT OUTER JOIN
        (SELECT spid_core,
          no_iwcs,
          related_dpid,
          MIN(spid) spid,
          MAX(NVL(da,0)) da_period,
          MAX(NVL(pa_perc,0)) pa_perc_period,
          --          MAX(NVL(Mdvol_1minusPtem,0)) da_included,  -- DA shouldn't be based on the water meter currently is?
          SUM(NVL(MdassocxSmax1less_Svneta,0)) MdassocxSmax1less_Svneta,
          SUM(
          CASE
             WHEN ABS(pa_perc) IN (0,1) THEN
                ROUND(NVL(mdvol_ddv,0),4)
             ELSE
                ROUND(NVL(mdvol_ddv,0) * (pa_perc),4)
          END) AS meter_vol_period,
          SUM(
          CASE
             WHEN ABS(NVL(rts,0)) IN (0,1) THEN
                NVL(sddv,0)
             WHEN rts > 0 THEN
--                NVL(sddv,0) * rts
NVL(sddv,0)
             ELSE
--                NVL(sddv,0) * rts *-1
NVL(sddv,0) *-1
             END) sddv_period  -- **** 
        FROM te_meters_tab_v
        GROUP BY spid_core,
          no_iwcs,
          related_dpid
        ) meters_ilv
      ON tes.no_iwcs = meters_ilv.no_iwcs
      LEFT OUTER JOIN
        (SELECT no_iwcs,
          MAX(
          CASE
            WHEN UPPER(te_category) = 'DOMESTIC'
            AND NVL(te,0)          != 0
            THEN 'Y'
            ELSE 'N'
          END ) da_included,
          MAX(fa_yn) fa_exists_yn,
          SUM (
          CASE
            WHEN fa_yn = 'Y' AND NVL(te_vol,0) != 0 THEN
              CASE
                WHEN pa_yn = 'Y' THEN
                    NVL(fa_vol,0) * (NVL(pa_perc,0))
                ELSE 
                   (NVL(fa_vol,0)* (ABS(NVL(te,0))))
              END
            ELSE 0
          END ) AS fa_vol_period,
          SUM (
          CASE
            WHEN fa_yn = 'Y'
            THEN
              CASE
                WHEN NVL(ms_vol,0) != 0
                THEN NVL(fa_vol,0)
                ELSE 0
              END
            ELSE 0
          END ) AS fa_ouw_vol_period,
          SUM(
          CASE
             WHEN UPPER(te_category) = 'ADJUSTMENT' THEN  -- Ignore TE adjustments
                0
             ELSE
                NVL(te_vol,0)
          END) AS te_vol_period,
          SUM(
          CASE
             WHEN UPPER(te_category) = 'ADJUSTMENT' THEN  -- Ignore TE adjustments
                0
             ELSE
                NVL(ms_vol,0)
          END) AS sewerage_vol_period,  
          SUM(
          CASE
            WHEN UPPER(te_category) = 'DOMESTIC'
                 AND NVL(ms_vol,0) != 0 THEN 
               NVL(ms_vol,0)
            ELSE
               0
          END) da_ouw,
          MAX(
          CASE
            WHEN UPPER(te_category) = 'CALCULATED'
            THEN 
               NVL(te,0)
            ELSE
               -1000                                -- Initially set to a number below -1, as te for calculated can be negative! *******
          END) AS calc_perc_period
        FROM te_working_v
          --where no_iwcs = 1237053601
        GROUP BY no_iwcs
        ) working_ilv
      ON tes.no_iwcs = working_ilv.no_iwcs
        --where tes.no_iwcs = 1237053601
      )
    ) ilv
  LEFT OUTER JOIN te_iwcs_meters_v tim
    ON ilv.no_iwcs = tim.no_iwcs
  LEFT OUTER JOIN
    (SELECT mt.no_iwcs, 
       SUM(w.te_vol) te_vol_working, 
       SUM(w.ms_vol) ms_vol_working,
       SUM(
         CASE
            WHEN mt.te_category = 'Water Meter' THEN
               CASE
                  WHEN NVL(mt.mdvol,0) != 0 THEN
                     1
                  ELSE
                     0
               END
            ELSE
               0
         END) AS wsm_tab_te_vol_calc_cnt,
       SUM(
         CASE
            WHEN mt.te_category = 'Water Meter' THEN
               CASE
                  WHEN NVL(mt.sddv,0) != 0 THEN
                     1
                  ELSE
                     0
               END
            ELSE
               0
         END) AS wsm_tab_ouw_vol_calc_cnt,
       SUM(
         CASE
            WHEN mt.te_category = 'Private TE Meter' THEN
               CASE
                  WHEN NVL(mt.mdvol,0) != 0 THEN
                     1
                  ELSE
                     0
               END
            ELSE
               0
         END) AS ptem_tab_te_vol_calc_cnt,
       SUM(
         CASE
            WHEN mt.te_category = 'Private TE Meter' THEN
               CASE
                  WHEN NVL(mt.sddv,0) != 0 THEN
                     1
                  ELSE
                     0
               END
            ELSE
               0
         END) AS ptem_tab_ouw_vol_calc_cnt,
      SUM(
        CASE
            WHEN mt.te_category = 'Private Water Meter' THEN
               CASE
                  WHEN NVL(mt.mdvol,0) != 0 THEN
                     1
                  ELSE
                     0
               END
            ELSE
               0
         END) AS pwm_tab_te_vol_calc_cnt,
       SUM(
         CASE
            WHEN mt.te_category = 'Private Water Meter' THEN
               CASE
                  WHEN NVL(mt.sddv,0) != 0 THEN
                     1
                  ELSE
                     0
               END
            ELSE
               0
         END) AS pwm_tab_ouw_vol_calc_cnt
  FROM te_meters_tab_v mt
  JOIN te_working_v w
    ON mt.no_iwcs = w.no_iwcs
       AND mt.met_ref = w.met_ref
GROUP BY mt.no_iwcs
    ) tab_meters_ilv
  ON tab_meters_ilv.no_iwcs = tim.no_iwcs;
--where ilv.no_iwcs = '10502003200';
--where ilv.no_iwcs = '5291006200';
--where ilv.no_iwcs = '5920050801';
--where ilv.no_iwcs = '3051001501'; --BALANCES --
--where ilv.no_iwcs = '10502003501';  --BALANCES 
--                         where ilv.no_iwcs = '9058050401';
--where ilv.no_iwcs = '11204024401' --'5291006200';
--      where ilv.no_iwcs = '5294031900' --'01345041801';  --'5294031900';;
    --    where no_iwcs in (1237034801);
    -- where ilv.no_iwcs in (1365035301);   -- da_inc  BALANCES
    --                                    where ilv.no_iwcs in (1345041801);   -- da_inc  IN ERROR
    -- where no_iwcs = 1237053601;   -- BALANCES
    --            where no_iwcs = 05292066801;  -- NO WATER METER IN TE  BALANCES
    --       where ilv.no_iwcs = 5291068701;  -- subtract
    --where no_iwcs = 5292014500;   -- ssw-43374;;
-- where ilv.no_iwcs = '6354415301';
-- where ilv.no_iwcs = '1345041801';
-- where ilv.no_iwcs = '9058050401';

--
-- TE_READ_TOLERANCE
--
CREATE OR REPLACE VIEW te_read_tolerance AS
SELECT btw1.no_iwcs, 
       btw1.met_ref, 
       btw1.period,
       btw1.start_read,
       btw1.start_date,
       btw1.end_read,
       btw1.end_date,
       CASE
          WHEN ilv.gradient != 0 THEN
             ROUND((period-ilv.line_constant)/ilv.gradient,4)
       END AS calculated_read, -- Y=MX+C, X=(Y-C)/G
       CASE
          WHEN ilv.gradient != 0 THEN
             ROUND(((period-ilv.line_constant)/ilv.gradient) * ((100-ilv.tolerance)/100),4)
       END AS tolerance_from,
       CASE
          WHEN ilv.gradient != 0 THEN
             ROUND(((period-ilv.line_constant)/ilv.gradient) * ((100+ilv.tolerance)/100),4)
       END AS tolerance_to,
       ilv.earliest_period,
       ilv.gradient,
       ilv.line_constant
  FROM bt_te_working btw1
  JOIN (SELECT no_iwcs, 
               met_ref, 
               earliest_period,
               1 tolerance,
               CASE
                  WHEN p16_end_read > earliest_end_read THEN
                     (16-(earliest_period)) / (p16_end_read-earliest_end_read)
                  ELSE
                     0
               END AS gradient,
               CASE
                  WHEN p16_end_read > earliest_end_read THEN
                     16
                      - ((16-(earliest_period)) / (p16_end_read-earliest_end_read) -- Gradient
                        * p16_end_read)
                  ELSE
                     0
               END AS line_constant
          FROM (SELECT no_iwcs,
                       met_ref,
                       row_type,
                       earliest_period,
                       (16 - earliest_period) period_between,
                       earliest_end_read,
                       p16_end_read,
                       ROW_NUMBER() OVER (PARTITION BY no_iwcs, met_ref
                                          ORDER BY no_iwcs, met_ref,row_type) first_row
                  FROM (
                SELECT btw1.no_iwcs,
                       btw1.met_ref,
                       1 row_type,         -- Earliest good reading
                       MIN(btw1.period) earliest_period,
                       MIN(NVL(btw1.start_read,0)) earliest_start_read,
                       MIN(NVL(btw1.end_read,0)) earliest_end_read,
                       MAX(
                           CASE
                              WHEN btw2.period = 16 THEN
                                 TO_NUMBER(NVL(btw2.end_read,0))
                              ELSE
                                 0
                           END) p16_end_read
                  FROM bt_te_working btw1  -- Earliest period data
                  JOIN bt_te_working btw2  -- Period 16 data
                    ON btw1.no_iwcs = btw2.no_iwcs
                       AND btw1.met_ref = btw2.met_ref
                       AND btw2.period = 16
                 WHERE (btw1.CODE LIKE '%A%'    -- Actual Read
                        OR btw1.CODE LIKE '%I%' -- Initial Read
                        OR btw1.CODE LIKE '%b%' -- Actual Read (STWA Employee)
                       )
                       AND btw1.period < 16
                       AND UPPER(btw1.te_category) IN ('PRIVATE TE METER', 'PRIVATE WATER METER')
                  GROUP BY btw1.no_iwcs,
                           btw1.met_ref
                 UNION ALL
                SELECT no_iwcs, 
                       met_ref, 
                       2 row_type,         -- Earliest good reading
                       MIN(period) earliest_period,
                       MIN(NVL(start_read,0)) earliest_start_read,
                       MIN(NVL(end_read,0)) earliest_end_read,
                       MAX(
                           CASE
                              WHEN period = 16 THEN
                                 TO_NUMBER(NVL(end_read,0))
                              ELSE
                                 0
                           END) p16_end_read
                  FROM bt_te_working btw2
                 WHERE period <= 16
                   AND UPPER(te_category) IN ('PRIVATE TE METER', 'PRIVATE WATER METER')
                 GROUP BY no_iwcs, 
                          met_ref)
               ) earliest_row
        WHERE first_row = 1
        ) ilv
    ON ilv.no_iwcs = btw1.no_iwcs
       AND ilv.met_ref = btw1.met_ref
 WHERE UPPER(btw1.te_category) IN ('PRIVATE TE METER', 'PRIVATE WATER METER')
--and btw1.no_iwcs = 10102100101 and btw1.met_ref = 4
  ORDER BY btw1.no_iwcs, btw1.met_ref, btw1.period;

CREATE OR REPLACE VIEW METER_READING_CDV_V AS 
SELECT DISTINCT 
    MANUFACTURER_PK
    , MANUFACTURERSERIALNUM_PK
    , METERREF
    , PREVMETERREADDATE
    , METERREADDATE    
    , PREVIOUSMETERREADING
    , METERREAD
    , NUMBEROFDIGITS
    , ROLLOVERINDICATOR
    , PREVMETERREADTYPE
    , ((METERREAD - PREVIOUSMETERREADING) + ROLLOVERINDICATOR * POWER(10,NUMBEROFDIGITS)) / NULLIF( (METERREADDATE - PREVMETERREADDATE) ,0) AS CDV
FROM (
          SELECT MO.MANUFACTURER_PK
                , MO.MANUFACTURERSERIALNUM_PK
                , MO.METERREF
                , MR.METERREADDATE
                , MR.METERREAD
                , MO.NUMBEROFDIGITS
                , MR.ROLLOVERINDICATOR
                , MR.METERREADTYPE
                ,LAG(MR.METERREADDATE,1) OVER (PARTITION BY MO.MANUFACTURER_PK,MO.MANUFACTURERSERIALNUM_PK ORDER BY MR.METERREADDATE ASC) PREVMETERREADDATE
                ,LAG(MR.METERREAD,1) OVER (PARTITION BY MO.MANUFACTURER_PK,MO.MANUFACTURERSERIALNUM_PK ORDER BY MR.METERREADDATE ASC) PREVIOUSMETERREADING
                ,LAG(MR.METERREADTYPE,1) OVER (PARTITION BY MO.MANUFACTURER_PK,MO.MANUFACTURERSERIALNUM_PK ORDER BY MR.METERREADDATE ASC) PREVMETERREADTYPE
          FROM MO_METER MO
          JOIN MO_METER_READING MR ON MR.METERREF = MO.METERREF
--         WHERE MR.METERREADTYPE <> 'I'
         GROUP BY MO.MANUFACTURER_PK, MO.MANUFACTURERSERIALNUM_PK, MO.METERREF, MR.METERREADDATE, MR.METERREAD, MO.NUMBEROFDIGITS, MR.ROLLOVERINDICATOR, MR.METERREADTYPE
)
--WHERE NOT (PREVMETERREADTYPE = 'I' AND ROLLOVERINDICATOR = 1)
ORDER BY METERREF, METERREADDATE ASC;

CREATE OR REPLACE VIEW METER_YEARLYVOLEST_V AS 
SELECT METERREF, MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, AVGDV, ROUND(AVGDV * 365,2) AS YEARLYVOLEST 
FROM (
    SELECT V1.METERREF, V1.MANUFACTURER_PK, V1.MANUFACTURERSERIALNUM_PK, SUM(V1.CDV)/COUNT(V1.CDV) AS AVGDV
    FROM METER_READING_CDV_V V1
    WHERE CDV IS NOT NULL
        AND CDV >= 0
    GROUP BY V1.METERREF, V1.MANUFACTURER_PK, V1.MANUFACTURERSERIALNUM_PK
)
;
    
COMMIT;
EXIT;