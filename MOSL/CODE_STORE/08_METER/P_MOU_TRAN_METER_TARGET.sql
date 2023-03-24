create or replace
PROCEDURE           P_MOU_TRAN_METER_TARGET (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO Extract
--
-- AUTHOR         : Ola Badmus
--
-- FILENAME       : P_MOU_TRAN_METER_TARGET.sql
--
-- Subversion $Revision: 5458 $
--
-- CREATED        : 11/03/2016
--
-- DESCRIPTION    : Procedure to create the Meter Target Extract
--                 Will read from key gen and target tables, apply any transformation
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         CR/DEF  Description
-- ---------   ---------------     -------        ------  ----------------------------------
-- v 9.24      08/09/2016          D.Cheung               Reverted to previous code version to back out I-352 fixes (report to business instead to fix data)
-- v 9.23      05/09/2016          D.Cheung               I-352 - Change to get CORESPID, SPID and FG_NMM from LOGICAL properties
-- V 9.22      25/08/2016          D.Cheung               I-350 - Duplicate Meterref exception caused by RTS returning two values
-- V 9.21      23/08/2016          D.Cheung               Appending properties in LU_SPID_OWC_RETAILER 
-- v 9.20      22/08/2016          D.Cheung               I-342 - Fixed join issue on RTS
-- v 9.19      16/09/2016          D.Cheung               I-342 - Use PC_USAGE_SPLIT value for RTS
--                                                        I-343 - If Tariff on ServiceComponent has Water Charge then set WaterChargeableMeterSize to PhysicalMeterSize
-- v 9.18      15/08/2016          D.Cheung               I-339 - Default FG_NMM to Y if installed property is not in an eligible premise
-- v 9.17      10/08/2016          L.Smith                SI-031 - New measures required for reconciliation
-- v 9.16      01/08/2016          D.Cheung               I-299 - change logic to get correct corespids for network subs-meters
--                                              S_QC_138  SAP-defect 138 - Can't have NULL RTS (MOSL technical guidance indicates will set to default value anyway if NULL)
-- v 9.15      19/07/2016          D.Cheung       CR_031  Workaround for remaining GIS exceptions - set to MOSL default values, output as warnings
-- v 9.14      13/07/2016          D.Cheung       CR_029  Add warning workaround for Not known Outreader protocols
-- v 9.13      12/07/2016          D.Cheung       CR_028  Add exception check to see if MPW Service Component Exists for SPID
-- V 9.12      12/07/2016          D.Cheung     S_CR_014  SAP Change to add new field UNITOFMEASURE with original target code
-- V 9.11      11/07/2016          D.Cheung               I-286 - Join logic setting nonmarketmeter flags incorrectly and duplicating meters
-- V 9.10      08/07/2016          D.Cheung               I-281 - No Meter network records due to dropped meters
--                                                        IF AGG_NET use NO_PROPERTY_MASTER
-- V 9.09      07/07/2016          D.Cheung     S_CR_007  SAP Change - Add field for METER_MODEL from Ttarget TVP063EQUIPMENT.CD_MODEL
--                                              S_CR_007  SAP Change - Change MeterLocationDesc rule to move LOC_STD_EQUIP_41 value
-- V 9.08      06/07/2016          D.Cheung       CR_021  Fix Multiple SPIDS - Change joins on cursor to only pick up MAIN meter records and logical property = installed property
--                                                        Add filter for DT_END IS NOT NULL to main cursor
-- V 9.07      05/07/2016          D.Cheung       CR_021  Add MASTER_PROPERTY for Aggregated Properties
-- V 9.06      04/07/2016          D.Cheung       CR_021  Fix non-market meters being dropped - SPID RULE ISSUE
-- V 9.05      29/06/2016          K.Burton               Issue - I-261 Change to GIS coordinates validation rules - now calls
--                                                        function FN_VALIDATE_GIS
-- V 9.04      29/06/2016          D.Cheung       CR_017  Reopen CR_17 If Outreader_ID is NULL and NOT TOUCH read type set to Unavailable
--                                                CR_021  Change to NON-MARKET METERS following changes to Aggregate Properties
-- V 9.03      28/06/2016          D.Cheung       CR_025  Change MANUFACTURER/MANUFCODE to use mapping table LU_METER_MANUFACTURER 
-- V 9.02      23/06/2016          L.Smith                Performance changes
-- V 9.01      21/06/2016          D.Cheung               I-241 OutreaderID cannot have spaces
-- V 8.03      14/06/2016          D.Cheung               Update change for Meter OutreaderGIS X and Meter OutreaderGIS Y 
-- V 8.02      13/06/2016          O.Badmus               Issue I-232 - Meter OutreaderGIS X and Meter OutreaderGIS Y 
--                                                        - Must be populated for where OutreaderID(D3039) is provided, otherwise must be un-populated
-- V 8.01      06/06/2016          D.Cheung       CR_018  CR_018 - Add Lookup for SAP Equipment Number
-- V 7.02      27/05/2016          D.Cheung       CR_017  New busines rules for eliminating data Exceptions
--                                                        1.	Missing outreader_id for touchpad meters. Set Outreader_id to ‘Not available for Touchpad’
--                                                        3.	Meter Location Code is Null - Set to Internal (I) where manufacturer is in this list:-
--                                                            (Radio Actaris, Fusion Meters Limited, Smartmeter) - Otherwise set to External (O)
--                                                        4.	Missing Meter Read Frequency These should be excluded so reduce the exception to a warning
--                                                            Meter marked as available but taken out of route – probably not on an active service (e.g. demolished/removed)’
-- V 7.01      26/05/2016          D.Cheung       D_51    MOSL TEST1 Defect m1 - Remove spaces from ManufacturerSerialNum
--                                                        Swap Manufacturer and ManufCode
--                                                D_54    MOSL TEST1 Defect m4 - non-market meter should not have GIS and Y
-- V 6.01      19/05/2016          D.Cheung       CR_014  MOSL defect - new MANUFACTURERCODE field mapping
--                                                        Corrected minor bug with Quarterly meter read warnings
--                                                CR_014  Add new fields to split FreeDescriptor values - for SAP
--                                                        Add new field for InstalledPropertyNumber
-- V 5.01      12/05/2016          D.Cheung       CR_013  Defect D37 - RETURNTOSEWER should not be populated for Water meters - i.e. default to NULL
-- V.4.02      10/05/2016          K.Burton               Added IF condition around YEARLYVOLESTIMATE so it is only populated
--                                                        for SEWERAGE, PRIVATETE and PRIVATEWATER
-- V 4.01      09/05/2016          D.Cheung               REOPENED Issue I-207 - Don't report meter freq-size discrepencies
--                                                        Exclude NULL FREQ (T163.NO_VISITABLE_ITEM = NULL)
-- V 3.02      05/05/2016          K.Burton               REOPENED Issue I-118 - removed link to BT_METER_SPID table from main cursor
--                                                        SPIDs now retrieved from LU_SPID_RANGE directly for W service category
-- V 3.01      29/04/2016          D.Cheung               Issue I-207 - No data found exception (NULL METER FREQUENCY)
-- V 2.04      28/04/2016          D.Cheung               Renamed procedure to align correctly with control document
--                                                D_25    Change Transform on OUTREADERID to drop meter if REMOTEREADFLAG = 1 and ID_SEAL is null
--                                                        Issue I-203 - NULL values for Meter Location Codes - workaround as confirmed by Chris
-- V 2.03      27/04/2016          D.Cheung       CR_006  Design Change:- D3011 Meter Read Frequency for Quarterly reads
--                                                        Fix for Issue I-193 - new transform rule for YEARLYVOLESTIMATE
-- V 2.02      26/04/2016          D.Cheung       CR_010  Added transform rules for OUTREADERPROTOCOL
--                                                CR_010  Revised translation mappings for REMOTEREADTYPE
-- V 2.01      25/04/2016          D.Cheung               PERFORMANCE TWEAKS - Replaced SPID Cursor with PIVOT query
--                                                        PERFORMANCE TWEAKS - Consolidate rules
-- V 1.02      22/04/2016          D.Cheung       CR_004  Add Flag for marketable or non-marketable meter
-- V 1.01      20/04/2016          D.Cheung       D2      Issue I-153 - Correct Rules for MEASUREUNITFREEDESCRIPTOR
--                                                D2      Issue I-155 - Correct Rules for SEWERAGECHARGABLEMETERSIZE and WATERCHARGABLEMETERSIZE and METERTREATMENT
--                                                        Update rules for GISX, GISY, OUTREADERGISX, OUTREADERGISY for latest MOSL requirements update
--                                                CR_005  Issue I-154 - Correct Rules for REMOTEREADTYPE
-- V 0.06      18/04/2016          D.Cheung               Add TRIM at source on cursors to fix rogue exceptions
--                                                        Correct bug on BT_METER_READ_FREQ - insert installed property
-- V 0.05      15/04/2016          D.Cheung               Re-Linked Main Cursor to get based on physical INSTALLED property location
-- V 0.04      13/04/2016          D.Cheung               Added new warning to check for NO_PROPERTY discrepencies between KEYGEN and TVP163
--                                                        Amended Cursor SQL to fix Join issue returning multiple TVP703 rows
-- V 0.03      11/04/2016          D.Cheung               Added new lookup table BT_METER_SPID for pre-procesing SPIDs.
--                                                        Changed T703 to LEFT Join. Consolidated Dataloggers rules into ONE query
--                                                        Added additional values to Meter Locations Rule
-- V 0.02      07/04/2016          D.Cheung               Relink to new BT_TVP163 Meter KeyGen, various fixes for exception issues
--                                                        Took SPID and CD_REG_CONFIG out of main cursor into separate rules
-- V 0.01      16/03/2016          O.Badmus               Initial Draft
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_TARGET';  -- modify
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  L_PROGRESS                    VARCHAR2(100);
  l_prev_prp                    CIS.TVP043METERREG.NO_EQUIPMENT%TYPE; --modify
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  L_ERR                         MIG_ERRORLOG%ROWTYPE;
  L_MO                          MO_METER%ROWTYPE; --modify
  L_FREQ                        BT_METER_READ_FREQ%ROWTYPE; --look up table
  l_hlt                         LU_DATALOGGERS%ROWTYPE; --look up table
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  L_NO_ROW_INSERT               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  L_REC_WRITTEN                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  l_rec_war                     BOOLEAN;
  l_sc_count                    NUMBER;
  l_marketable_meter_cnt        NUMBER;  --SI-031
  l_marketable_new_meter_cnt    NUMBER;  --SI-031
  l_water_charge_cnt            NUMBER;  --v9.19

--**** V 1.03 Performance optimization code
--  l_w_spid                      MO_METER.SPID_PK%TYPE;
--  l_s_spid                      MO_METER.SPID_PK%TYPE;
  
  l_gis_code VARCHAR2(60); -- V 9.05

CURSOR CUR_MET (P_NO_EQUIPMENT_START   CIS.TVP043METERREG.NO_EQUIPMENT%type,
                 P_NO_EQUIPMENT_END     CIS.TVP043METERREG.NO_EQUIPMENT%type)
    IS
      SELECT /*+ FULL(T163) FULL(TV163) FULL(T043) FULL(T225) FULL(TV163_I) FULL(TV163_S) FULL(TV054) FULL(TV163_RTS) PARALLEL(T703,12) PARALLEL(TV163,12)  PARALLEL(T034,12) PARALLEL(T036,12) */
      DISTINCT TRIM(T703.CD_EXT_REF)CD_EXT_REF ,T043.NO_EQUIPMENT
          ,TRIM(T036.NM_PREFERRED) NM_PREFERRED
          ,TRIM(T063.NO_UTL_EQUIP) NO_UTL_EQUIP
          ,TRIM(T225.CD_WATR_MTR_SZ_158) CD_WATR_MTR_SZ_158
          ,MAX(T009.TS_START) TS_START_MAX
          ,TRIM(T053.CD_UNIT_OF_MEASURE) CD_UNIT_OF_MEASURE
          ,CASE WHEN T043.NO_EQUIPMENT = 955002908 THEN 1 ELSE 0 END COMBIMETERFLAG
          ,TRIM(T163.CD_MTR_RD_MTHD_330) CD_MTR_RD_MTHD_330
          ,TRIM(T163.LOC_STD_EQUIP_41) LOC_STD_EQUIP_41
          ,TRIM(T163.ID_SEAL) ID_SEAL
          ,TV163.NO_PROPERTY_INST NO_PROPERTY
          ,TRIM(T163.DS_LOCATION) DS_LOCATION
          ,TRIM(T163.TXT_SPECIAL_INSTR) TXT_SPECIAL_INSTR
          ,CASE WHEN NVL(TRIM(TV163_I.FG_NMM),'Y') = 'Y' THEN NULL ELSE TRIM(TV054.CORESPID) END CORESPID -- CR_021   v9.18
          ,CASE WHEN NVL(TRIM(TV163_I.FG_NMM),'Y') = 'Y' THEN NULL ELSE TRIM(LSR.SPID_PK) END SPID_PK -- V 3.02   v9.18
          ,CASE WHEN NVL(TRIM(TV163_I.FG_NMM),'Y') = 'Y' THEN 1 ELSE 0 END NONMARKETMETERFLAG -- v9.04      v9.18
          ,'POTABLE' METERTREATMENT  -- V 3.02
          ,LSE.SAPEQUIPMENT --v8.01
          ,CASE WHEN TRIM(TV163.AGG_NET) IN ('N','M') THEN NULL ELSE TV163.NO_PROPERTY_MASTER END NO_PROPERTY_MASTER -- v9.07
--          , TV163.NO_PROPERTY_MASTER -- v9.07
          ,TRIM(T063.CD_MODEL) METER_MODEL    --v9.09
          ,MAX(TV163_RTS.PC_USAGE_SPLIT * 100) AS RETURNTOSEWER   --v9.22
          ,CASE WHEN OWC.STWPROPERTYNUMBER_PK IS NULL THEN 0 ELSE 1 END OWC_PROPERTY    --v9.21
      FROM CIS.TVP040LOGICALREG T040
      JOIN CIS.TVP034INSTREGASSGN T034 on (T034.CD_COMPANY_SYSTEM = T040.CD_COMPANY_SYSTEM
          AND T034.NO_COMBINE_054  = T040.NO_COMBINE_054
          AND T034.TP_EQUIPMENT    = T040.TP_EQUIPMENT
          AND T034.CD_REG_SPEC     = T040.CD_REG_SPEC
          AND T034.CD_TARIFF       = T040.CD_TARIFF
          AND T034.NO_TARIFF_GROUP = T040.NO_TARIFF_GROUP
          AND T034.NO_TARIFF_SET   = T040.NO_TARIFF_SET
          AND T034.DT_START_LR     = T040.DT_START
      )
      JOIN CIS.TVP043METERREG T043 ON (T043.CD_COMPANY_SYSTEM = T034.CD_COMPANY_SYSTEM
          AND T043.NO_COMBINE_043 = T034.NO_COMBINE_043
      )
      JOIN CIS.TVP163EQUIPINST T163 ON (T163.NO_EQUIPMENT = T043.NO_EQUIPMENT
          AND TRIM(T163.CD_COMPANY_SYSTEM) = 'STW1'
--          AND TRIM(T163.ST_EQUIP_INST)     = 'A' --Available    -- v9.21
          AND (
              (TRIM(T163.ST_EQUIP_INST) = 'A') --Available
              OR 
              (T163.NO_PROPERTY IN (SELECT STWPROPERTYNUMBER_PK FROM LU_SPID_OWC_RETAILER))
          )  -- v9.21
      )
      JOIN BT_TVP163 TV163 ON (T163.NO_PROPERTY = TV163.NO_PROPERTY_INST
          AND TV163.NO_EQUIPMENT = T163.NO_EQUIPMENT
          AND TV163.NO_EQUIPMENT = T043.NO_EQUIPMENT
          AND TV163.FG_ADD_SUBTRACT = '+'     --v9.08          
          AND TV163.DT_END_054 IS NULL
          AND TV163.CD_SERVICE_PROV = 'W'
      )
      JOIN CIS.TVP063EQUIPMENT    T063 on (T063.NO_EQUIPMENT      = T043.NO_EQUIPMENT
          AND T063.CD_COMPANY_SYSTEM = 'STW1')
      JOIN CIS.TVP036LEGALENTITY T036 on  T036.NO_LEGAL_ENTITY = T063.NO_BUSINESS
      JOIN CIS.TVP009APPLIEDCONFG T009 on (T009.NO_EQUIPMENT = T063.NO_EQUIPMENT
          AND T009.CD_COMPANY_SYSTEM='STW1')    -- ADDED FOR OPTIMIZATION
      JOIN CIS.TVP225WATERMTR T225 on T225.NO_EQUIPMENT = T063.NO_EQUIPMENT
      JOIN CIS.TVP053REGSPEC T053 on (T053.CD_REG_SPEC = T043.CD_REG_SPEC
          AND T053.TP_EQUIPMENT = T043.TP_EQUIPMENT
          AND T043.CD_COMPANY_SYSTEM='STW1'
      )
      LEFT JOIN LU_SPID_OWC_RETAILER OWC ON (TV163.NO_PROPERTY_INST = OWC.STWPROPERTYNUMBER_PK)
      LEFT JOIN LU_SAP_EQUIPMENT LSE ON (LSE.STWMETERREF = T063.NO_EQUIPMENT)   --v8.01
      LEFT JOIN  CIS.TVP703EXTERNREFDET T703 on (T703.NO_PROPERTY = TV163.NO_PROPERTY_INST
          AND T703.NO_SERV_PROV     = TV163.TVP202_NO_SERV_PROV_INST
          AND T703.TP_ENTITY       = 'S' -- Serv Prov
          AND T703.NO_EXT_REFERENCE  = 24 -- Meter Grid Ref
          AND T703.CD_COMPANY_SYSTEM = 'STW1'
      )
-- GET INSTALLED METER DETAILS IF IT IS A NETWORK OR MIXED SUB METER      
      LEFT JOIN BT_TVP163 TV163_S ON (TV163.NO_PROPERTY_INST = TV163_S.NO_PROPERTY 
          AND TV163_S.NO_PROPERTY_MASTER <> TV163_S.NO_PROPERTY
          AND TV163_S.FG_ADD_SUBTRACT = '+' 
          AND TV163_S.AGG_NET IN ('N','M')  
          AND TV163_S.DT_END_054 IS NULL 
          AND TV163_S.CD_SERVICE_PROV = 'W')
-- EVALUATE CORESPID IN ORDER PRIORITY - SUB-METER, AGGREGATE-MASTER, INSTALLED METER
      LEFT JOIN BT_TVP054 TV054 ON (NVL(TV163_S.NO_PROPERTY,NVL(TV163.NO_PROPERTY_MASTER,TV163.NO_PROPERTY_INST)) = TV054.NO_PROPERTY)
-- GET NONMARKETMETER FLAG FROM INSTALLED PROPERTY      
      LEFT JOIN BT_TVP163 TV163_I ON (TV163.NO_PROPERTY_INST = TV163_I.NO_PROPERTY) --CR_021
      LEFT JOIN LU_SPID_RANGE LSR ON (TV054.CORESPID = LSR.CORESPID_PK
          AND LSR.SERVICECATEGORY = 'W')
-- GET SEWERAGE PC_USAGE_SPLIT FROM S SERVICE PROVISION   V9.19    
      LEFT JOIN BT_TVP163 TV163_RTS ON (TV163_RTS.NO_PROPERTY_INST = TV163.NO_PROPERTY_INST
          AND TV163_RTS.NO_EQUIPMENT = TV163.NO_EQUIPMENT
          AND TV163_RTS.FG_ADD_SUBTRACT = '+'
          AND TV163_RTS.DT_END_054 IS NULL
          AND TV163_RTS.CD_SERVICE_PROV = 'S'
      )
--      WHERE T043.NO_EQUIPMENT BETWEEN 1 AND  999999999
      WHERE T043.NO_EQUIPMENT BETWEEN P_NO_EQUIPMENT_start AND  P_NO_EQUIPMENT_end
--      AND TV163.NO_PROPERTY_INST IN (SELECT STWPROPERTYNUMBER_PK FROM LU_SPID_OWC_RETAILER)
      AND TV163.CD_COMPANY_SYSTEM = 'STW1'
          AND T053.CD_COMPANY_SYSTEM='STW1'
          AND T040.NO_TARIFF_GROUP =1
          AND T040.NO_TARIFF_SET = 1
    group by TRIM(T703.CD_EXT_REF), T043.NO_EQUIPMENT, TRIM(T036.NM_PREFERRED), TRIM(T063.NO_UTL_EQUIP), TRIM(T225.CD_WATR_MTR_SZ_158), TRIM(T053.CD_UNIT_OF_MEASURE), CASE WHEN T043.NO_EQUIPMENT = 955002908 THEN 1 ELSE 0 END, TRIM(T163.CD_MTR_RD_MTHD_330), TRIM(T163.LOC_STD_EQUIP_41), TRIM(T163.ID_SEAL), TV163.NO_PROPERTY_INST, TRIM(T163.DS_LOCATION), TRIM(T163.TXT_SPECIAL_INSTR), CASE WHEN NVL(TRIM(TV163_I.FG_NMM),'Y') = 'Y' THEN NULL ELSE TRIM(TV054.CORESPID) END, CASE WHEN NVL(TRIM(TV163_I.FG_NMM),'Y') = 'Y' THEN NULL ELSE TRIM(LSR.SPID_PK) END, CASE WHEN NVL(TRIM(TV163_I.FG_NMM),'Y') = 'Y' THEN 1 ELSE 0 END, 'POTABLE', LSE.SAPEQUIPMENT, CASE WHEN TRIM(TV163.AGG_NET) IN ('N','M') THEN NULL ELSE TV163.NO_PROPERTY_MASTER END, TRIM(T063.CD_MODEL), CASE WHEN OWC.STWPROPERTYNUMBER_PK IS NULL THEN 0 ELSE 1 END
    ORDER BY T043.NO_EQUIPMENT;

type TAB_METER is table of CUR_MET%ROWTYPE index by PLS_INTEGER;
T_MET TAB_METER;

--create table sub procedure

BEGIN

    l_PROGRESS := 'Start';
    l_ERR.TXT_DATA := C_MODULE_NAME;
    l_err.TXT_KEY := 0;
    l_job.NO_INSTANCE := 0;
    l_no_row_read := 0;
    L_NO_ROW_INSERT := 0;
    l_no_row_dropped := 0;
    l_no_row_war := 0;
    l_no_row_err := 0;
    l_no_row_exp := 0;
    l_prev_prp := 0;
    l_marketable_meter_cnt := 0;      --SI-031
    l_marketable_new_meter_cnt := 0;  --SI-031
    l_job.IND_STATUS := 'RUN';

    -- get job no
    P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name,
                         l_job.NO_INSTANCE,
                         l_job.ERR_TOLERANCE,
                         l_job.EXP_TOLERANCE,
                         l_job.WAR_TOLERANCE,
                         l_job.NO_COMMIT,
                         l_job.NO_STREAM,
                         l_job.NO_RANGE_MIN,
                         l_job.NO_RANGE_MAX,
                         L_JOB.IND_STATUS);


    BEGIN
        l_progress := 'Rebuilding BT_METER_READ_FREQ lookup';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_METER_READ_FREQ';
        EXECUTE IMMEDIATE 'ALTER INDEX INDEX3 UNUSABLE';
        --delete from  BT_METER_READ_FREQ;
        INSERT /*+ append */
               INTO BT_METER_READ_FREQ
        SELECT /*+ FULL(T163) FULL(TV163) FULL(T249) FULL(T200) PARALLEL(T163,12) PARALLEL(TV163,12)  PARALLEL(T249,12) PARALLEL(T200,12) */
        DISTINCT TRIM(T249.CD_SCHED_FREQ) CD_SCHED_FREQ, TRIM(T235.DS_SCHED_FREQ) DS_SCHED_FREQ
-- **** DE-LINK LOGICAL ASSOCIATION AND BASE METER KEYGEN JOIN ON PHYSICAL INSTALLED LOCATION
        , TV163.NO_PROPERTY_INST NO_PROPERTY
        , T163.NO_EQUIPMENT
        FROM CIS.TVP235SCHEDFREQ T235,
            CIS.TVP200ROUTE     T200,
            CIS.TVP249BILLINGCYCLE T249,
            CIS.TVP346VISITABLEITM T346,
            CIS.TVP163EQUIPINST T163,
            -- **** RELINK TO NEW METER KEYGEN INSTEAD *****
            --      BT_TVP054 TV54
            BT_TVP163 TV163
        WHERE TRIM(T235.CD_SCHED_FREQ)     = TRIM(t249.CD_SCHED_FREQ)
            and TRIM(T235.CD_COMPANY_SYSTEM) = 'STW1'
            and TV163.NO_PROPERTY_INST = T163.NO_PROPERTY    -- RELINK TO BASE ON INSTALLED PROPERTY
            AND TV163.NO_EQUIPMENT = T163.NO_EQUIPMENT
            AND T249.NO_BILLING_CYCLE  = T200.NO_BILLING_CYCLE
            AND TRIM(T249.CD_COMPANY_SYSTEM) = 'STW1'
            AND TRIM(T200.CD_COMPANY_SYSTEM) = 'STW1'
            AND T200.NO_ROUTE          = T346.NO_ROUTE
            AND TRIM(T346.CD_COMPANY_SYSTEM) = 'STW1'
            AND T346.NO_VISITABLE_ITEM = T163.NO_VISITABLE_ITEM
            and TRIM(T163.CD_COMPANY_SYSTEM) = 'STW1'
            and TRIM(T163.ST_EQUIP_INST)     = 'A';
        commit;
        EXECUTE IMMEDIATE 'ALTER INDEX INDEX3 REBUILD';
--        DBMS_STATS.gather_table_stats('MOUTRAN', 'BT_METER_READ_FREQ');
    END;

    l_progress := 'processing ';

    IF l_job.IND_STATUS = 'ERR' THEN
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
        return_code := -1;
        RETURN;
    END IF;

    -- process all records for range supplied
    OPEN cur_met (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX); -- modify

    l_progress := 'loop processing ';

    LOOP

        FETCH cur_met BULK COLLECT INTO t_met LIMIT l_job.NO_COMMIT;

        l_no_row_read := l_no_row_read + t_met.COUNT;

        FOR i IN 1..t_met.COUNT
        LOOP

            L_ERR.TXT_KEY := t_met(I).NO_EQUIPMENT; -- modify
            L_MO := null;
            L_REC_EXC := false;
            L_REC_WAR := false;

-- *** v9.06 - check for NULL SPID_PK on marketable meter
            L_PROGRESS := 'CHECK SPID AVAILABLE FOR MARKETABLE METER';
            IF (t_met(I).SPID_PK IS NULL AND t_met(i).NONMARKETMETERFLAG = 0) THEN
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NULL SPID_PK on Marketable meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
-- *** v9.13                
            ELSIF (t_met(I).SPID_PK IS NOT NULL) THEN
                BEGIN
                    SELECT COUNT(*)
                    INTO   l_sc_count
                    FROM   MO_SERVICE_COMPONENT
                    WHERE  SPID_PK = t_met(I).SPID_PK
                        AND SERVICECOMPONENTTYPE = 'MPW';
                    
                    IF (l_sc_count = 0) THEN
                        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO MPW Service Component for POTABLE Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                        L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                        L_REC_EXC := true;
                    END IF;
                END;
            END IF;

--*** v6.01 - ManufacturerCode mapping
            L_PROGRESS := 'GETTING MANUFACTURER (CODE)';
            L_MO.MANUFCODE := T_MET(I).NM_PREFERRED;                                  --V7.01
--*** v9.03            
            BEGIN
                SELECT MANUFCODE
                    INTO   L_MO.MANUFACTURER_PK
                FROM   LU_METER_MANUFACTURER
                WHERE  MANUFACTURER_PK = UPPER(L_MO.MANUFCODE);
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Manufacturer not in lookup mappings',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                    L_REC_EXC := true;
            END;
            --L_MO.MANUFACTURER_PK := REPLACE(UPPER(T_MET(I).NM_PREFERRED),' ','_');    --V7.01
            L_MO.MANUFACTURERSERIALNUM_PK := REPLACE(UPPER(T_MET(I).NO_UTL_EQUIP),' ','');    --V7.01
--*** v6.01

            L_PROGRESS := 'GETTING PHYSICALMETERSIZE';
            --PHYSICALMETERSIZE
            if T_MET(I).CD_WATR_MTR_SZ_158 ='g'
                then L_MO.PHYSICALMETERSIZE := 100;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='h'
                then L_MO.PHYSICALMETERSIZE := 150;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='a'
                then L_MO.PHYSICALMETERSIZE := 15;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='i'
                then L_MO.PHYSICALMETERSIZE := 200;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='b'
                then L_MO.PHYSICALMETERSIZE := 22;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='j'
                then L_MO.PHYSICALMETERSIZE := 250;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='c'
                then L_MO.PHYSICALMETERSIZE := 28;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='k'
                then L_MO.PHYSICALMETERSIZE := 300;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='d'
                then L_MO.PHYSICALMETERSIZE := 42;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='e'
                then L_MO.PHYSICALMETERSIZE := 50;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='f'
                then L_MO.PHYSICALMETERSIZE := 80;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='x'
                then L_MO.PHYSICALMETERSIZE := 0;
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid  x PHYSICALMETERSIZE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='y'
                then L_MO.PHYSICALMETERSIZE := 0;
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid y PHYSICALMETERSIZE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            ELSIF T_MET(I).CD_WATR_MTR_SZ_158 ='z'
                then L_MO.PHYSICALMETERSIZE := 0;
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid z PHYSICALMETERSIZE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            end if;

            L_PROGRESS := 'SELECT Latest CD_REG_CONFIG based on MAX(TS_START)';
            -- ADDED TO GET UNIQUE CD_REG_CONFIG for latest TS_START
            BEGIN
                L_MO.NUMBEROFDIGITS := NULL;
                SELECT SUBSTR(CD_REG_CONFIG,1,1)
                into   L_MO.NUMBEROFDIGITS
                from   CIS.TVP009APPLIEDCONFG
                WHERE  NO_EQUIPMENT = t_met(i).NO_EQUIPMENT
                    AND TS_START = t_met(i).TS_START_MAX;
            EXCEPTION
                when NO_DATA_FOUND then
                    L_ERROR_NUMBER := SQLCODE;
                    L_ERROR_MESSAGE := SQLERRM;
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                    L_REC_EXC := true;
            end;


            if T_MET(I).CD_UNIT_OF_MEASURE = 'M3'
                then L_MO.MEASUREUNITATMETER := 'METRICm3';
            ELSIF T_MET(I).CD_UNIT_OF_MEASURE = 'GAL'
                then L_MO.MEASUREUNITATMETER := 'METRICNONm3';
            end if;

            if T_MET(I).CD_UNIT_OF_MEASURE = 'M3'
                then L_MO.MEASUREUNITFREEDESCRIPTOR := NULL;    --V1.01 - I-153 - FIX MEASUREUNITFREEDESCRIPTOR RULE
            ELSIF T_MET(I).CD_UNIT_OF_MEASURE = 'GAL'
                then L_MO.MEASUREUNITFREEDESCRIPTOR := 'Gallons';
            end if;

            if T_MET(I).CD_MTR_RD_MTHD_330 in ('c','b','d')
                then L_MO.REMOTEREADFLAG := 1;
            else
                L_MO.REMOTEREADFLAG := 0;
            end if;

            L_PROGRESS := 'GETTING REMOTEREADTYPE';
            if T_MET(I).CD_MTR_RD_MTHD_330 = 'a' THEN
                L_MO.REMOTEREADTYPE := NULL;     --V1.01 - I-154 - FIX REMOTEREADTYPE RULE
            ELSIF T_MET(I).CD_MTR_RD_MTHD_330 = 'b' THEN
                L_MO.REMOTEREADTYPE := 'TOUCH';
--**** CR_010 REVISED TRANSLATION MAPPINGS FOR REMOTEREADTYPE
            ELSIF  T_MET(I).CD_MTR_RD_MTHD_330 = 'c' THEN
                L_MO.REMOTEREADTYPE := '2WRAD';
            ELSIF  T_MET(I).CD_MTR_RD_MTHD_330 = 'd' THEN
                L_MO.REMOTEREADTYPE := '2WRAD';
            ELSE
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NULL REMOTEREADTYPE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
--**** CR_010
            END IF;

            L_PROGRESS := 'SELECT BT_METER_READ_FREQ';
            BEGIN
                L_FREQ.CD_SCHED_FREQ := NULL;
                SELECT CD_SCHED_FREQ
                into   L_FREQ.CD_SCHED_FREQ
                from   BT_METER_READ_FREQ
                WHERE  NO_EQUIPMENT = t_met(i).NO_EQUIPMENT
                    AND NO_PROPERTY = t_met(i).NO_PROPERTY;	    -- Added to Fix ORA-01422 error
            EXCEPTION
                when NO_DATA_FOUND then
--v9.21 - FORCE OWC properties
                    IF (t_met(i).OWC_PROPERTY = 1) THEN
                        IF (L_MO.PHYSICALMETERSIZE >= 80) THEN
                            L_FREQ.CD_SCHED_FREQ := 'M';
                        ELSE
                            L_FREQ.CD_SCHED_FREQ := 'B';
                        END IF;
                    ELSE
                        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Meter marked available but out of route,probably not on an active service(e.g.demolished/removed)',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                        L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                        L_REC_EXC := true;
                    END IF;
--v9.21
            end;

            L_PROGRESS := 'drop quarterly read rows';
            -- to handle business rules
            if L_FREQ.CD_SCHED_FREQ ='Q' THEN
                --L_FREQ.CD_SCHED_FREQ := 'QUARTERLY'; -- warning for quarterly read rows
--***CR_006 Design Change:- D3011 Meter Read Frequency for Quarterly reads
                IF (L_MO.PHYSICALMETERSIZE >= 80) THEN
                    L_FREQ.CD_SCHED_FREQ := 'M';
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-Quarterly read converted to Monthly',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                    L_REC_WAR := true;
                ELSE
                    L_FREQ.CD_SCHED_FREQ := 'B';
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-Quarterly read converted to Bi-annually',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                    L_REC_WAR := true;
                END IF;
--*** V4.01 - REOPENED Issue I-207 - no need to report meter size discepencies
            --ELSIF  (L_MO.PHYSICALMETERSIZE >= 80 and L_FREQ.CD_SCHED_FREQ <> 'M') -- check for B
            --    then --L_FREQ.CD_SCHED_FREQ := 'Wrong Bi- Annual or QUARTERLY'; -- output warning with meter size greater than 80 that are not monthly
            --    L_FREQ.CD_SCHED_FREQ := 'M';
            --    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-Bi-Annual read where meter size > 80 converted to monthly',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
            --    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
            --    L_REC_WAR := true;
--*** V4.01 - REOPENED Issue I-207
--***CR_006
            ELSIF  (L_FREQ.CD_SCHED_FREQ = 'S') -- map valid S (Bi-Annual) values to B
                THEN L_FREQ.CD_SCHED_FREQ := 'B';
            ELSE
                L_FREQ.CD_SCHED_FREQ := L_FREQ.CD_SCHED_FREQ;
            end if;

            L_PROGRESS := 'Getting METERLOCATIONCODE';
            if T_MET(I).LOC_STD_EQUIP_41 in ('A',	'A1',	'A2',	'A3',	'A4',	'A5',	'B1',	'B5',	'BB',	'C1',	'C5',	'D4',	'D5',	'E2',	'E3',	'E4',	'D1',	'EX',	'R2',	'R3',	'F1',	'F2')
                then L_MO.METERLOCATIONCODE := 'O';
            elsif  T_MET(I).LOC_STD_EQUIP_41 in ('T1',	'T2',	'T3',	'T4',	'T5',	'T6',	'T7',	'T8',	'T9',	'IN',	'TE',	'TF',	'TG',	'TH',	'R1',	'R4',	'F3')
                then L_MO.METERLOCATIONCODE := 'I';
            -- **** MISSING VALUES ON MAPPING - PENDING UPDATE OF F and V DOC
            elsif  T_MET(I).LOC_STD_EQUIP_41 in ('TA',	'TB',	'TC',	'TD')
                then L_MO.METERLOCATIONCODE := 'I';
--**** v2.04 WORKAROUND for Issue I-203 (CR_017)
            ELSIF T_MET(I).LOC_STD_EQUIP_41 IS NULL THEN
                CASE
                    WHEN t_met(i).NM_PREFERRED = 'Smartmeter' THEN L_MO.METERLOCATIONCODE := 'I'; -- IN, Internal Visual
                    WHEN t_met(i).NM_PREFERRED = 'Fusion Meters Limited' THEN L_MO.METERLOCATIONCODE := 'I'; -- IN, Internal Visual
                    WHEN t_met(i).NM_PREFERRED = 'Radio Actaris' THEN L_MO.METERLOCATIONCODE := 'I'; -- IN, Internal Visual
                    ELSE L_MO.METERLOCATIONCODE := 'O'; -- BB, Boundary Box
                END CASE;
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-NULL Meter Location',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                L_REC_WAR := true;
--**** v2.04 I-203
--v9.21 - FORCE OWC properties
            ELSIF (t_met(i).OWC_PROPERTY = 1) THEN
                L_MO.METERLOCATIONCODE := 'I';
--v9.21
            else
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Meter Location not in translation table',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            END IF ;

--*** v6.01 - Add new fields to split FreeDescriptor values - for SAP
            L_PROGRESS := 'Getting and Concatinating FreeDescriptor';
            L_MO.METERLOCATIONDESC := CASE TRIM(T_MET(I).LOC_STD_EQUIP_41)
                WHEN 'A' THEN 'Chamber in field '
                WHEN 'A1' THEN 'BB rural verge '
                WHEN 'A2' THEN 'BB left side front '
                WHEN 'A3' THEN 'BB left side in line '
                WHEN 'A4' THEN 'BB left side rear '
                WHEN 'A5' THEN 'BB in field '
                WHEN 'B1' THEN 'BB front left '
                WHEN 'B5' THEN 'BB rear left '
                WHEN 'BB' THEN 'Boundary box '
                WHEN 'C1' THEN 'BB front in line '
                WHEN 'C5' THEN 'BB rear in line '
                WHEN 'D4' THEN 'BB access reqd '
                WHEN 'D5' THEN 'BB rear right '
                WHEN 'E2' THEN 'BB right side front '
                WHEN 'E3' THEN 'BB right side in line '
                WHEN 'E4' THEN 'BB right side rear '
                WHEN 'D1' THEN 'BB front right '
                WHEN 'T1' THEN 'TP location unknown '
                WHEN 'T2' THEN 'TP front centre '
                WHEN 'T3' THEN 'TP front left '
                WHEN 'T4' THEN 'TP front right '
                WHEN 'T5' THEN 'TP rear centre '
                WHEN 'T6' THEN 'TP rear left '
                WHEN 'T7' THEN 'TP rear right '
                WHEN 'T8' THEN 'TP left side centre '
                WHEN 'T9' THEN 'TP left side front '
                WHEN 'IN' THEN 'Internal visual '
                WHEN 'EX' THEN 'External chamber '
                WHEN 'TE' THEN 'TP Basement '
                WHEN 'TF' THEN 'TP internal other '
                WHEN 'TG' THEN 'TP access reqd '
                WHEN 'TH' THEN 'TP MTR CBOARD/RM '
                WHEN 'R1' THEN 'RD no TP/SC '
                WHEN 'R2' THEN 'RD TP/SC on rd '
                WHEN 'R3' THEN 'RD TP/SC off rd '
                WHEN 'R4' THEN 'RD radio '
                WHEN 'F1' THEN 'Radio BB '
                WHEN 'F2' THEN 'Radio chamber '
                WHEN 'F3' THEN 'Radio internal '
                ELSE NULL
            END;
            IF (L_MO.METERLOCATIONDESC IS NOT NULL) THEN
                L_MO.FREEDESCRIPTOR := CONCAT(CONCAT(CONCAT(UPPER(L_MO.METERLOCATIONDESC), T_MET(I).DS_LOCATION),' '),T_MET(I).TXT_SPECIAL_INSTR);
                L_MO.METERLOCATIONDESC := T_MET(I).LOC_STD_EQUIP_41; --v9.09
            END IF;
--*** v6.01
-- V 9.05
            L_PROGRESS := 'Validating CD_EXT_REF length to extract GISX or GISY';
            IF T_MET(I).CD_EXT_REF IS NULL THEN
                    L_MO.GPSX := 82644.0;    
                    L_MO.GPSY := 5186.0; 
            ELSE
                l_gis_code := FN_VALIDATE_GIS(T_MET(I).CD_EXT_REF);
            
                IF l_gis_code LIKE 'Invalid%' THEN
--***v9.15 - workaround - set to MOSL default values
                    L_MO.GPSX := 82644.0;    
                    L_MO.GPSY := 5186.0; 
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-' || l_gis_code,1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                    L_REC_WAR := true;            
--***v9.15
                ELSE
                    L_MO.GPSX := TO_NUMBER(TRIM(SUBSTR(l_gis_code,1,INSTR(l_gis_code,';')-1)));
                    L_MO.GPSY := TO_NUMBER(TRIM(SUBSTR(l_gis_code,INSTR(l_gis_code,';')+1)));      
                END IF;
            END IF;
-- V 9.05

--            L_PROGRESS := 'Checking CD_EXT_REF length to extract GISX or GISY';
--            --GPSX and GPSY transformations
----***V7.01 MOSL TEST1 Defect m4
--                if (T_MET(I).CD_EXT_REF like '%;%' and length(TRIM(T_MET(I).CD_EXT_REF)) < 13) then
--                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid GISX or GISY length',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--                    L_REC_EXC := true;
--                ELSIF (T_MET(I).CD_EXT_REF like '%;%' and length(TRIM(T_MET(I).CD_EXT_REF)) =13) then
--                    IF ((NVL(LENGTH(TRIM(TRANSLATE(SUBSTR(T_MET(I).CD_EXT_REF,1,6), '0123456789',' '))),0) = 0) AND (NVL(LENGTH(TRIM(TRANSLATE(SUBSTR(T_MET(I).CD_EXT_REF,8,6), '0123456789',' '))),0) = 0)) THEN
--                        L_MO.GPSX := SUBSTR(T_MET(I).CD_EXT_REF,1,6);
--                        L_MO.GPSY := SUBSTR(T_MET(I).CD_EXT_REF,8,6);
--                    ELSE
--                        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NON_Numeric GISX or GISY',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--                        L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--                        L_REC_EXC := true;
--                    END IF;
--                ELSIF T_MET(I).CD_EXT_REF is null then 
--                    L_MO.GPSX := 82644.0;    --V1.01 - Update to GISX RULES in MOSL Req.
--                    L_MO.GPSY := 5186.0;          --V1.01 - Update to GISY RULES in MOSL Req.
--                else
--                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid GISX or GISY length > 13',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--                    L_REC_EXC := true;
--                end if;
----***V7.01

--**** V 1.03 Performance optimization code (Consolidate multiple similar rules)
            --OUTREADERID, METEROUTREADERLOCCODE, METEROUTREADERGPSX, METEROUTREADERGPSY, OUTREADERLOCFREEDES
            IF L_MO.REMOTEREADFLAG = 1 THEN
                IF T_MET(I).ID_SEAL IS NULL THEN
--*** v7.02-v9.04 - CR_017 (1) - workaround for null ID_SEAL                
                    IF L_MO.REMOTEREADTYPE = 'TOUCH' THEN
                        L_MO.OUTREADERID := 'Not_available_for_Touchpad';
                    ELSE
                        L_MO.OUTREADERID := 'Unavailable';
                    END IF;
                ELSE
                    L_MO.OUTREADERID := REPLACE(T_MET(I).ID_SEAL,' ','');
                END IF;
                L_MO.METEROUTREADERLOCCODE := 'O';

                IF L_MO.OUTREADERID IS NULL THEN 
                    L_MO.METEROUTREADERGPSX := NULL;
                    L_MO.METEROUTREADERGPSY := NULL;
                ELSE
                    L_MO.METEROUTREADERGPSX := L_MO.GPSX;
                    L_MO.METEROUTREADERGPSY := L_MO.GPSY;
                END IF;
                
                L_MO.OUTREADERLOCFREEDES := L_MO.FREEDESCRIPTOR;

                L_PROGRESS := 'GETTING OUTREADERPROTOCOL';
                BEGIN
                    SELECT DISTINCT OUTREADERPROTOCOL
                    INTO   L_MO.OUTREADERPROTOCOL
                    FROM   LU_OUTREADER_PROTOCOLS
                    WHERE  TRIM(READMETHOD_PK) = T_MET(I).CD_MTR_RD_MTHD_330
                    AND    TRIM(MANUFACTURER_PK)   = T_MET(I).NM_PREFERRED;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        L_MO.OUTREADERPROTOCOL := 'NOT KNOWN';
                        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-OutReader Protocol Unknown',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                        L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                        L_REC_WAR := TRUE;
                END;
                IF (UPPER(L_MO.OUTREADERPROTOCOL) = 'NOT KNOWN') THEN
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-OutReader Protocol Not Known',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                    L_REC_WAR := TRUE;
                END IF;
            END IF;
            
-- *** v8.03 - Set GPSX and Y to NULL if NONMARKETMETER, but still keep OUTREADERGPSX and Y
            IF (t_met(i).NONMARKETMETERFLAG = 1) THEN
                L_MO.GPSX := NULL;
                L_MO.GPSY := NULL;
            END IF;

            L_PROGRESS := 'GETTING WATERCHARGEMETERSIZE';
            --WATERCHARGEMETERSIZE
            if L_MO.PHYSICALMETERSIZE is not null then
--*** v9.19 Check tariff if any water charge component
                IF L_MO.METERTREATMENT = 'SEWERAGE' THEN
                    L_MO.SEWCHARGEABLEMETERSIZE := L_MO.PHYSICALMETERSIZE;
                    L_MO.WATERCHARGEMETERSIZE := 0;
                ELSE
                    L_MO.WATERCHARGEMETERSIZE := L_MO.PHYSICALMETERSIZE;
                      L_MO.SEWCHARGEABLEMETERSIZE := NULL;                  
                END IF;
-- *** NEED TO TEST MOSL VALIDATION AND CHECK WITH SETTLEMENT ****
--                  BEGIN
--                      SELECT COUNT(*) INTO l_water_charge_cnt 
--                      FROM MO_MPW_METER_MWMFC MMMM 
--                          JOIN MO_TARIFF_TYPE_MPW MTTM ON MTTM.TARIFF_TYPE_PK = MMMM.TARIFF_TYPE_PK
--                          JOIN MO_TARIFF_VERSION MTV ON MTV.TARIFF_VERSION_PK = MTTM.TARIFF_VERSION_PK
--                          JOIN MO_SERVICE_COMPONENT MSC ON MSC.TARIFFCODE_PK = MTV.TARIFFCODE_PK 
--                      WHERE SERVICECOMPONENTTYPE = 'MPW'
--                      AND MSC.SPID_PK = T_MET(I).SPID_PK;
--                  END;
--                  IF l_water_charge_cnt > 0 THEN
--                      L_MO.WATERCHARGEMETERSIZE := L_MO.PHYSICALMETERSIZE;
--                  ELSE
--                      L_MO.WATERCHARGEMETERSIZE := 0;
--                  END IF;
--                  L_MO.SEWCHARGEABLEMETERSIZE := NULL;
--*** v9.19
            ELSIF  L_MO.PHYSICALMETERSIZE is null
                then
                --L_MO.WATERCHARGEMETERSIZE := 'Invalid';
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Null PHYSICALMETERSIZE',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            end if;

--*** I-193 - New rule for YEARLYVOLESTIMATE
            L_PROGRESS := 'GETTING YEARLYVOLESTIMATE';
            IF TRIM(T_MET(I).METERTREATMENT) IN ('SEWERAGE','PRIVATETE','PRIVATEWATER') THEN  -- V 4.02
              BEGIN
                  SELECT DISTINCT
                      NVL(round(sum(t195.am_usage) / sum(t195.no_day_prev_read) * 365),0) as Est_365
                  INTO L_MO.YEARLYVOLESTIMATE
                  FROM cis.tvp195reading    t195
                  WHERE T195.CD_COMPANY_SYSTEM = 'STW1'
                      AND t195.no_equipment = T_MET(I).NO_EQUIPMENT
                      AND T195.st_reading_168     = 'B'
                      AND T195.ts_captured        > to_date('2014-10-01','YYYY-MM-DD')
                      AND MONTHS_BETWEEN(SYSDATE, T195.TS_CAPTURED) <= 24
                      AND T195.no_day_prev_read   <> 0
                      --AND T195.am_usage           <> 0
                      AND t195.tp_reading IN ('M','I')
                  ;
              EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                      L_MO.YEARLYVOLESTIMATE := 0;
              END;
            ELSE
              L_MO.YEARLYVOLESTIMATE := NULL;   -- V 4.02
            END IF;
--*** I-193

            L_PROGRESS := 'SELECTing NONWHOLESALER LU_DATALOGGERS';
            BEGIN
                SELECT DISTINCT NVL(DATALOGGERNONWHOLESALER,0), NVL(DATALOGGERWHOLESALER,0)
                INTO   l_hlt.DATALOGGERNONWHOLESALER, l_hlt.DATALOGGERWHOLESALER
                FROM   LU_DATALOGGERS
                where  STWPROPERTYNUMBER_PK = T_MET(I).NO_PROPERTY
                    AND    MANUFACTURERSERIALNUM_PK   = T_MET(I).NO_UTL_EQUIP;
            EXCEPTION
                when NO_DATA_FOUND then
                    l_hlt.DATALOGGERNONWHOLESALER := 0;
                    l_hlt.DATALOGGERWHOLESALER := 0;
            end;

            if L_REC_EXC = true then  --using this if statement to pick up data that passes the biz rules into the table otherwise they will be dropped and would have be sent into the exception table
                IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                    OR l_no_row_err > l_job.ERR_TOLERANCE
                    OR l_no_row_war > l_job.WAR_TOLERANCE)
                THEN
                    CLOSE cur_met;
                    L_JOB.IND_STATUS := 'ERR';
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                    commit;
                    return_code := -1;
                    return;
                end if;

                L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
            else
                IF (L_REC_WAR = true AND l_no_row_war > l_job.WAR_TOLERANCE) THEN
                    CLOSE cur_met;
                    L_JOB.IND_STATUS := 'ERR';
                    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Warning tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                    commit;
                    return_code := -1;
                    return;
                END IF;
                L_REC_WRITTEN := true;
                l_progress := 'loop processing ABOUT TO INSERT INTO TABLE';
                begin
                    INSERT INTO MO_METER
                        (COMBIMETERFLAG,	DATALOGGERNONWHOLESALER,	DATALOGGERWHOLESALER,	FREEDESCRIPTOR,	GPSX,	GPSY,	MANUFACTURER_PK,	MANUFACTURERSERIALNUM_PK,	MDVOL,	MEASUREUNITATMETER,	MEASUREUNITFREEDESCRIPTOR
                        ,	METERADDITIONREASON,	METERLOCATIONCODE,	METERLOCFREEDESCRIPTOR,	METERNETWORKASSOCIATION,	METEROUTREADERGPSX,	METEROUTREADERGPSY,	METEROUTREADERLOCCODE,	METERREADFREQUENCY,	METERREF,	METERREMOVALREASON
                        ,	METERTREATMENT,	NUMBEROFDIGITS,	OUTREADERID,	OUTREADERLOCFREEDES,	OUTREADERPROTOCOL,	PHYSICALMETERSIZE,	REMOTEREADFLAG,	REMOTEREADTYPE,	RETURNTOSEWER,	SEWCHARGEABLEMETERSIZE,	SPID_PK,	WATERCHARGEMETERSIZE,	YEARLYVOLESTIMATE
                        , NONMARKETMETERFLAG  --*** CR_04 flag for marketable or nonmarketable meter
                        , METERLOCATIONDESC, METERLOCSPECIALLOC, METERLOCSPECIALINSTR, MANUFCODE, INSTALLEDPROPERTYNUMBER  --*** v6.01
                        , SAPEQUIPMENT  --v8.01
                        , MASTER_PROPERTY  --v9.07
                        , METER_MODEL   --v9.09
                        , UNITOFMEASURE --v9.12
                        )
                    VALUES
                        (T_MET(I).COMBIMETERFLAG, L_HLT.DATALOGGERNONWHOLESALER,L_HLT.DATALOGGERWHOLESALER,TRIM(L_MO.FREEDESCRIPTOR),L_MO.GPSX,L_MO.GPSY,TRIM(L_MO.MANUFACTURER_PK),TRIM(l_mo.MANUFACTURERSERIALNUM_PK),NULL, TRIM(L_MO.MEASUREUNITATMETER),TRIM(L_MO.MEASUREUNITFREEDESCRIPTOR)
                        ,NULL,TRIM(L_MO.METERLOCATIONCODE),TRIM(L_MO.FREEDESCRIPTOR),0, L_MO.METEROUTREADERGPSX,L_MO.METEROUTREADERGPSY,TRIM(L_MO.METEROUTREADERLOCCODE),TRIM(L_FREQ.CD_SCHED_FREQ),T_MET(I).NO_EQUIPMENT,NULL
                        ,TRIM(T_MET(I).METERTREATMENT),L_MO.NUMBEROFDIGITS,TRIM(L_MO.OUTREADERID),TRIM(L_MO.OUTREADERLOCFREEDES),TRIM(L_MO.OUTREADERPROTOCOL),L_MO.PHYSICALMETERSIZE,L_MO.REMOTEREADFLAG,TRIM(L_MO.REMOTEREADTYPE),T_MET(I).RETURNTOSEWER,L_MO.SEWCHARGEABLEMETERSIZE, TRIM(T_MET(I).SPID_PK), L_MO.WATERCHARGEMETERSIZE, L_MO.YEARLYVOLESTIMATE
                        , T_MET(I).NONMARKETMETERFLAG  --*** CR_04 flag for marketable or nonmarketable meter
                        , L_MO.METERLOCATIONDESC, T_MET(I).DS_LOCATION, T_MET(I).TXT_SPECIAL_INSTR, L_MO.MANUFCODE, T_MET(I).NO_PROPERTY  --*** v6.01
                        , T_MET(I).SAPEQUIPMENT   --v8.01
                        , T_MET(I).NO_PROPERTY_MASTER  --v9.07
                        , T_MET(I).METER_MODEL    --v9.09
                        , T_MET(I).CD_UNIT_OF_MEASURE   --v9.12
                        );
                EXCEPTION
                    WHEN OTHERS THEN
                        L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
                        l_rec_written := FALSE;
                        l_error_number := SQLCODE;
                        l_error_message := SQLERRM;

                        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_ERR.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                        l_no_row_exp := l_no_row_exp + 1;

                        -- if tolearance limit has een exceeded, set error message and exit out
                        IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                            OR l_no_row_err > l_job.ERR_TOLERANCE
                            OR l_no_row_war > l_job.WAR_TOLERANCE)
                        THEN
                            CLOSE cur_met;
                            L_JOB.IND_STATUS := 'ERR';
                            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                            P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                            commit;
                            return_code := -1;
                            RETURN;
                        END IF;
                END;

                IF l_rec_written THEN
                    l_no_row_insert := l_no_row_insert + 1;
                    IF T_MET(I).NONMARKETMETERFLAG = 0 THEN                             --SI-031
                      l_marketable_meter_cnt := l_marketable_meter_cnt + 1;             --SI-031
                      IF T_MET(I).SAPEQUIPMENT IS NULL THEN                             --SI-031
                         l_marketable_new_meter_cnt := l_marketable_new_meter_cnt + 1;  --SI-031
                      END IF;                                                           --SI-031
                    END IF;                                                             --SI-031
                end if;
            end if;  --close of if  L_REC_EXC statement
            l_prev_prp := t_met(i).NO_EQUIPMENT;

        END LOOP;

        IF t_met.COUNT < l_job.NO_COMMIT THEN
            EXIT;
        ELSE
            commit;
        END IF;

    END LOOP;

    CLOSE cur_met;
    -- write counts
    l_progress := 'Writing Counts';

    --  the recon key numbers used will be specific to each procedure

    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP32', 1090, L_NO_ROW_READ,    'Distinct Eligible Meters read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP32', 1100, L_NO_ROW_DROPPED, 'Eligible Meters  dropped during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP32', 1110, l_no_row_insert,  'Eligible Meters written to MO_METER during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP32', 1111, l_marketable_meter_cnt,  'Eligible Marketable Meters written to MO_METER during Transform');  --SI-031
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP32', 1112, l_marketable_new_meter_cnt,  'Eligible New Marketable Meters written to MO_METER during Transform');  --SI-031

    --  check counts match

    IF l_no_row_read <> l_no_row_insert + L_NO_ROW_DROPPED THEN
        l_job.IND_STATUS := 'ERR';
        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_no_row_read || ',' || l_no_row_insert, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
        commit;
        return_code := -1;
    ELSE
        l_job.IND_STATUS := 'END';
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    END IF;

    l_progress := 'End';

    commit;

EXCEPTION
WHEN OTHERS THEN
     L_ERROR_NUMBER := SQLCODE;
     L_ERROR_MESSAGE := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY,  SUBSTR(L_ERR.TXT_DATA || ',' || L_PROGRESS,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     commit;
     RETURN_CODE := -1;
END P_MOU_TRAN_METER_TARGET;
/
show errors;
exit;
