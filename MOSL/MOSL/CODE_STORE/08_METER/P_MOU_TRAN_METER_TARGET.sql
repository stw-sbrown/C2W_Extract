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
-- Subversion $Revision: 4036 $
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

--**** V 1.03 Performance optimization code
  l_w_spid                      MO_METER.SPID_PK%TYPE;
  l_s_spid                      MO_METER.SPID_PK%TYPE;


CURSOR CUR_MET (P_NO_EQUIPMENT_START   CIS.TVP043METERREG.NO_EQUIPMENT%type,
                 P_NO_EQUIPMENT_END     CIS.TVP043METERREG.NO_EQUIPMENT%type)
    IS
      SELECT /*+ PARALLEL(T703,12) PARALLEL(TV163,12)  PARALLEL(T034,12) PARALLEL(T036,12) */
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
-- **** DE-LINK LOGICAL ASSOCIATION AND BASE ON PHYSICAL INSTALLED LOCATION
          --,TV163.NO_PROPERTY
          ,TV163.NO_PROPERTY_INST NO_PROPERTY
          ,TRIM(T163.DS_LOCATION) DS_LOCATION
          ,TRIM(T163.TXT_SPECIAL_INSTR) TXT_SPECIAL_INSTR
          ,TRIM(TV054.CORESPID) CORESPID
          ,LSR.SPID_PK -- V 3.02
          ,CASE WHEN TRIM(TV054.CORESPID) IS NULL THEN 1 ELSE 0 END NONMARKETMETERFLAG  -- V 3.02
          ,'POTABLE' METERTREATMENT  -- V 3.02
      FROM CIS.TVP040LOGICALREG T040
-- **** DE-LINK LOGICAL ASSOCIATION AND BASE ON PHYSICAL INSTALLED LOCATION
--      JOIN CIS.TVP054SERVPROVRESP T054 on (T040.NO_COMBINE_054 = T054.NO_COMBINE_054
--          AND  T040.CD_COMPANY_SYSTEM = 'STW1' AND T054.DT_END is null)
--      JOIN BT_TVP163 TV163 ON (T054.NO_PROPERTY = TV163.NO_PROPERTY_INST AND TV163.CD_COMPANY_SYSTEM = 'STW1')  -- CHANGE TO NO_PROPERTY_INST to get unique meters
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
          AND TRIM(T163.ST_EQUIP_INST)     = 'A' --Available
          AND TRIM(T163.NO_VISITABLE_ITEM) IS NOT NULL    -- V4.01
      )
-- **** DE-LINK LOGICAL ASSOCIATION AND BASE METER KEYGEN JOIN ON PHYSICAL INSTALLED LOCATION
      JOIN BT_TVP163 TV163 ON (T163.NO_PROPERTY = TV163.NO_PROPERTY_INST
          AND TV163.NO_EQUIPMENT = T163.NO_EQUIPMENT
          AND TV163.NO_EQUIPMENT = T043.NO_EQUIPMENT
      )
-- ****
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
      LEFT JOIN  CIS.TVP703EXTERNREFDET T703 on (T703.NO_PROPERTY = TV163.NO_PROPERTY_INST
          AND T703.NO_SERV_PROV     = TV163.TVP202_NO_SERV_PROV_INST
          AND T703.TP_ENTITY       = 'S' -- Serv Prov
          AND T703.NO_EXT_REFERENCE  = 24 -- Meter Grid Ref
          AND T703.CD_COMPANY_SYSTEM = 'STW1'
      )
-- ***** JOIN BACK ONTO KEYGEN TO CHECK FOR NON-ELIGABLE PHYSICAL LOCATIONS
      LEFT JOIN BT_TVP054 TV054 ON (TV163.NO_PROPERTY_INST = TV054.NO_PROPERTY)
-- ***** V 3.02 - REOPENED  Issue I-118 - SPIDs now retrieved from lookup for water only
      LEFT JOIN LU_SPID_RANGE LSR ON (TV054.CORESPID = LSR.CORESPID_PK
          AND LSR.SERVICECATEGORY = 'W')
      WHERE T043.NO_EQUIPMENT BETWEEN P_NO_EQUIPMENT_start AND  P_NO_EQUIPMENT_end
--      WHERE T043.NO_EQUIPMENT BETWEEN 245002007 AND 357002011
-- **** DE-LINK LOGICAL ASSOCIATION AND BASE ON PHYSICAL INSTALLED LOCATION
--          AND T054.CD_COMPANY_SYSTEM = 'STW1'
--          AND T054.NO_ACCT_BILL_GROUP = 1
--          AND T054.DT_START > TO_DATE('1901-01-01','YYYY-MM-DD')
          AND TV163.CD_COMPANY_SYSTEM = 'STW1'
          AND T053.CD_COMPANY_SYSTEM='STW1'
          AND T040.NO_TARIFF_GROUP =1
          AND T040.NO_TARIFF_SET = 1
          --AND TV163.NO_EQUIPMENT IN (146002372, 134008018)  -- **** TEST EXAMPLES
  GROUP BY TRIM(T703.CD_EXT_REF), T043.NO_EQUIPMENT, TRIM(T036.NM_PREFERRED), TRIM(T063.NO_UTL_EQUIP), TRIM(T225.CD_WATR_MTR_SZ_158), TRIM(T053.CD_UNIT_OF_MEASURE), CASE WHEN T043.NO_EQUIPMENT = 955002908 THEN 1 ELSE 0 END, TRIM(T163.CD_MTR_RD_MTHD_330), TRIM(T163.LOC_STD_EQUIP_41), TRIM(T163.ID_SEAL), TV163.NO_PROPERTY_INST, TRIM(T163.DS_LOCATION), TRIM(T163.TXT_SPECIAL_INSTR), TRIM(TV054.CORESPID), LSR.SPID_PK, CASE WHEN TRIM(TV054.CORESPID) IS NULL THEN 1 ELSE 0 END, 'POTABLE'
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
        --delete from  BT_METER_READ_FREQ;
        INSERT INTO BT_METER_READ_FREQ
        SELECT /*+ PARALLEL(T163,12) PARALLEL(TV163,12)  PARALLEL(T249,12) PARALLEL(T200,12) */
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

-- ***** V 3.02 REOPENED Issue I-118 - NONMARKETMETERFLAG and METERTREATMENT now derived directly in main cursor
--                        L_PROGRESS := 'CHECKING NULL CORESPID MATCH';
--            --CHECK IF CORESPID IS NULL FOR NO_EQUIPMENT - (i.e. physical property location is not on eligable properties list)
--            IF (t_met(I).CORESPID IS NULL) THEN
----*** CR_04 flag for marketable or nonmarketable meter
--                --IF NO CORESPID - SET TO NON-MARKETABLE
--                l_mo.NONMARKETMETERFLAG := 1;
--                L_MO.SPID_PK := NULL;
--                L_MO.METERTREATMENT := 'POTABLE';
--                --P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NULL CORESPID',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--                --L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--                --L_REC_EXC := true;
--            ELSE
--                L_PROGRESS := 'GETTING SPID AND METERTREATMENT';
--                --GET SPID_PK AND METERTREATMENT
--                --USES BT_METER_SPID lookup table
----**** V 1.03 Performance optimization code
--                --SET TO MARKETABLE
--                l_mo.NONMARKETMETERFLAG := 0;  --*** CR_04 flag for marketable or nonmarketable meter
--                BEGIN
--                    SELECT *
--                    INTO l_w_spid, l_s_spid
--                    FROM (
--                        SELECT DISTINCT
--                            SPID_PK
--                            , SUPPLY_POINT_CODE
--                        FROM BT_METER_SPID
--                        WHERE CORESPID = t_met(i).CORESPID
--                            AND NO_PROPERTY = t_met(i).NO_PROPERTY
--                    )
--                    PIVOT (
--                        MAX(SPID_PK)
--                        FOR SUPPLY_POINT_CODE
--                            IN ('W' WATERSPID, 'S' SEWAGESPID)
--                    );
--                EXCEPTION
--                    WHEN NO_DATA_FOUND THEN
--                        l_w_spid := NULL;
--                        l_s_spid := NULL;
--                        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO SPID FOUND FOR CORESPID',1,100),  t_met(i).NO_PROPERTY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
--                        L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
--                        L_REC_EXC := true;
--                END;
--                IF (l_w_spid IS NULL AND l_s_spid IS NOT NULL) THEN
--                    --only have Sewage meter
--                    L_MO.SPID_PK := l_s_spid;
--                    L_MO.METERTREATMENT := 'SEWERAGE';
--                ELSE
--                    --water only or both - get WATER spid
--                    L_MO.SPID_PK := l_w_spid;
--                    L_MO.METERTREATMENT := 'POTABLE';
--                END IF;
--            END IF;

--*** v6.01 - ManufacturerCode mapping
            L_PROGRESS := 'GETTING MANUFACTURER (CODE)';
            --L_MO.MANUFCODE := REPLACE(UPPER(T_MET(I).NM_PREFERRED),' ','_');
            L_MO.MANUFCODE := CASE TRIM(UPPER(T_MET(I).NM_PREFERRED))
                WHEN 'CAST CONVERSION' THEN 'CastConversion'
                WHEN 'FROST METERS LIMITED' THEN 'Frost'
                WHEN 'FUSION METERS LIMITED' THEN 'Fusion'
                WHEN 'GLENFIELD METERS LIMITED' THEN 'Glenfield'
                WHEN 'KENT METERS LIMITED' THEN 'Kent'
                WHEN 'LEEDS METERS LIMITED' THEN 'Leeds'
                WHEN 'NEPTUNE METERS LIMITED' THEN 'Neptune'
                WHEN 'PONT-A-MOUSSON' THEN 'Pont-a-Mousson'
                WHEN 'RADIO ACTARIS' THEN 'RadioActaris'
                WHEN 'SCHLUMBERGER WATER METERS LTD' THEN 'Schlumberger'
                WHEN 'SENSUS' THEN 'Sensus'
                WHEN 'SMARTMETER' THEN 'Smartmeter'
                WHEN 'SOC-A-MOUSSON' THEN 'Soc-a-Mousson'
                WHEN 'STELLA METERS LIMITED' THEN 'Stella'
                WHEN 'UNKNOWN' THEN 'UNKNOWN'
                ELSE NULL
            END;
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
--*** V4.01 - REOPENED Issue I-207 - revert to exclude NULL FREQ
--*** V3.01 - Issue I-207 - NULL METER FREQENCY
                --IF (L_MO.PHYSICALMETERSIZE < 80) THEN
                --    L_FREQ.CD_SCHED_FREQ := 'B';
                --    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-NULL freqency converted to Bi-annually',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                --    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                --    L_REC_WAR := true;
                --ELSIF (L_MO.PHYSICALMETERSIZE >= 80) THEN
                --    L_FREQ.CD_SCHED_FREQ := 'M';
                --    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('WARN-NULL frequency converted to Monthly',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                --    L_NO_ROW_WAR := L_NO_ROW_WAR + 1;
                --    L_REC_WAR := true;
                --ELSE
                    --L_ERROR_NUMBER := SQLCODE;
                    --L_ERROR_MESSAGE := SQLERRM;
                    --P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NULL Meter Frequency',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                    L_REC_EXC := true;
                --END IF;
--*** V3.01 - Issue I-207
--*** V4.01 - REOPENED Issue I-207
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
--**** v2.04 WORKAROUND for Issue I-203
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
            else
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Meter Location not in translation table',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            END IF ;

--*** v6.01 - Add new fields to split FreeDescriptor values - for SAP
            L_PROGRESS := 'Getting and Concatinating FreeDescriptor';
            L_MO.METERLOCATIONDESC := CASE TRIM(T_MET(I).LOC_STD_EQUIP_41)
                WHEN 'A' THEN 'Chamber in field.'
                WHEN 'A1' THEN 'BB rural verge.'
                WHEN 'A2' THEN 'BB left side front.'
                WHEN 'A3' THEN 'BB left side in line.'
                WHEN 'A4' THEN 'BB left side rear.'
                WHEN 'A5' THEN 'BB in field.'
                WHEN 'B1' THEN 'BB front left.'
                WHEN 'B5' THEN 'BB rear left.'
                WHEN 'BB' THEN 'Boundary box.'
                WHEN 'C1' THEN 'BB front in line.'
                WHEN 'C5' THEN 'BB rear in line.'
                WHEN 'D4' THEN 'BB access reqd.'
                WHEN 'D5' THEN 'BB rear right.'
                WHEN 'E2' THEN 'BB right side front.'
                WHEN 'E3' THEN 'BB right side in line.'
                WHEN 'E4' THEN 'BB right side rear.'
                WHEN 'D1' THEN 'BB front right.'
                WHEN 'T1' THEN 'TP location unknown.'
                WHEN 'T2' THEN 'TP front centre.'
                WHEN 'T3' THEN 'TP front left.'
                WHEN 'T4' THEN 'TP front right.'
                WHEN 'T5' THEN 'TP rear centre.'
                WHEN 'T6' THEN 'TP ear left.'
                WHEN 'T7' THEN 'TP rear right.'
                WHEN 'T8' THEN 'TP left side centre.'
                WHEN 'T9' THEN 'TP left side front.'
                WHEN 'IN' THEN 'Internal visual.'
                WHEN 'EX' THEN 'External chamber.'
                WHEN 'TE' THEN 'TP Basement.'
                WHEN 'TF' THEN 'TP internal other.'
                WHEN 'TG' THEN 'TP access reqd.'
                WHEN 'TH' THEN 'TP mtr/cboard/rm.'
                WHEN 'R1' THEN 'RD no TP/SC.'
                WHEN 'R2' THEN 'RD TP/SC on rd.'
                WHEN 'R3' THEN 'RD TP/SC off rd.'
                WHEN 'R4' THEN 'RD radio.'
                WHEN 'F1' THEN 'Radio BB.'
                WHEN 'F2' THEN 'Radio chamber.'
                WHEN 'F3' THEN 'Radio internal.'
                ELSE NULL
            END;
            IF (L_MO.METERLOCATIONDESC IS NOT NULL) THEN
                L_MO.FREEDESCRIPTOR := CONCAT(CONCAT(CONCAT(L_MO.METERLOCATIONDESC, T_MET(I).DS_LOCATION),'. '),T_MET(I).TXT_SPECIAL_INSTR);
            END IF;
--*** v6.01

            L_PROGRESS := 'Checking CD_EXT_REF length to extract GISX or GISY';
            --GPSX and GPSY transformations
            if (T_MET(I).CD_EXT_REF like '%;%' and length(TRIM(T_MET(I).CD_EXT_REF)) < 13)
                then
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid GISX or GISY length',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            ELSIF (T_MET(I).CD_EXT_REF like '%;%' and length(TRIM(T_MET(I).CD_EXT_REF)) =13)
                then
                IF ((NVL(LENGTH(TRIM(TRANSLATE(SUBSTR(T_MET(I).CD_EXT_REF,1,6), '0123456789',' '))),0) = 0) AND (NVL(LENGTH(TRIM(TRANSLATE(SUBSTR(T_MET(I).CD_EXT_REF,8,6), '0123456789',' '))),0) = 0))
                    THEN
                    L_MO.GPSX := SUBSTR(T_MET(I).CD_EXT_REF,1,6);
                    L_MO.GPSY := SUBSTR(T_MET(I).CD_EXT_REF,8,6);
                ELSE
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NON_Numeric GISX or GISY',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                    L_REC_EXC := true;
                END IF;
            ELSIF T_MET(I).CD_EXT_REF is null
                then L_MO.GPSX := 82644.0;    --V1.01 - Update to GISX RULES in MOSL Req.
                L_MO.GPSY := 5186.0;          --V1.01 - Update to GISY RULES in MOSL Req.
            else
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid GISX or GISY length > 13',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := true;
            end if ;

--**** V 1.03 Performance optimization code (Consolidate multiple similar rules)
            --OUTREADERID, METEROUTREADERLOCCODE, METEROUTREADERGPSX, METEROUTREADERGPSY, OUTREADERLOCFREEDES
            IF L_MO.REMOTEREADFLAG = 1 THEN
                IF T_MET(I).ID_SEAL IS NULL THEN
                    P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('Invalid NULL OUTREADERID when REMOTEREADFLAG = 1',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                    L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                    L_REC_EXC := TRUE;
                ELSE
                    L_MO.OUTREADERID := T_MET(I).ID_SEAL;
                END IF;
                L_MO.METEROUTREADERLOCCODE := 'O';
                L_MO.METEROUTREADERGPSX := L_MO.GPSX;
                L_MO.METEROUTREADERGPSY := L_MO.GPSY;
                L_MO.OUTREADERLOCFREEDES := L_MO.FREEDESCRIPTOR;
--*** CR_010 - ADD TRANSFORM RULES FOR OUTREADERPROTOCOL AGAINST LU TABLE
                L_PROGRESS := 'GETTING OUTREADERPROTOCOL';
                --CASE
                    --WHEN T_MET(I).CD_MTR_RD_MTHD_330 = 'b' THEN L_MO.OUTREADERPROTOCOL := 'Reads are transmitted over a wired connection from the meter via M-Bus protocol and displayed on the touchpad reader display';
                    --WHEN T_MET(I).CD_MTR_RD_MTHD_330 = 'c' AND T_MET(I).NM_PREFERRED = 'Radio Actaris' THEN L_MO.OUTREADERPROTOCOL := 'Reads are requested and delivered via 2-way radio communication over 433MHz frequency using RADIAN protocol, the read is displayed on the meter reader¿s handheld display.';
                    --WHEN T_MET(I).CD_MTR_RD_MTHD_330 = 'd' AND T_MET(I).NM_PREFERRED = 'Radio Actaris' THEN L_MO.OUTREADERPROTOCOL := 'Reads are requested and delivered via 2-way radio communication over 433MHz frequency using RADIAN protocol, the read is displayed on the meter reader¿s handheld display.';
                    --WHEN T_MET(I).CD_MTR_RD_MTHD_330 = 'c' AND T_MET(I).NM_PREFERRED = 'Schlumberger Water Meters Ltd' THEN L_MO.OUTREADERPROTOCOL := 'Reads are requested and delivered via 2-way radio communication over 433MHz frequency using RADIAN protocol, the read is displayed on the meter reader¿s handheld display.';
                    --WHEN T_MET(I).CD_MTR_RD_MTHD_330 = 'd' AND T_MET(I).NM_PREFERRED = 'Schlumberger Water Meters Ltd' THEN L_MO.OUTREADERPROTOCOL := 'Reads are requested and delivered via 2-way radio communication over 433MHz frequency using RADIAN protocol, the read is displayed on the meter reader¿s handheld display.';
                    --ELSE
                        --L_MO.OUTREADERPROTOCOL := 'NOT KNOWN';
                        --P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('OutReader Protocol Not Known',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                        --L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                        --L_REC_EXC := TRUE;
                --END CASE;
                BEGIN
                    SELECT DISTINCT OUTREADERPROTOCOL
                    INTO   L_MO.OUTREADERPROTOCOL
                    FROM   LU_OUTREADER_PROTOCOLS
                    WHERE  TRIM(READMETHOD_PK) = T_MET(I).CD_MTR_RD_MTHD_330
                    AND    TRIM(MANUFACTURER_PK)   = T_MET(I).NM_PREFERRED;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        L_MO.OUTREADERPROTOCOL := 'NOT KNOWN';
                        P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('OutReader Protocol Not Known',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                        L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                        L_REC_EXC := TRUE;
                END;
--*** CR_010
            END IF;

            L_PROGRESS := 'GETTING WATERCHARGEMETERSIZE';
            --WATERCHARGEMETERSIZE
            if L_MO.PHYSICALMETERSIZE is not null then
                IF L_MO.METERTREATMENT = 'SEWERAGE' THEN
                    L_MO.SEWCHARGEABLEMETERSIZE := L_MO.PHYSICALMETERSIZE;
                    L_MO.WATERCHARGEMETERSIZE := 0;
                ELSE
                    L_MO.WATERCHARGEMETERSIZE := L_MO.PHYSICALMETERSIZE;
                    L_MO.SEWCHARGEABLEMETERSIZE := NULL;
                END IF;
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
                        )
                    VALUES
                        (T_MET(I).COMBIMETERFLAG, L_HLT.DATALOGGERNONWHOLESALER,L_HLT.DATALOGGERWHOLESALER,TRIM(L_MO.FREEDESCRIPTOR),L_MO.GPSX,L_MO.GPSY,TRIM(T_MET(I).NM_PREFERRED),TRIM(T_MET(I).NO_UTL_EQUIP),NULL, TRIM(L_MO.MEASUREUNITATMETER),TRIM(L_MO.MEASUREUNITFREEDESCRIPTOR)
                        ,NULL,TRIM(L_MO.METERLOCATIONCODE),TRIM(L_MO.FREEDESCRIPTOR),0, L_MO.METEROUTREADERGPSX,L_MO.METEROUTREADERGPSY,TRIM(L_MO.METEROUTREADERLOCCODE),TRIM(L_FREQ.CD_SCHED_FREQ),T_MET(I).NO_EQUIPMENT,NULL
                        ,TRIM(T_MET(I).METERTREATMENT),L_MO.NUMBEROFDIGITS,TRIM(L_MO.OUTREADERID),TRIM(L_MO.OUTREADERLOCFREEDES),TRIM(L_MO.OUTREADERPROTOCOL),L_MO.PHYSICALMETERSIZE,L_MO.REMOTEREADFLAG,TRIM(L_MO.REMOTEREADTYPE),NULL,L_MO.SEWCHARGEABLEMETERSIZE, TRIM(T_MET(I).SPID_PK), L_MO.WATERCHARGEMETERSIZE, L_MO.YEARLYVOLESTIMATE
                        , T_MET(I).NONMARKETMETERFLAG  --*** CR_04 flag for marketable or nonmarketable meter
                        , L_MO.METERLOCATIONDESC, T_MET(I).DS_LOCATION, T_MET(I).TXT_SPECIAL_INSTR, L_MO.MANUFCODE, T_MET(I).NO_PROPERTY  --*** v6.01
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
/
show errors;
exit;
