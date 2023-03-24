create or replace
PROCEDURE           P_MOU_TRAN_METER_TE (no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO_METER Extract for TE
--
-- AUTHOR         : Dominic Cheung
--
-- FILENAME       : P_MOU_TRAN_METER_TE.sql
--
-- Subversion $Revision: 5458 $
--
-- CREATED        : 24/05/2016
--
-- DESCRIPTION    : Procedure to create the Meter Target Extract for TE meters
--                 Will read from key gen and target tables, apply any transformation
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         CR/DEF  Description
-- ---------   ---------------     -------        ------  ----------------------------------
-- V 0.01      24/05/2016          D.Cheung               Initial Draft
-- V 1.01      01/06/2016          D.Cheung               Fixes for delivery exceptions for MOSL rules
-- V 2.01      06/06/2016          D.Cheung       CR_018  CR_018 - Add Lookup for SAP Equipment Number
-- V 3.01      21/06/2016          D.Cheung               I-243 - Remove linebreaks from METERLOCFREEDESCRIPTOR processing
-- V 3.02      28/06/2016          D.Cheung       CR_025  Change MANUFACTURER/MANUFCODE to use mapping table LU_METER_MANUFACTURER
-- V 3.03      01/07/2016          K.Burton               I-266 - YEARLYVOLESTIMATE needs to be +ive integer value
-- V 3.04      05/07/2016          K.Burton               SAP Issue SI-019 - corrected calculation of RETURNTOSEWER
-- V 3.05      07/07/2016          D.Cheung       CR_007  SAP Change - Add field for METER_MODEL from Ttarget TVP063EQUIPMENT.CD_MODEL
-- V 3.06      11/07/2016          D.Cheung               I-286 - Can't Join on NO_ACCOUNT to MO_DISHCARGE_POINT due to changes to BT_TE_WORKING
-- V 3.07      13/07/2016          D.Cheung     S_CR_014  SAP Change to add new field UNITOFMEASURE with original target code
-- V 3.08      20/07/2016          D.Cheung               I-311 - Get SewerageChargableMeterSize from PhysicalMeterSize on related water meter
-- V 3.09      10/08/2016          S.Badhan               I-331 - for PRIVATETE meters set SewerageChargableMeterSize to 0 and 
--                                                        Return to Sewer to null as per CSD 0104.
-- V 3.10      23/08/2016          D.Cheung               I-347 - MOSL Test 3 feedback - Not evaluating Numberofdigits correctly (less than length of readings)
-- V 3.11      08/09/2016          D.Cheung               Chenge serialnum logic to just get from NO_IWCS || MER_REF
-- V 3.12      13/09/2016          D.Cheung               I-356 - Add rule to check Sewerage Service Component Exists
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_METER_TE';  -- modify
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  L_PROGRESS                    VARCHAR2(100);
  l_prev_met                    MO_METER.METERREF%TYPE; --modify
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  L_ERR                         MIG_ERRORLOG%ROWTYPE;
  L_MO                          MO_METER%ROWTYPE; --modify
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  L_NO_ROW_INSERT               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  L_REC_WRITTEN                 BOOLEAN;
  l_rec_exc                     BOOLEAN;
  --l_rec_war                     BOOLEAN;
  l_mo_stw_balanced_yn          VARCHAR2(1);
  l_index_count                 NUMBER;
  l_sc_count                    NUMBER;

CURSOR CUR_MET (P_NO_EQUIPMENT_START   MO_METER.METERREF%type,
                 P_NO_EQUIPMENT_END     MO_METER.METERREF%type)
    IS
      SELECT /*+ FULL(MM) PARALLEL(BTW,12) PARALLEL(MD,12) PARALLEL(MM,12) */
      DISTINCT
              BTW.NO_IWCS || BTW.MET_REF AS NO_EQUIPMENT      
              , MD.SPID_PK
              , MD.DPID_PK
              , 'UNKNOWN' AS NM_PREFERRED
--              , TRIM(REPLACE(UPPER(NVL(BTW.SERIAL_NO, BTW.NO_IWCS || BTW.MET_REF)),' ','')) AS NO_UTL_EQUIP   --v3.11
              , TRIM(REPLACE(UPPER(BTW.NO_IWCS || BTW.MET_REF),' ','')) AS NO_UTL_EQUIP     --v3.11
              , CASE BTW.TE_CATEGORY WHEN 'Private Water Meter' THEN 0 ELSE NULL END AS PHYSICALMETERSIZE   --V1.01
              , GREATEST(BTWM.LEN_START, BTWM.LEN_END, 4) AS NUMBEROFDIGITS   --V1.01
              , UPPER(TRIM(BTW.UNIT)) AS CD_UNIT_OF_MEASURE
              , 0 AS COMBIMETERFLAG
              , 0 AS REMOTEREADFLAG
              , NULL AS REMOTEREADTYPE
              , 'B' AS METERREADFREQUENCY
              , NULL AS FREEDESCRIPTOR
              , 0 AS METERNETWORKASSOCIATION
              , CASE BTW.TE_CATEGORY WHEN 'Private TE Meter' THEN 'PRIVATETE' ELSE 'PRIVATEWATER' END AS METERTREATMENT
              , 82644.0 AS GPSX
              , 5186.0 AS GPSY
              , NULL AS OUTREADERID
              , NULL AS METEROUTREADERLOCCODE
              , NULL AS METEROUTREADERGPSX
              , NULL AS METEROUTREADERGPSY
              , NULL AS OUTREADERPROTOCOL
              , NULL AS OUTREADERLOCFREEDES
              , MAX(NVL(MM.PHYSICALMETERSIZE,0)) AS SEWCHARGEABLEMETERSIZE
              , 0 AS WATERCHARGEMETERSIZE
              , DECODE((1-ABS(NVL(BTW.MS,0)))*100, 0, 100,(1-ABS(NVL(BTW.MS,0)))*100) AS RETURNTOSEWER -- V 3.04 
              , NVL(BTW.MDVOL_FOR_TE_METER_PERC,0)*100 AS MDVOL
              , 0 AS DATALOGGERNONWHOLESALER
              , 0 AS DATALOGGERWHOLESALER
              , NULL AS METERADDITIONREASON
              , NULL AS METERREMOVALREASON
              , 0 AS NONMARKETMETERFLAG
              , NULL AS METERLOCATIONDESC
              , NULL AS DS_LOCATION
              , NULL AS TXT_SPECIAL_INSTR
              , MD.STWPROPERTYNUMBER_PK AS NO_PROPERTY
              , BTW.NO_IWCS
              , LSE.SAPEQUIPMENT --v2.01
              , 'UNKNOWN' AS METER_MODEL  --v3.05
      FROM BT_TE_WORKING BTW,
           MO_DISCHARGE_POINT MD,
           (SELECT NO_IWCS, MAX(PERIOD) MAXPERIOD, MAX(LENGTH(ROUND(ABS(START_READ)))) LEN_START, MAX(LENGTH(ROUND(ABS(END_READ)))) LEN_END FROM BT_TE_WORKING GROUP BY NO_IWCS ) BTWM,
           LU_SAP_EQUIPMENT LSE,
           (SELECT SUBSTR(SPID_PK,1,10) CORESPID, PHYSICALMETERSIZE FROM MO_METER WHERE METERTREATMENT = 'POTABLE' ) MM
      WHERE BTW.NO_IWCS = MD.DPID_PK
      AND BTW.TE_CATEGORY IN ('Private TE Meter', 'Private Water Meter')
      AND BTW.NO_IWCS = BTWM.NO_IWCS 
      AND BTW.PERIOD = BTWM.MAXPERIOD
      AND BTW.NO_IWCS || BTW.MET_REF = LSE.STWMETERREF(+)
      AND SUBSTR(MD.SPID_PK,1,10) = MM.CORESPID(+) 
      GROUP BY BTW.NO_IWCS || BTW.MET_REF, MD.SPID_PK, MD.DPID_PK, 'UNKNOWN', TRIM(REPLACE(UPPER(NVL(BTW.SERIAL_NO, BTW.NO_IWCS || BTW.MET_REF)),' ','')), CASE BTW.TE_CATEGORY WHEN 'Private Water Meter' THEN 0 ELSE NULL END, GREATEST(BTWM.LEN_START, BTWM.LEN_END, 4), UPPER(TRIM(BTW.UNIT)), 0, 0, NULL, 'B', NULL, 0, CASE BTW.TE_CATEGORY WHEN 'Private TE Meter' THEN 'PRIVATETE' ELSE 'PRIVATEWATER' END, 82644.0, 5186.0, NULL, NULL, NULL, NULL, NULL, NULL, 0, DECODE((1-ABS(NVL(BTW.MS,0)))*100, 0, 100,(1-ABS(NVL(BTW.MS,0)))*100), NVL(BTW.MDVOL_FOR_TE_METER_PERC,0)*100, 0, 0, NULL, NULL, 0, NULL, NULL, NULL, MD.STWPROPERTYNUMBER_PK, BTW.NO_IWCS, LSE.SAPEQUIPMENT, 'UNKNOWN'
      ORDER BY BTW.NO_IWCS || BTW.MET_REF;

type TAB_METER is table of CUR_MET%ROWTYPE index by PLS_INTEGER;
T_MET TAB_METER;

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
    l_prev_met := 0;
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
            --L_REC_WAR := false;

            L_PROGRESS := 'CHECK IF VALIDATED TARIFF';
            BEGIN
                SELECT MAX(MO_STW_BALANCED_YN)
                INTO l_mo_stw_balanced_yn
                FROM BT_TE_SUMMARY
                WHERE NO_IWCS = T_MET(I).NO_IWCS
                ;
            END;
            IF (l_mo_stw_balanced_yn <> 'Y') THEN
                P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('TE TARIFF NOT VALIDATED - CALCULATION MISMATCH',1,100),  L_ERR.TXT_KEY, SUBSTR(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                L_REC_EXC := TRUE;
            ELSE
                L_PROGRESS := 'PROCESSING TRANSFORMS';
            
                --MEASUREUNITATMETER, MEASUREUNITFREEDESCRIPTOR
                IF T_MET(I).CD_UNIT_OF_MEASURE = 'M3' THEN 
                    L_MO.MEASUREUNITATMETER := 'METRICm3';
                    --L_MO.MEASUREUNITFREEDESCRIPTOR := 'Meter with readings in m3'; 
                    L_MO.MEASUREUNITFREEDESCRIPTOR := NULL;     --V1.01
                ELSE 
                    T_MET(I).CD_UNIT_OF_MEASURE := 'GAL';   --v3.07
                    L_MO.MEASUREUNITATMETER := 'METRICNONm3';
                    L_MO.MEASUREUNITFREEDESCRIPTOR := 'Meter with readings in non m3'; 
                END IF;

                --METERLOCATIONCODE
                L_MO.METERLOCATIONCODE := 'O'; -- BB, Boundary Box

                --METERLOCFREEDESCRIPTOR
                L_MO.METERLOCFREEDESCRIPTOR := SUBSTR('Details of Private Meter was not supply by customer at the point of loading. These details have been supplied to enable continued calulation of the charge elements for the DPID. Default values have been used where no data exists for the meter. When correct details are supplied by the customer these will be updated.',1,255); --v3.01
            
                --YEARLYVOLESTIMATE - CALCULATED FROM LAST 2 PERIODS
                L_PROGRESS := 'GETTING YEARLYVOLESTIMATE';
                BEGIN
                    SELECT NVL(ROUND(SUM(ABS(BTW.TE_VOL)) / NULLIF(COUNT(BTW.TE_VOL),0)),0) as Est_365 -- V 3.03 
                    INTO L_MO.YEARLYVOLESTIMATE
                    FROM BT_TE_WORKING BTW
                    JOIN (SELECT NO_IWCS, MAX(PERIOD) MAXPERIOD FROM BT_TE_WORKING GROUP BY NO_IWCS ) BTWM 
                        ON BTW.NO_IWCS = BTWM.NO_IWCS 
                        AND (BTW.PERIOD = BTWM.MAXPERIOD OR BTW.PERIOD = BTWM.MAXPERIOD-1)
                    WHERE BTW.NO_IWCS || BTW.MET_REF = T_MET(I).NO_EQUIPMENT
                    ;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        L_MO.YEARLYVOLESTIMATE := 0;
                END;
            
--*** v3.02 - ManufacturerCode mapping
                L_PROGRESS := 'GETTING MANUFACTURER (CODE)';
                L_MO.MANUFCODE := T_MET(I).NM_PREFERRED;
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
                --L_MO.MANUFACTURER_PK := REPLACE(UPPER(T_MET(I).NM_PREFERRED),' ','_');
                
--*** v3.12 - check for service component
                IF (TRIM(T_MET(I).SPID_PK) IS NOT NULL AND T_MET(I).METERTREATMENT = 'PRIVATEWATER') THEN
                    BEGIN
                        SELECT COUNT(*)
                        INTO   l_sc_count
                        FROM   MO_SERVICE_COMPONENT
                        WHERE  SPID_PK = t_met(I).SPID_PK
                            AND SERVICECOMPONENTTYPE IN ('AS','US','SW','MS')
                        ;
                    
                        IF (l_sc_count = 0) THEN
                            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO Service Component for PRIVATEWATER Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                            L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                            L_REC_EXC := true;
                        END IF;
                    END;
                ELSIF (TRIM(T_MET(I).SPID_PK) IS NOT NULL) THEN
                    BEGIN
                        SELECT COUNT(*)
                        INTO   l_sc_count
                        FROM   MO_DISCHARGE_POINT
                        WHERE  SPID_PK = t_met(I).SPID_PK
                            AND SERVICECOMPTYPE IN ('TE')
                        ;
                    
                        IF (l_sc_count = 0) THEN
                            P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'X', SUBSTR('NO Discharge Point for PRIVATETE Meter',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
                            L_NO_ROW_EXP := L_NO_ROW_EXP + 1;
                            L_REC_EXC := true;
                        END IF;
                    END;
                END IF;
                
                IF T_MET(I).METERTREATMENT = 'PRIVATETE' THEN 
                   T_MET(I).RETURNTOSEWER := NULL;
                   T_MET(I).SEWCHARGEABLEMETERSIZE := NULL;
                END IF;
                 
            END IF;

            IF L_REC_EXC = TRUE THEN  --using this if statement to pick up data that passes the biz rules into the table otherwise they will be dropped and would have be sent into the exception table
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
                END IF;

                L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
            ELSE
                --IF (L_REC_WAR = true AND l_no_row_war > l_job.WAR_TOLERANCE) THEN
                --    CLOSE cur_met;
                --    L_JOB.IND_STATUS := 'ERR';
                --    P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Warning tolerance level exceeded- Dropping bad data',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
                --    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                --    commit;
                --    return_code := -1;
                --    return;
                --END IF;
                L_REC_WRITTEN := TRUE;
                l_progress := 'loop processing ABOUT TO INSERT INTO TABLE';
                BEGIN
                    INSERT INTO MO_METER
                        (COMBIMETERFLAG,	DATALOGGERNONWHOLESALER,	DATALOGGERWHOLESALER,	FREEDESCRIPTOR,	GPSX,	GPSY,	MANUFACTURER_PK,	MANUFACTURERSERIALNUM_PK,	MDVOL,	MEASUREUNITATMETER,	MEASUREUNITFREEDESCRIPTOR
                        ,	METERADDITIONREASON,	METERLOCATIONCODE,	METERLOCFREEDESCRIPTOR,	METERNETWORKASSOCIATION,	METEROUTREADERGPSX,	METEROUTREADERGPSY,	METEROUTREADERLOCCODE,	METERREADFREQUENCY,	METERREF,	METERREMOVALREASON
                        ,	METERTREATMENT,	NUMBEROFDIGITS,	OUTREADERID,	OUTREADERLOCFREEDES,	OUTREADERPROTOCOL,	PHYSICALMETERSIZE,	REMOTEREADFLAG,	REMOTEREADTYPE,	RETURNTOSEWER,	SEWCHARGEABLEMETERSIZE,	SPID_PK,	WATERCHARGEMETERSIZE,	YEARLYVOLESTIMATE
                        , NONMARKETMETERFLAG, METERLOCATIONDESC, METERLOCSPECIALLOC, METERLOCSPECIALINSTR, MANUFCODE, INSTALLEDPROPERTYNUMBER, DPID_PK
                        , SAPEQUIPMENT  --v2.01
                        , METER_MODEL   --v3.05
                        , UNITOFMEASURE --v3.07
                        )
                    VALUES
                        (T_MET(I).COMBIMETERFLAG, t_met(i).DATALOGGERNONWHOLESALER,t_met(i).DATALOGGERWHOLESALER,t_met(i).FREEDESCRIPTOR,t_met(i).GPSX,t_met(i).GPSY,TRIM(L_MO.MANUFACTURER_PK),TRIM(T_MET(I).NO_UTL_EQUIP),t_met(i).MDVOL, TRIM(L_MO.MEASUREUNITATMETER),TRIM(L_MO.MEASUREUNITFREEDESCRIPTOR)
                        ,t_met(i).METERADDITIONREASON,TRIM(L_MO.METERLOCATIONCODE),TRIM(L_MO.METERLOCFREEDESCRIPTOR),t_met(i).METERNETWORKASSOCIATION, t_met(i).METEROUTREADERGPSX,t_met(i).METEROUTREADERGPSY,t_met(i).METEROUTREADERLOCCODE,t_met(i).METERREADFREQUENCY,T_MET(I).NO_EQUIPMENT,t_met(i).METERREMOVALREASON
                        ,TRIM(T_MET(I).METERTREATMENT),t_met(i).NUMBEROFDIGITS,t_met(i).OUTREADERID,t_met(i).OUTREADERLOCFREEDES,t_met(i).OUTREADERPROTOCOL,t_met(i).PHYSICALMETERSIZE,t_met(i).REMOTEREADFLAG,t_met(i).REMOTEREADTYPE,t_met(i).RETURNTOSEWER,t_met(i).SEWCHARGEABLEMETERSIZE, TRIM(T_MET(I).SPID_PK), t_met(i).WATERCHARGEMETERSIZE, l_mo.YEARLYVOLESTIMATE
                        , T_MET(I).NONMARKETMETERFLAG, t_met(i).METERLOCATIONDESC, T_MET(I).DS_LOCATION, T_MET(I).TXT_SPECIAL_INSTR, L_MO.MANUFCODE, T_MET(I).NO_PROPERTY, t_met(i).DPID_PK
                        , T_MET(I).SAPEQUIPMENT   --v2.01
                        , T_MET(I).METER_MODEL   --v3.05
                        , T_MET(I).CD_UNIT_OF_MEASURE -- v3.07
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
                END IF;
            END IF;  --close of if  L_REC_EXC statement
            l_prev_met := t_met(i).NO_EQUIPMENT;

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

    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP33', 1120, L_NO_ROW_READ,    'Distinct Eligible TE Meters read during Transform');
    P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP33', 1130, L_NO_ROW_DROPPED, 'Eligible TE Meters dropped during Transform');
    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP33', 1140, l_no_row_insert,  'Eligible TE Meters written to MO_METER during Transform');

    --  check counts match

    IF l_no_row_read <> l_no_row_insert + L_NO_ROW_DROPPED THEN
        l_job.IND_STATUS := 'ERR';
        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Reconciliation counts do not match',  l_no_row_read || ',' || l_no_row_insert, substr(l_ERR.TXT_DATA || ',' || l_progress,1,100));
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
        commit;
        return_code := -1;
    ELSE
        l_job.IND_STATUS := 'END';
        P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
    END IF;

    --BEGIN
    --    SELECT COUNT(*) INTO l_index_count FROM user_indexes WHERE index_name = 'INSTALLEDPROPERTYNUMBER_IDX';
    --    IF l_index_count = 0 THEN
    --        EXECUTE IMMEDIATE 'CREATE INDEX INSTALLEDPROPERTYNUMBER_IDX ON MO_METER (INSTALLEDPROPERTYNUMBER)';
    --    END IF;
    --END;

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
END P_MOU_TRAN_METER_TE;
/
/
show errors;
exit;