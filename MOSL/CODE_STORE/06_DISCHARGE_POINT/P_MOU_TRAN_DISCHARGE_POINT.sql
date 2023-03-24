CREATE OR REPLACE PROCEDURE P_MOU_TRAN_DISCHARGE_POINT(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                       no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                       return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Service Component MO Extract 
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_DISCHARGE_POINT.sql
--
-- Subversion $Revision: 6324 $
--
-- CREATED        : 05/04/2016
--
-- DESCRIPTION    : Procedure to create the Discharge Point MO Extract 
--                  Will read from key gen and target tables, apply any transformationn
--                  rules and write to normalised tables.
--
-- NOTES  :
-- This package must be run each time the transform batch is run. 
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 1.25      21/11/2016  D.Cheung   Add union onto BT_MISS_AG_SC to pick up dropped services on aggregates
-- V 1.24      18/11/2016  K.Burton   Back out below - need to accommodate little m's
-- V 1.23      18/11/2016  S.Badhan   Write exception if no spid found for corespid.
-- V 1.22      17/11/2016  K.Burton   UPPER TARIFFCODE_PK - fix for settlements issue
-- V 1.21      10/11/2016  S.Badhan   I-375. Write warning if DA value is zero if required for tariff.
-- V 1.20      09/11/2016  S.Badhan   I-374. Use absolute value of fixed allowance.
-- V 1.19      01/11/2016  S.Badhan   If chemical oxygen demand and suspended solids values not found
--                                    check in previous period. Allow value of 0 previously disallowed in MOSL.
-- V 1.18      26/10/2016  S.Badhan   I-369. Write warning and set PA percentage to 100 if > 100.
-- V 1.17      14/09/2016  S.Badhan   Report error if no Target NO_IWCS found.
-- V 1.16      26/07/2016  S.Badhan   I-316. Report error if SEWERAGEVOLUMEADJMENTHOD is 'DA and there is no DOMMESTICALLOWANCE.
-- V 1.14      21/07/2016  S.Badhan   SAP CR-20. Set up data in new column DPID_TYPE.
-- V 1.13      15/07/2016  S.Badhan   I-302. IF SEWERAGEVOLUMEADJMENTHOD IS NONE OR SUBTRACT THEN DOMMESTICALLOWANCE must be NULL 
-- V 1.12      13/07/2016  S.Badhan   SAP CR-16. Populate new field VOLUME_LIMIT.
-- V 1.11      05/07/2016  S.Badhan   I-272 - use chemical oxygen demand and suspended solids
--                                    average values if agreed levels not available - 'CODST' and 'SSST'.
-- V 1.10      04/07/2016  S.Badhan   I-271 - Set chemical oxygen demand and suspended solids
--                                    from algorithm 'CODST' and 'SSST'.
-- V 1.09      01/07/2016  S.Badhan   I-263 - Set SEWERAGEVOLUMEADJMENTHOD from BT_TE_SUMMARY
-- V 1.08      30/06/2016  S.Badhan   CR-027 - Update inline with MOSL guidelines.
-- V 1.07      29/06/2016  S.Badhan   I-263 - Alter the SEWERAGEVOLUMEADJMENTHOD if DOMMESTICALLOWANCE is null or zero.
-- V 1.06      23/06/2016  D.Cheung   I-254 - 1.Check for NULL values on BT_SPR_TARIFF_ALGITEM
--                                            2.Remove spaces on DPID Special Agreement Reference
--                                            3.Default ChargableDailyAllowance to 0 (ZERO)
-- V 1.05      21/06/2016  S.Badhan   I-252. Look up MO_TARIFF to retrieve servicecomponenttype
-- V 1.04      18/06/2016  S.Badhan   I-247. Remove join to MO_TARIFF to report on missing tariffs.
-- V 1.03      17/06/2016  S.Badhan   I-246. Report error if no consent number present.
-- V 1.02      16/06/2016  S.Badhan   CR_019. Set DPID to NO_IWCS.
-- V 1.01      14/06/2016  D.Cheung   I-234, CR_50 - set Effective Dates to Supply Point Effective From Date
-- V 0.10      07/06/2016  L.Smith    Multiply TE percentages by 100 for MOSL
-- V 0.09      27/05/2016  S.Badhan   Add check on IWCS on BT_TE_SUMMARY, report error
--                                    if calculations not balanced.
-- V 0.08      25/05/2016  S.Badhan   Populate new column STWPROPERTYNUMBER_PK.
-- V 0.07      25/05/2016  S.Badhan   Default percentage allowance, domestic allowance and
--                                    fixed allowance to 0 if nulls.
-- V 0.06      24/05/2016  S.Badhan   Populate new columns for TE.
-- V 0.05      17/05/2016  S.Badhan   Use table MO_TARIFF instead of LU_TARIFF.
-- V 0.04      27/04/2016  S.Badhan   I-179. Band check amended to also check on BT_SP_TARIFF_ALGITEM.
-- V 0.03      25/04/2016  L.Smith    Set columns DPIDSPECIALAGREEMENTFACTOR, DPIDSPECIALAGREEMENTREFERENCE
-- V 0.02      22/04/2016  L.Smith    Set TARRIFBAND to NULL not 0. (I-171)
-- V 0.01      05/04/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_DISCHARGE_POINT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_srt_order                   VARCHAR2(1);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE; 
  l_t314                        CIS.TVP314TARACCLSAPPL%ROWTYPE; 
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_DISCHARGE_POINT%ROWTYPE; 
  l_age                         LU_TARIFF_SPECIAL_AGREEMENTS%ROWTYPE; 
  l_spid                        LU_SPID_RANGE%ROWTYPE;   
  l_mpw                         BT_SC_MPW%ROWTYPE; 
  l_spt                         BT_SP_TARIFF_SPLIT%ROWTYPE;    
  l_uw                          BT_SC_UW%ROWTYPE;   
  l_ext                         BT_SPR_TARIFF_EXTREF%ROWTYPE;   
  l_ext2                        BT_SPR_TARIFF_EXTREF%ROWTYPE;   
  l_alg                         BT_SPR_TARIFF_ALGITEM%ROWTYPE;   
  l_spalg                       BT_SP_TARIFF_ALG%ROWTYPE;   
  l_ref                         BT_SP_TARIFF_REFTAB%ROWTYPE;     
  l_t358                        CIS.TVP358REFTAB%ROWTYPE;  
  l_t048                        CIS.TVP048SAMPLEPOLL%ROWTYPE;  
  l_tes                         BT_TE_SUMMARY%ROWTYPE;
  l_mot                         MO_TARIFF%ROWTYPE;  
  l_mtv                         MO_TARIFF_VERSION%ROWTYPE;  
  l_band                        MO_TE_BAND_CHARGE%ROWTYPE;  
  l_tet                         MO_TARIFF_TYPE_TE%ROWTYPE;
  l_robt                        MO_TE_BLOCK_ROBT%ROWTYPE;
  l_bobt                        MO_TE_BLOCK_BOBT%ROWTYPE;
  l_vol                         LU_DISCHARGE_VOL_LIMITS%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_effectivefromdate           DATE;

nLCount                         PLS_INTEGER := 0;
nLCommitFrequency               PLS_INTEGER := 1000;

      
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
    IS 
SELECT DISTINCT * FROM (
    SELECT t054.NO_PROPERTY,
            t054.NO_ACCOUNT,
            t054.NO_SERV_PROV,
            t054.NO_COMBINE_054,
            t054.CD_SERVICE_PROV,
            t054.CORESPID,
            t054.DT_END AS T054_DT_END,
            tcat.SUPPLY_POINT_CODE,
            tcat.SERVICECOMPONENTTYPE,
            trf.CD_TARIFF, 
            trf.NO_TARIFF_GROUP, 
            trf.NO_TARIFF_SET, 
            trf.DT_START, 
            trf.DT_END,
            t056.DT_STATUS,
            t056.ST_SERV_PROV,    
            t056.DT_START AS T056_DT_START,
            t054.NO_PROPERTY AS NO_PROPERTY_MASTER
     FROM   BT_TVP054           t054,
            LU_SERVICE_CATEGORY tcat,
            BT_SPR_TARIFF       trf,
            CIS.TVP056SERVPROV  t056
     WHERE  t054.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end
     AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
     AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV                          
     AND    tcat.SERVICECOMPONENTTYPE  IN ('TE')
     AND    tcat.TARGET_SERV_PROV_CODE  IN ('TW')
     AND    t056.CD_COMPANY_SYSTEM = t054.CD_COMPANY_SYSTEM
     AND    t056.NO_PROPERTY       = t054.NO_PROPERTY
     AND    t056.NO_SERV_PROV      = t054.NO_SERV_PROV    
     UNION
     SELECT BT.NO_PROPERTY,
            BT.NO_ACCOUNT,
            BT.NO_SERV_PROV,
            BT.NO_COMBINE_054,
            BT.CD_SERVICE_PROV,
            t054.CORESPID,
            BT.DT_END AS T054_DT_END,
            tcat.SUPPLY_POINT_CODE,
            tcat.SERVICECOMPONENTTYPE,
            trf.CD_TARIFF, 
            trf.NO_TARIFF_GROUP, 
            trf.NO_TARIFF_SET, 
            trf.DT_START, 
            trf.DT_END,
            t056.DT_STATUS,
            t056.ST_SERV_PROV,    
            t056.DT_START AS T056_DT_START,
            BT.NO_PROPERTY_MASTER
     FROM   BT_MISS_AG_SC       BT,
            BT_TVP054           t054,
            LU_SERVICE_CATEGORY tcat,
            BT_SPR_TARIFF       trf,
            CIS.TVP056SERVPROV  t056
     WHERE  BT.NO_PROPERTY_MASTER BETWEEN p_no_property_start AND p_no_property_end
--     WHERE  BT.NO_PROPERTY_MASTER BETWEEN 1 AND 999999999
     AND    BT.NO_PROPERTY_MASTER = T054.NO_PROPERTY
     AND    trf.NO_COMBINE_054         = BT.NO_COMBINE_054
     AND    tcat.TARGET_SERV_PROV_CODE = BT.CD_SERVICE_PROV                          
     AND    tcat.SERVICECOMPONENTTYPE  IN ('TE')
     AND    tcat.TARGET_SERV_PROV_CODE  IN ('TW')
     AND    t056.CD_COMPANY_SYSTEM = BT.CD_COMPANY_SYSTEM
     AND    t056.NO_PROPERTY       = BT.NO_PROPERTY
     AND    t056.NO_SERV_PROV      = BT.NO_SERV_PROV   
)
ORDER BY NO_PROPERTY, NO_SERV_PROV, NO_TARIFF_GROUP, NO_TARIFF_SET;
--     ORDER BY t054.NO_PROPERTY, t054.NO_SERV_PROV, trf.NO_TARIFF_GROUP, trf.NO_TARIFF_SET;
                          
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
   l_tet := null;
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
 
    FOR i IN 1..t_prop.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop(i).NO_PROPERTY || ',' || t_prop(i).NO_SERV_PROV || ',' || t_prop(i).SERVICECOMPONENTTYPE || ',' || t_prop(i).CD_TARIFF;
      l_mo := NULL;
      l_rec_written := TRUE;

      -- keep count of distinct property
      l_no_row_read := l_no_row_read + 1;

--      l_mo.STWPROPERTYNUMBER_PK := t_prop(i).NO_PROPERTY;
      l_mo.STWPROPERTYNUMBER_PK := t_prop(i).NO_PROPERTY_MASTER;
      l_mo.NO_ACCOUNT := t_prop(i).NO_ACCOUNT;

      -- Get Discharge Point id (NO_IWCS)

      l_progress := 'SELECT BT_SPR_TARIFF_EXTREF - 1';   
      BEGIN 
        SELECT CD_EXT_REF
        INTO   l_ext.CD_EXT_REF
        FROM   BT_SPR_TARIFF_EXTREF
        WHERE  NO_PROPERTY   = t_prop(i).NO_PROPERTY
        AND    NO_SERV_PROV  = t_prop(i).NO_SERV_PROV
        AND    TP_ENTITY_332 = 'S' 
        AND    NO_EXT_REFERENCE IN (4);
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No Target NO_IWCS found',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
           l_ext.CD_EXT_REF := null;
      WHEN TOO_MANY_ROWS THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;

      l_mo.DPID_PK := l_ext.CD_EXT_REF;   

      IF REGEXP_LIKE(l_ext.CD_EXT_REF, '[[:digit:]]') THEN
         l_mo.NO_IWCS := TO_NUMBER(l_ext.CD_EXT_REF);
      ELSE
         l_mo.NO_IWCS := null;
      END IF;

      -- Get Sample point
      
      l_progress := 'SELECT BT_SPR_TARIFF_EXTREF - 2';   
      BEGIN         
        SELECT CD_EXT_REF
        INTO   l_ext2.CD_EXT_REF
        FROM   BT_SPR_TARIFF_EXTREF
        WHERE  NO_PROPERTY   = t_prop(i).NO_PROPERTY
        AND    NO_SERV_PROV  = t_prop(i).NO_SERV_PROV
        AND    TP_ENTITY_332 = 'S' 
        AND    NO_EXT_REFERENCE IN (1);
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No Target NO_SAMPLE_POINT found',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
           l_ext2.CD_EXT_REF := null;
      WHEN TOO_MANY_ROWS THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;

      IF REGEXP_LIKE(l_ext2.CD_EXT_REF, '[[:digit:]]') THEN
         l_mo.NO_SAMPLE_POINT := TO_NUMBER(l_ext2.CD_EXT_REF);
      ELSE
         l_mo.NO_SAMPLE_POINT := NULL;
      END IF;

      -- Get Consent Number
      
      l_progress := 'SELECT BT_SPR_TARIFF_EXTREF - 3';   
      l_ext.CD_EXT_REF := null;
      BEGIN 
        SELECT CD_EXT_REF
        INTO   l_ext.CD_EXT_REF
        FROM   BT_SPR_TARIFF_EXTREF
        WHERE  NO_PROPERTY   = t_prop(i).NO_PROPERTY
        AND    NO_SERV_PROV  = t_prop(i).NO_SERV_PROV
        AND    TP_ENTITY_332 = 'S' 
        AND    NO_EXT_REFERENCE IN (2);
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No Consent number present',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
      WHEN TOO_MANY_ROWS THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;

      l_mo.CONSENT_NO := l_ext.CD_EXT_REF;


      -- Get Supply Point ID

      BEGIN
        l_progress := 'SELECT LU_SPID_RANGE';   
        SELECT SPID_PK
        INTO   l_spid.SPID_PK
        FROM   LU_SPID_RANGE
        WHERE  CORESPID_PK     = t_prop(i).CORESPID
        AND    SERVICECATEGORY = t_prop(i).SUPPLY_POINT_CODE;	
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No SPID found for corespid',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
           l_spid.SPID_PK := null;
      END;
      
      l_mo.SPID_PK := l_spid.SPID_PK;
      
      -- Set Effective Dates
      IF l_spid.SPID_PK  IS NOT NULL THEN
          SELECT MAX(NVL(SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')))
          INTO  l_effectivefromdate
          FROM MO_SUPPLY_POINT
          WHERE SPID_PK = l_mo.SPID_PK;
      ELSE
         l_effectivefromdate := NULL;
      END IF;
      
      l_mo.SCEFFECTIVEFROMDATE := l_effectivefromdate;
      l_mo.DPEFFECTFROMDATE := l_effectivefromdate;
      l_mo.DPEFFECTTODATE := null;      

      l_mo.EFFECTFROMDATE := l_effectivefromdate;      
      l_mo.EFFECTTODATE := NULL;  
                  
      -- Status of service component
       
      IF t_prop(i).ST_SERV_PROV IN ('A','G') THEN
         l_mo.DISCHARGEPOINTERASEFLAG := 0;
      ELSE
         l_mo.DISCHARGEPOINTERASEFLAG := 1;         
      END IF;

      -- Set Wholesalerid
 
      l_mo.WHOLESALERID := 'SEVERN-W';      -- check cross border ?? tariff link to wholesaler

      -- Retrieve tariff details

      l_mo.TARRIFCODE := t_prop(i).CD_TARIFF;

      -- Get translation tariff if tariffs have different charges based on the Zone or
      -- PCODE Algorithm item

      l_progress := 'SELECT BT_SP_TARIFF_SPLIT ';
      BEGIN
        SELECT TRIM(CD_SPLIT_TARIFF)
        INTO   l_spt.CD_SPLIT_TARIFF
        FROM   BT_SP_TARIFF_SPLIT     spt,
               BT_SPR_TARIFF_ALGITEM  alg
        WHERE  alg.NO_COMBINE_054   = t_prop(i).NO_COMBINE_054
        AND    alg.CD_TARIFF        = spt.CD_TARIFF
        AND    alg.CD_BILL_ALG_ITEM = spt.CD_BILL_ALG_ITEM
        AND    alg.CD_REF_TAB       = spt.CD_REF_TAB
        AND    ROWNUM = 1;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_spt.CD_SPLIT_TARIFF := null;
      END;

      IF l_spt.CD_SPLIT_TARIFF IS NOT NULL THEN
         l_mo.TARRIFCODE := l_spt.CD_SPLIT_TARIFF;
      END IF;

      
      l_progress := 'SELECT MO_TARIFF_VERSION/MO_TARIFF_TYPE_TE';
      BEGIN      
         SELECT mtv.TARIFF_VERSION_PK,
                tet.TARIFF_TYPE_PK,
                tet.TECHARGECOMPOS,            -- Chemical Oxygen Demand D7566
                tet.TECHARGECOMPSS,            -- suspended solids D7567 
                tet.TECHARGECOMPAS,            -- Base value of Ammoniacal Nitrogen D7568
                tet.TECHARGECOMPSO,            -- Sludge Treatment D7564
                tet.TECHARGECOMPAA	           -- Ammonia capacity charging component D7558
         INTO   l_mtv.TARIFF_VERSION_PK,
                l_tet.TARIFF_TYPE_PK,
                l_tet.TECHARGECOMPOS,
                l_tet.TECHARGECOMPSS,
                l_tet.TECHARGECOMPAS,
                l_tet.TECHARGECOMPSO,
                l_tet.TECHARGECOMPAA
         FROM   MO_TARIFF_VERSION   mtv,
                MO_TARIFF_TYPE_TE   tet
         WHERE  mtv.TARIFFCODE_PK     = l_mo.TARRIFCODE
         AND    mtv.TARIFFVERSION     = (SELECT MAX(TARIFFVERSION) 
                                         FROM   MO_TARIFF_VERSION
                                         WHERE  TARIFFCODE_PK = mtv.TARIFFCODE_PK)
         AND    tet.TARIFF_VERSION_PK = mtv.TARIFF_VERSION_PK;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_mtv.TARIFF_VERSION_PK := 0;
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid Tariff',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;

      -- Update Trade Effluent Availability 
  
      l_mo.CHEMICALOXYGENDEMAND := NULL;
      l_mo.SUSPENDEDSOLIDSLOAD := NULL; 
      l_mo.AMMONIANITROCAL := null;
      l_mo.CHARGEABLEDAILYVOL := 0;  
      
      -- Update Seasonal Factor

      l_mo.SEASONALFACTOR := NULL;
      
      -- Check Percentage Allowance, Domestic Allowance and Fixed Allowance on TE database first

      l_progress := 'SELECT BT_TE_SUMMARY ';   
      BEGIN
         SELECT NVL(ABS(FA_VOL),0),   
                NVL(DA_VOL,0),
                NVL(PA_PERC,0),                                  
                MO_STW_BALANCED_YN,
                SEWERAGEVOLUMEADJMENTHOD
         INTO   l_tes.FA_VOL,
                l_tes.DA_VOL,
                l_tes.PA_PERC ,
                l_tes.MO_STW_BALANCED_YN,
                l_tes.SEWERAGEVOLUMEADJMENTHOD
         FROM   BT_TE_SUMMARY
         WHERE  NO_IWCS       = l_mo.NO_IWCS;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_tes.FA_VOL := 0;
           l_tes.DA_VOL := null;
           l_tes.PA_PERC := 0;
           l_tes.MO_STW_BALANCED_YN := 'Y';
           l_tes.SEWERAGEVOLUMEADJMENTHOD := null;
      END;  

      IF SQL%ROWCOUNT > 0 THEN 
         IF l_tes.PA_PERC > 99  THEN 
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'PA_PERC VALUE too large, truncated to 100',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_war := l_no_row_war + 1;
            l_tes.PA_PERC := 100;
         ELSE
            l_tes.PA_PERC := l_tes.PA_PERC * 100;
         END IF;
      END IF;
    
      -- report error if not balanced
      -- if no data found on TE d/b check on Target
      
      IF l_tes.MO_STW_BALANCED_YN = 'Y' THEN
        IF nvl(l_tes.DA_VOL,0) = 0 THEN
           l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - AUFV ';   
           BEGIN
              SELECT NVL(NO_VALUE,0) NO_VALUE       --v1.05
              INTO   l_alg.NO_VALUE
              FROM   BT_SPR_TARIFF_ALGITEM
              WHERE  NO_PROPERTY   = t_prop(i).NO_PROPERTY
              AND    NO_SERV_PROV  = t_prop(i).NO_SERV_PROV
              AND    CD_BILL_ALG_ITEM = 'AUFV';
           EXCEPTION
           WHEN NO_DATA_FOUND THEN
                l_alg.NO_VALUE := null;
           END;
           l_tes.DA_VOL := l_alg.NO_VALUE;
        END IF;
        l_mo.DOMMESTICALLOWANCE := l_tes.DA_VOL;
  
        IF l_tes.FA_VOL = 0 THEN
           IF t_prop(i).T054_DT_END IS NULL THEN
              l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - FSR';  
              BEGIN
               SELECT NO_VALUE
               INTO   l_alg.NO_VALUE
               FROM   BT_SPR_TARIFF_ALGITEM
               WHERE  NO_PROPERTY   = t_prop(i).NO_PROPERTY
               AND    NO_SERV_PROV  = t_prop(i).NO_SERV_PROV
               AND    CD_BILL_ALG_ITEM IN ( 'TW','FSR')
               AND    DT_END           IS NULL;
              EXCEPTION
              WHEN NO_DATA_FOUND THEN
                  l_alg.NO_VALUE := 0;
              END;
              l_tes.FA_VOL := l_alg.NO_VALUE;
           END IF;  
        END IF; 

      ELSE
         IF l_rec_written = TRUE THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid calculation on IWCS',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
            l_no_row_exp := l_no_row_exp + 1;
         END IF;
         l_rec_written := FALSE;
      END IF;     

      
      l_mo.FIXEDALLOWANCE := l_tes.FA_VOL;

      l_mo.PERCENTAGEALLOWANCE := l_tes.PA_PERC;
      
      l_mo.SEASONALFACTOR := NULL;

      -- set Sewerage volumen adjustment method

      IF l_tes.SEWERAGEVOLUMEADJMENTHOD IS NOT NULL THEN
         l_mo.SEWERAGEVOLUMEADJMENTHOD := l_tes.SEWERAGEVOLUMEADJMENTHOD;

         IF (   l_mo.SEWERAGEVOLUMEADJMENTHOD = 'NONE'
             OR l_mo.SEWERAGEVOLUMEADJMENTHOD = 'SUBTRACT')
         THEN   
            l_mo.DOMMESTICALLOWANCE := null;
         ELSE
             IF nvl(l_mo.DOMMESTICALLOWANCE,0) = 0 THEN      
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'DOMMESTICALLOWANCE value is zero',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
                l_no_row_war := l_no_row_war + 1;
                l_mo.DOMMESTICALLOWANCE := 0;
             END IF;
         END IF;
         
      ELSE 
        IF nvl(l_mo.DOMMESTICALLOWANCE,0) = 0 THEN      
           l_mo.SEWERAGEVOLUMEADJMENTHOD := 'NONE';
           l_mo.DOMMESTICALLOWANCE := NULL;
        ELSE
           l_mo.SEWERAGEVOLUMEADJMENTHOD := 'DA';
        END IF;
      END IF;

      -- Set Reception Indicator
      
      l_progress := 'SELECT MO_TE_BLOCK_ROBT';  
      l_mo.RECEPTIONTREATMENTINDICATOR := 1;
      BEGIN
        SELECT CHARGE
        INTO   l_robt.CHARGE
        FROM   MO_TE_BLOCK_ROBT
        WHERE  TARIFF_TYPE_PK = l_tet.TARIFF_TYPE_PK
        AND    ROWNUM = 1;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_mo.RECEPTIONTREATMENTINDICATOR := 0;
      END;

--      -- Form target
--      IF t_prop(i).T054_DT_END IS NULL THEN
--         l_progress := 'SELECT BT_SP_TARIFF_ALG';  
--         l_mo.RECEPTIONTREATMENTINDICATOR := 1;
--         BEGIN
--           SELECT CD_ALGORITHM
--           INTO   l_spalg.CD_ALGORITHM
--           FROM   BT_SP_TARIFF_ALG
--           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
--           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
--           AND    CD_ALGORITHM IN ('AUCCA', 'CCLUA', 'TWCCA');
--         EXCEPTION
--         WHEN NO_DATA_FOUND THEN
--              l_mo.RECEPTIONTREATMENTINDICATOR := 0;
--         END;
--      END IF;


      -- Set Primary Treatment Indicator

      l_progress := 'SELECT MO_TE_BLOCK_BOBT';  
      l_mo.PRIMARYTREATMENTINDICATOR := 1;
      l_bobt.CHARGE := 0;
      BEGIN
        SELECT CHARGE
        INTO   l_bobt.CHARGE
        FROM   MO_TE_BLOCK_BOBT
        WHERE  TARIFF_TYPE_PK = l_tet.TARIFF_TYPE_PK
        AND    ROWNUM = 1;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_mo.PRIMARYTREATMENTINDICATOR := 0;
      END;

--      -- from Target
--      IF t_prop(i).T054_DT_END IS NULL THEN
--         l_progress := 'SELECT BT_SP_TARIFF_REFTAB';   
--
--         l_mo.PRIMARYTREATMENTINDICATOR := 1;
--         BEGIN
--           SELECT TP_EQUIPMENT
--           INTO   l_ref.TP_EQUIPMENT
--           FROM   BT_SP_TARIFF_REFTAB
--           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
--           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
--           AND    TP_EQUIPMENT  IS NOT NULL;
--         EXCEPTION
--         WHEN NO_DATA_FOUND THEN
--              l_mo.PRIMARYTREATMENTINDICATOR := 0;
--         END;
--      END IF;
--

      -- Set Marine Indicator
      
      l_mo.MARINETREATMENTINDICATOR := 0;

      -- Set Biological Treatment Indicator
      
      IF nvl(l_bobt.CHARGE,0) > 0 THEN
         l_mo.BIOLOGICALTREATMENTINDICATOR := 1;      
      ELSE
         l_mo.BIOLOGICALTREATMENTINDICATOR := 0;
      END IF;

--    
--      --FROM TARGET
--   --   l_mo.BIOLOGICALTREATMENTINDICATOR := 0;
--      IF t_prop(i).T054_DT_END IS NULL THEN  
--         l_progress := 'SELECT BT_SP_TARIFF_ALG - AUBTA';   
--
--         l_mo.BIOLOGICALTREATMENTINDICATOR := 1;
--         BEGIN
--           SELECT CD_ALGORITHM
--           INTO   l_spalg.CD_ALGORITHM
--           FROM   BT_SP_TARIFF_ALG
--           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
--           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
--           AND    CD_ALGORITHM IN ('AUBTA');
--         EXCEPTION
--         WHEN NO_DATA_FOUND THEN
--              l_mo.BIOLOGICALTREATMENTINDICATOR := 0;
--         END;
--      END IF;


      -- Set Sludge Treatment Indicator
      
      IF nvl(l_tet.TECHARGECOMPSO,0) > 0 THEN
         l_mo.SLUDGETREATMENTINDICATOR := 1;      
      ELSE
         l_mo.SLUDGETREATMENTINDICATOR := 0;
      END IF;


--      --from target  (SAME MATCHES)
--      l_mo.SLUDGETREATMENTINDICATOR := 0;
--      IF t_prop(i).T054_DT_END IS NULL THEN  
--         l_mo.SLUDGETREATMENTINDICATOR := 1;
--         l_progress := 'SELECT BT_SP_TARIFF_ALG - AUSTA';   
--         BEGIN
--           SELECT CD_ALGORITHM
--           INTO   l_spalg.CD_ALGORITHM
--           FROM   BT_SP_TARIFF_ALG
--           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
--           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
--           AND    CD_ALGORITHM IN ('AUSTA', 'TWSTA');
--         EXCEPTION
--         WHEN NO_DATA_FOUND THEN
--              l_mo.SLUDGETREATMENTINDICATOR := 0;
--         END;
--      END IF;

      l_mo.TEFXTREATMENTINDICATOR := 0;
      l_mo.TEFYTREATMENTINDICATOR := 0;
      l_mo.TEFZTREATMENTINDICATOR := 0;
      
      -- Set Band
            
      l_progress := 'SELECT MO_TE_BAND_CHARGE';         
      BEGIN 
        SELECT BAND
        INTO   l_band.BAND
        FROM   MO_TE_BAND_CHARGE
        WHERE  TARIFF_TYPE_PK = l_tet.TARIFF_TYPE_PK;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_band.BAND := NULL;
      END;
      l_mo.TARRIFBAND := l_band.BAND;

    
--      -- FROM TARGET
--      IF t_prop(i).T054_DT_END IS NULL THEN
--         l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - PCODE';   
--         l_mo.TARRIFBAND := 1;
--         BEGIN
--            SELECT DS_REF_TAB
--            INTO   l_t358.DS_REF_TAB
--            FROM   BT_SPR_TARIFF_ALGITEM alg,
--                   BT_SP_TARIFF_ALGITEM  salg,
--                   CIS.TVP358REFTAB      t358
--            WHERE  alg.NO_PROPERTY       = t_prop(i).NO_PROPERTY
--            AND    alg.NO_SERV_PROV      = t_prop(i).NO_SERV_PROV
--            AND    alg.CD_BILL_ALG_ITEM  = 'PCODE'
--            AND    salg.CD_TARIFF        = alg.CD_TARIFF
--            AND    salg.CD_BILL_ALG_ITEM = alg.CD_BILL_ALG_ITEM            
--            AND    t358.TP_REF_TAB       = alg.TP_REF_TAB
--            AND    t358.CD_REF_TAB       = alg.CD_REF_TAB
--            AND    t358.CD_COMPANY_SYSTEM = 'STW1';
--         EXCEPTION
--         WHEN NO_DATA_FOUND THEN
--              l_mo.TARRIFBAND := NULL;                                                        
--         WHEN TOO_MANY_ROWS THEN
--              l_rec_written := FALSE;
--              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
--              l_no_row_exp := l_no_row_exp + 1;
--         END; 
--      END IF;

     
      --  Set Chemical Oxygen Demand 
      
      l_mo.TREFODCHEMOXYGENDEMAND := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN
      
         l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - CODST';   
         BEGIN
            SELECT CD_BILL_ALG_ITEM,
                   NO_VALUE
            INTO   l_alg.CD_BILL_ALG_ITEM,
                   l_alg.NO_VALUE
            FROM   BT_SPR_TARIFF_ALGITEM
            WHERE  NO_PROPERTY      = t_prop(i).NO_PROPERTY
            AND    NO_SERV_PROV     = t_prop(i).NO_SERV_PROV       
            AND    CD_BILL_ALG_ITEM = 'CODST'
            AND    DT_END           IS NULL;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_alg.CD_BILL_ALG_ITEM := NULL;
         END;
         
         -- Use agreed value if availiable, else use average 
          
         IF l_alg.CD_BILL_ALG_ITEM IS NOT NULL THEN
            l_mo.TREFODCHEMOXYGENDEMAND := l_alg.NO_VALUE;
         ELSE
            l_progress := 'SELECT TVP035SAMPLEDATA/TVP048SAMPLEPOLL';   
            BEGIN          
              SELECT NVL(AVG(t048.AM_POLLUTANT_CON),0)
              INTO   l_t048.AM_POLLUTANT_CON
              FROM   CIS.TVP035SAMPLEDATA t035,
                     CIS.TVP048SAMPLEPOLL t048
              WHERE  t035.CD_COMPANY_SYSTEM = 'STW1'
              AND    t035.NO_PROPERTY       = t_prop(i).NO_PROPERTY
              AND    t035.NO_SERV_PROV      = t_prop(i).NO_SERV_PROV
              AND    t035.DT_CREATED BETWEEN CURRENT_DATE - 180 + 1
                                        AND CURRENT_DATE
              AND    t048.CD_COMPANY_SYSTEM = t035.CD_COMPANY_SYSTEM
              AND    t048.NO_PROPERTY       = t035.NO_PROPERTY
              AND    t048.NO_SERV_PROV      = t035.NO_SERV_PROV
              AND    t048.NO_BOTTLE         = t035.NO_BOTTLE
              AND    t048.DT_CREATED        = t035.DT_CREATED
              AND    t048.FG_BILLABLE       = 'Y'
              AND    t048.TP_POLLUTANT_389  = '0V0';
            EXCEPTION
            WHEN NO_DATA_FOUND THEN 
                l_t048.AM_POLLUTANT_CON := 0;
            END;
            
            l_mo.TREFODCHEMOXYGENDEMAND := l_t048.AM_POLLUTANT_CON;            
         END IF;
        
      END IF;
      
      -- From Tariff Charges
      
      IF l_tet.TECHARGECOMPOS > 0 THEN 
         IF NVL(l_mo.TREFODCHEMOXYGENDEMAND,0) = 0 THEN
            
            -- get last period data
            l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM 2 - CODST';               
            BEGIN
               SELECT NO_VALUE
               INTO   l_alg.NO_VALUE
               FROM   BT_SPR_TARIFF_ALGITEM
               WHERE  NO_PROPERTY      = t_prop(i).NO_PROPERTY
               AND    NO_SERV_PROV     = t_prop(i).NO_SERV_PROV       
               AND    CD_BILL_ALG_ITEM = 'CODST'
               AND    (   DT_START >= to_date('01/10/2015','dd/mm/yyyy') and DT_END <= to_date('01/04/2016','dd/mm/yyyy')
                       or DT_START >= to_date('01/04/2016','dd/mm/yyyy') and DT_END <= to_date('01/10/2016','dd/mm/yyyy') )
               order by DT_START desc;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 l_alg.NO_VALUE := 0;
            END;
         
            l_mo.TREFODCHEMOXYGENDEMAND := nvl(l_alg.NO_VALUE,0);
            
            IF NVL(l_mo.TREFODCHEMOXYGENDEMAND,0) = 0 THEN 
               P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'No agreed limit or average strength for Oxygen demand, defaulted to 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
               l_no_row_war := l_no_row_war + 1;
            END IF;
                
         END IF;
      ELSE         
         l_mo.TREFODCHEMOXYGENDEMAND := 0;
      END IF;
      
      -- Set suspended solids
      --Use SSSD (STW Standard  Suspended Solids Strength), if not available, SSST (Agreed Suspended Solids Strength). When neither available, get the SSAP (Suspended Solids Sample Average Period, default 90) and then calculate average Suspended Solids Strength derived from 'Calculate Average Suspended Solids Strength' SQL in 'Extract Criteria'

      l_mo.TREFODCHEMSUSPSOLDEMAND := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN        
         l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - SSST';  

         BEGIN
            SELECT CD_BILL_ALG_ITEM,
                   NO_VALUE
            INTO   l_alg.CD_BILL_ALG_ITEM,
                   l_alg.NO_VALUE
            FROM   BT_SPR_TARIFF_ALGITEM
            WHERE  NO_PROPERTY      = t_prop(i).NO_PROPERTY
            AND    NO_SERV_PROV     = t_prop(i).NO_SERV_PROV       
            AND    CD_BILL_ALG_ITEM = 'SSST'
            AND    DT_END           IS NULL;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_alg.CD_BILL_ALG_ITEM := NULL;
         END;

         -- Use agreed value if availiable, else use average

         IF l_alg.CD_BILL_ALG_ITEM IS NOT NULL THEN
            l_mo.TREFODCHEMSUSPSOLDEMAND := l_alg.NO_VALUE;
         ELSE
            l_progress := 'SELECT TVP035SAMPLEDATA/TVP048SAMPLEPOLL';  
            BEGIN 
              SELECT NVL(AVG(t048.AM_POLLUTANT_CON),0)
              INTO   l_t048.AM_POLLUTANT_CON
              FROM   CIS.TVP035SAMPLEDATA t035,
                     CIS.TVP048SAMPLEPOLL t048
              WHERE  t035.CD_COMPANY_SYSTEM = 'STW1'
              AND    t035.NO_PROPERTY       = t_prop(i).NO_PROPERTY
              AND    t035.NO_SERV_PROV      = t_prop(i).NO_SERV_PROV
              AND    t035.DT_CREATED BETWEEN CURRENT_DATE - 180 + 1
                                        AND CURRENT_DATE
              AND    t048.CD_COMPANY_SYSTEM = t035.CD_COMPANY_SYSTEM
              AND    t048.NO_PROPERTY       = t035.NO_PROPERTY
              AND    t048.NO_SERV_PROV      = t035.NO_SERV_PROV
              AND    t048.NO_BOTTLE         = t035.NO_BOTTLE
              AND    t048.DT_CREATED        = t035.DT_CREATED
              AND    t048.FG_BILLABLE       = 'Y'
              AND    t048.TP_POLLUTANT_389  = '007';
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 l_t048.AM_POLLUTANT_CON := 0;
            END;
            
            l_mo.TREFODCHEMSUSPSOLDEMAND := l_t048.AM_POLLUTANT_CON;
            
         END IF;

      END IF;

      -- From Tariff Charges
      
      IF l_tet.TECHARGECOMPSS > 0 THEN 
         IF NVL(l_mo.TREFODCHEMSUSPSOLDEMAND,0) = 0 THEN
         
            -- get last period data
            l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM 2 - SSST';  
            BEGIN
                SELECT NO_VALUE
                INTO   l_alg.NO_VALUE
                FROM   BT_SPR_TARIFF_ALGITEM
                WHERE  NO_PROPERTY      = t_prop(i).NO_PROPERTY
                AND    NO_SERV_PROV     = t_prop(i).NO_SERV_PROV       
                AND    CD_BILL_ALG_ITEM = 'SSST'
                AND    (   DT_START >= to_date('01/10/2015','dd/mm/yyyy') and DT_END <= to_date('01/04/2016','dd/mm/yyyy')
                        or DT_START >= to_date('01/04/2016','dd/mm/yyyy') and DT_END <= to_date('01/10/2016','dd/mm/yyyy') )
                order by DT_START desc;
             EXCEPTION
             WHEN NO_DATA_FOUND THEN
                  l_alg.NO_VALUE := 0;
             END;

            l_mo.TREFODCHEMSUSPSOLDEMAND := nvl(l_alg.NO_VALUE,0);                
                
            IF NVL(l_mo.TREFODCHEMSUSPSOLDEMAND,0) = 0 THEN    
                P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'No agreed limit or average strength for Suspended Solids, defaulted to 0',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
                l_no_row_war := l_no_row_war + 1;
            END IF;
            
         END IF;
      ELSE         
         l_mo.TREFODCHEMSUSPSOLDEMAND := 0;
      END IF;

     -- set Ammoniacal Nitrogen demand
     
     l_mo.TREFODCHEMAMONIANITROGENDEMAND := null ;
     
     IF l_tet.TECHARGECOMPAA > 0 THEN
        l_mo.AMMONIATREATMENTINDICATOR := 1;  
     ELSE
        l_mo.AMMONIATREATMENTINDICATOR := 0;
     END IF;
     
     IF (    l_mo.AMMONIATREATMENTINDICATOR = 1 
         AND l_mo.TREFODCHEMAMONIANITROGENDEMAND IS NULL)
     THEN
        l_rec_written := FALSE;
        P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'No Ammonia Nitrogen demand',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK, 1,100));
        l_no_row_exp := l_no_row_exp + 1;
     END IF;

     -- Set Trade Effluent components
        
     l_mo.TREFODCHEMCOMPXDEMAND := NULL;
     l_mo.TREFODCHEMCOMPYDEMAND := NULL;
     l_mo.TREFODCHEMCOMPZDEMAND := NULL;

     -- Find any special agreements

     l_progress := 'SELECT LU_TARIFF_SPECIAL_AGREEMENTS ';   
     BEGIN 
       SELECT SPECIAL_AGREEMENT_FACTOR,
              SPECIAL_AGREEMENT_FLAG,
              OFWAT_REFERENCE_NUMBER
       INTO   l_age.SPECIAL_AGREEMENT_FACTOR,
              l_age.SPECIAL_AGREEMENT_FLAG,
              l_age.OFWAT_REFERENCE_NUMBER
       FROM   LU_TARIFF_SPECIAL_AGREEMENTS
       WHERE  PROPERTY_NO = t_prop(i).NO_PROPERTY
       AND    TARIFFCODE  = t_prop(i).CD_TARIFF
       AND    SERVICECOMPONENTTYPE = t_prop(i).SERVICECOMPONENTTYPE;	           
     EXCEPTION
     WHEN NO_DATA_FOUND THEN
          l_age.SPECIAL_AGREEMENT_FACTOR := NULL;
          l_age.SPECIAL_AGREEMENT_FLAG := 'N';
          l_age.OFWAT_REFERENCE_NUMBER  := null;
     END;
 
     l_mo.DPIDSPECIALAGREEMENTFACTOR := l_age.SPECIAL_AGREEMENT_FACTOR;
     l_mo.DPIDSPECIALAGREEMENTREFERENCE := REPLACE(l_age.OFWAT_REFERENCE_NUMBER,' ','');
        
     IF l_age.SPECIAL_AGREEMENT_FLAG = 'Y' THEN
        l_mo.DPIDSPECIALAGREEMENTINPLACE := 1;
     ELSIF l_age.SPECIAL_AGREEMENT_FLAG = 'N' THEN
        l_mo.DPIDSPECIALAGREEMENTINPLACE := 0;
     END IF;

    -- Set up volume limits
    
      l_progress := 'SELECT LU_DISCHARGE_VOL_LIMITS ';
      BEGIN
         SELECT VOLUME_LIMIT,
                DPID_TYPE
         INTO   l_vol.VOLUME_LIMIT,
                l_vol.DPID_TYPE
         FROM   LU_DISCHARGE_VOL_LIMITS
         WHERE  NO_IWCS = l_mo.NO_IWCS;
       EXCEPTION
       WHEN NO_DATA_FOUND THEN
            l_vol.VOLUME_LIMIT := NULL;
            l_vol.DPID_TYPE := NULL;
       END;
      l_mo.VOLUME_LIMIT := l_vol.VOLUME_LIMIT;
      l_mo.DPID_TYPE := l_vol.DPID_TYPE;
      
      -- Get service component type for tariff
    
      l_progress := 'SELECT MO_TARIFF ';
      BEGIN
         SELECT SERVICECOMPONENTTYPE
         INTO   l_mot.SERVICECOMPONENTTYPE
         FROM   MO_TARIFF
         WHERE  TARIFFCODE_PK = l_mo.TARRIFCODE;
       EXCEPTION
       WHEN NO_DATA_FOUND THEN
            l_mot.SERVICECOMPONENTTYPE := null;
            P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid Tariff',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARRIFCODE,1,100));
            l_no_row_exp := l_no_row_exp + 1;
            l_rec_written := FALSE;
       END;
      l_mo.SERVICECOMPTYPE := l_mot.SERVICECOMPONENTTYPE;
    
     l_progress := 'INSERT MO_DISCHARGE_POINT ';           
    
     IF l_rec_written THEN
        BEGIN 
          INSERT INTO MO_DISCHARGE_POINT
          (DPID_PK, SPID_PK, SERVICECOMPTYPE, SCEFFECTIVEFROMDATE, DPEFFECTFROMDATE, DPEFFECTTODATE, DISCHARGEPOINTERASEFLAG, EFFECTFROMDATE, EFFECTTODATE, WHOLESALERID,
           TARRIFCODE, CHARGEABLEDAILYVOL, AMMONIANITROCAL, CHEMICALOXYGENDEMAND, SUSPENDEDSOLIDSLOAD, DOMMESTICALLOWANCE, SEASONALFACTOR, PERCENTAGEALLOWANCE, FIXEDALLOWANCE,
           RECEPTIONTREATMENTINDICATOR, PRIMARYTREATMENTINDICATOR, MARINETREATMENTINDICATOR, BIOLOGICALTREATMENTINDICATOR, SLUDGETREATMENTINDICATOR, AMMONIATREATMENTINDICATOR,
           TEFXTREATMENTINDICATOR, TEFYTREATMENTINDICATOR, TEFZTREATMENTINDICATOR, TEFAVAILABILITYDATAX, TEFAVAILABILITYDATAY, TEFAVAILABILITYDATAZ, TARRIFBAND, 
           SEWERAGEVOLUMEADJMENTHOD, SECONDADDRESSABLEOBJ, PRIMARYADDRESSABLEOBJ, TREFODCHEMOXYGENDEMAND, TREFODCHEMSUSPSOLDEMAND, TREFODCHEMAMONIANITROGENDEMAND, 
           TREFODCHEMCOMPXDEMAND, TREFODCHEMCOMPYDEMAND, TREFODCHEMCOMPZDEMAND, DPIDSPECIALAGREEMENTINPLACE, DPIDSPECIALAGREEMENTFACTOR, DPIDSPECIALAGREEMENTREFERENCE,
           NO_IWCS, NO_SAMPLE_POINT, NO_ACCOUNT, CONSENT_NO, STWPROPERTYNUMBER_PK, VOLUME_LIMIT, DPID_TYPE)
           VALUES
           (l_mo.DPID_PK, l_mo.SPID_PK, l_mo.SERVICECOMPTYPE, l_mo.SCEFFECTIVEFROMDATE, l_mo.DPEFFECTFROMDATE, l_mo.DPEFFECTTODATE, l_mo.DISCHARGEPOINTERASEFLAG, l_mo.EFFECTFROMDATE, l_mo.EFFECTTODATE, l_mo.WHOLESALERID,
            l_mo.TARRIFCODE, l_mo.CHARGEABLEDAILYVOL, l_mo.AMMONIANITROCAL, l_mo.CHEMICALOXYGENDEMAND, l_mo.SUSPENDEDSOLIDSLOAD, l_mo.DOMMESTICALLOWANCE, l_mo.SEASONALFACTOR, l_mo.PERCENTAGEALLOWANCE, l_mo.FIXEDALLOWANCE, -- V 1.22/1.23
            l_mo.RECEPTIONTREATMENTINDICATOR, l_mo.PRIMARYTREATMENTINDICATOR, l_mo.MARINETREATMENTINDICATOR, l_mo.BIOLOGICALTREATMENTINDICATOR, l_mo.SLUDGETREATMENTINDICATOR, l_mo.AMMONIATREATMENTINDICATOR,
            l_mo.TEFXTREATMENTINDICATOR, l_mo.TEFYTREATMENTINDICATOR, l_mo.TEFZTREATMENTINDICATOR, l_mo.TEFAVAILABILITYDATAX, l_mo.TEFAVAILABILITYDATAY, l_mo.TEFAVAILABILITYDATAZ, l_mo.TARRIFBAND,
            l_mo.SEWERAGEVOLUMEADJMENTHOD, l_mo.SECONDADDRESSABLEOBJ, l_mo.PRIMARYADDRESSABLEOBJ, l_mo.TREFODCHEMOXYGENDEMAND, l_mo.TREFODCHEMSUSPSOLDEMAND, l_mo.TREFODCHEMAMONIANITROGENDEMAND, 
            l_mo.TREFODCHEMCOMPXDEMAND, l_mo.TREFODCHEMCOMPYDEMAND, l_mo.TREFODCHEMCOMPZDEMAND, l_mo.DPIDSPECIALAGREEMENTINPLACE, l_mo.DPIDSPECIALAGREEMENTFACTOR, l_mo.DPIDSPECIALAGREEMENTREFERENCE,
            l_mo.NO_IWCS, l_mo.NO_SAMPLE_POINT, l_mo.NO_ACCOUNT, l_mo.CONSENT_NO, l_mo.STWPROPERTYNUMBER_PK, l_mo.VOLUME_LIMIT, l_mo.DPID_TYPE);            
        EXCEPTION 
        WHEN OTHERS THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.DPID_PK,1,100));      -- LSmith V0.03
             l_no_row_err := l_no_row_err + 1;
        END;
     ELSE
        l_no_row_dropped := l_no_row_dropped + 1;
     END IF;  

     IF l_rec_written THEN
        l_no_row_insert := l_no_row_insert + 1;
         
       nLCount := nLCount + 1;
       --
       IF MOD(nLCount, nLCommitFrequency) = 0 THEN
          COMMIT;
       END IF;

     ELSE 
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
         
     l_prev_prp := t_prop(i).NO_PROPERTY;
      
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
  
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP31', 1060, l_no_row_read,    'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP31', 1070, l_no_row_dropped, 'Dropped during Transform');   
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP31', 1080, l_no_row_insert,  'Written to Table ');    

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
END P_MOU_TRAN_DISCHARGE_POINT;
/
show error;

--CREATE OR REPLACE PUBLIC SYNONYM P_MOU_TRAN_DISCHARGE_POINT FOR P_MOU_TRAN_DISCHARGE_POINT;
exit;