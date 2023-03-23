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
-- Subversion $Revision: 4023 $
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
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;

nLCount                         PLS_INTEGER := 0;
nLCommitFrequency               PLS_INTEGER := 1000;

      
CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)                 
    IS 
    SELECT  t054.NO_PROPERTY,
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
            t056.DT_START AS T056_DT_START
     FROM   BT_TVP054           t054,
            LU_SERVICE_CATEGORY tcat,
            BT_SPR_TARIFF       trf,
            MO_TARIFF           com,
            CIS.TVP056SERVPROV  t056
     WHERE  t054.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end
     AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
     AND    com.TARIFFCODE_PK          = trf.CD_TARIFF
     AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV                          
     AND    tcat.SERVICECOMPONENTTYPE  = com.SERVICECOMPONENTTYPE   
     AND    tcat.SERVICECOMPONENTTYPE  in ('TE')
     AND    t056.CD_COMPANY_SYSTEM = t054.CD_COMPANY_SYSTEM
     AND    t056.NO_PROPERTY       = t054.NO_PROPERTY
     AND    t056.NO_SERV_PROV      = t054.NO_SERV_PROV     
     ORDER BY t054.NO_PROPERTY, t054.NO_SERV_PROV, trf.NO_TARIFF_GROUP, trf.NO_TARIFF_SET;
                          
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

      -- get discharge point id

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
      WHEN TOO_MANY_ROWS THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;
        
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
      WHEN TOO_MANY_ROWS THEN
           l_rec_written := FALSE;
           P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
           l_no_row_exp := l_no_row_exp + 1;
      END;

      l_mo.DPID_PK := substr(l_ext.CD_EXT_REF || l_ext2.CD_EXT_REF,1,13);          --- fix  should be 32 char
        
      -- get supply point id

      l_progress := 'SELECT LU_SPID_RANGE ';   
      SELECT SPID_PK
      INTO   l_spid.SPID_PK
      FROM   LU_SPID_RANGE
      WHERE  CORESPID_PK     = t_prop(i).CORESPID
      AND    SERVICECATEGORY = t_prop(i).SUPPLY_POINT_CODE;	
       
      l_mo.SPID_PK := l_spid.SPID_PK;
      l_mo.SERVICECOMPTYPE := t_prop(i).SERVICECOMPONENTTYPE;

      --SCEFFECTIVEFROMDATE	DATE	No		4	SC Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from. STW001 - FK implementing relationship to Trade Effluent Service Component of SPID
      --DPEFFECTFROMDATE	  DATE	No		5	DP Effective From Date~~~D4006 - Where this is included in a Data Transaction, this is the date that new data or any change to data included in the Data Transaction is effective from
      --DPEFFECTTODATE	    DATE	Yes		6	DP Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is tThe date that new data or any change to data included in the Data Transaction is effective to

      l_mo.SCEFFECTIVEFROMDATE := t_prop(i).DT_STATUS;   -- ??         supply point start date
      l_mo.DPEFFECTFROMDATE := t_prop(i).T056_DT_START;   -- ??
      l_mo.DPEFFECTTODATE := null;   -- ??
                  
      -- State of service component
       
      IF t_prop(i).ST_SERV_PROV IN ('A','G') THEN
         l_mo.DISCHARGEPOINTERASEFLAG := 0;
      ELSE
         l_mo.DISCHARGEPOINTERASEFLAG := 1;         
      END IF;

      --EFFECTFROMDATE	DATE	NO		8	Effective FROM Date~~~D4006 - WHERE this IS included IN A DATA TRANSACTION, this IS THE DATE that NEW DATA OR ANY CHANGE TO DATA included IN THE DATA TRANSACTION IS effective FROM. STW001 - FK implementing relationship TO Trade Effluent Service Component OF SPID
      --EFFECTTODATE	DATE	Yes		9	Effective To Date~~~D4024 - Where this is included in a Data Transaction, this is tThe date that new data or any change to data included in the Data Transaction is effective to

      l_mo.EFFECTFROMDATE := t_prop(i).T056_DT_START;   -- ??
      l_mo.EFFECTTODATE := NULL;   -- ??

      --WHOLESALERID	VARCHAR2(12 BYTE)	No		10	Wholesaler ID~~~D4025 - Unique ID identifying the Wholesaler. STW003 - FK implementing relationship to Tariff and Tariff Band (if required).
      l_mo.WHOLESALERID := 'SEVERN-W';      -- check cross border ?? tariff link to wholesaler

      l_mo.TARRIFCODE := t_prop(i).CD_TARIFF;
      l_mo.CHARGEABLEDAILYVOL := NULL;
      l_mo.AMMONIANITROCAL := null;
      l_mo.CHEMICALOXYGENDEMAND := NULL;
      l_mo.SUSPENDEDSOLIDSLOAD := NULL; 
         
      l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - AUFV ';   
      BEGIN
         SELECT NO_VALUE
         INTO   l_alg.NO_VALUE
         FROM   BT_SPR_TARIFF_ALGITEM
         WHERE  NO_PROPERTY   = t_prop(i).NO_PROPERTY
         AND    NO_SERV_PROV  = t_prop(i).NO_SERV_PROV
         AND    CD_BILL_ALG_ITEM = 'AUFV';
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
           l_alg.NO_VALUE := 0;
      END;  
      l_mo.DOMMESTICALLOWANCE := l_alg.NO_VALUE;

      l_mo.SEASONALFACTOR := NULL;
      l_mo.PERCENTAGEALLOWANCE := 0;

      l_mo.FIXEDALLOWANCE := 0; 
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
         l_mo.FIXEDALLOWANCE := l_alg.NO_VALUE;
      END IF;  

      l_mo.RECEPTIONTREATMENTINDICATOR := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN
         l_progress := 'SELECT BT_SP_TARIFF_ALG';  
         l_mo.RECEPTIONTREATMENTINDICATOR := 1;
         BEGIN
           SELECT CD_ALGORITHM
           INTO   l_spalg.CD_ALGORITHM
           FROM   BT_SP_TARIFF_ALG
           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
           AND    CD_ALGORITHM IN ('AUCCA', 'CCLUA', 'TWCCA');
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_mo.RECEPTIONTREATMENTINDICATOR := 0;
         END;
      END IF;

      -- TP_EQUIPMENT ADDEED TO BT_SP_TARIFF_REFTAB

      l_mo.PRIMARYTREATMENTINDICATOR := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN
         l_progress := 'SELECT BT_SP_TARIFF_REFTAB';   

         l_mo.PRIMARYTREATMENTINDICATOR := 1;
         BEGIN
           SELECT TP_EQUIPMENT
           INTO   l_ref.TP_EQUIPMENT
           FROM   BT_SP_TARIFF_REFTAB
           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
           AND    TP_EQUIPMENT  IS NOT NULL;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_mo.PRIMARYTREATMENTINDICATOR := 0;
         END;
      END IF;


      l_mo.MARINETREATMENTINDICATOR := 0;

      l_mo.BIOLOGICALTREATMENTINDICATOR := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN  
         l_progress := 'SELECT BT_SP_TARIFF_ALG - AUBTA';   

         l_mo.BIOLOGICALTREATMENTINDICATOR := 1;
         BEGIN
           SELECT CD_ALGORITHM
           INTO   l_spalg.CD_ALGORITHM
           FROM   BT_SP_TARIFF_ALG
           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
           AND    CD_ALGORITHM IN ('AUBTA');
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_mo.BIOLOGICALTREATMENTINDICATOR := 0;
         END;
      END IF;

      l_mo.SLUDGETREATMENTINDICATOR := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN  
         l_mo.SLUDGETREATMENTINDICATOR := 1;
         l_progress := 'SELECT BT_SP_TARIFF_ALG - AUSTA';   
         BEGIN
           SELECT CD_ALGORITHM
           INTO   l_spalg.CD_ALGORITHM
           FROM   BT_SP_TARIFF_ALG
           WHERE  CD_TARIFF     = t_prop(i).CD_TARIFF
           AND    CD_SERV_PROV  = t_prop(i).CD_SERVICE_PROV
           AND    CD_ALGORITHM IN ('AUSTA', 'TWSTA');
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_mo.SLUDGETREATMENTINDICATOR := 0;
         END;
      END IF;

      --SAMMONIATREATMENTINDICATOR	NUMBER(1,0)	No		25	Ammonia Treatment Indicator~~~D6019 - Flag to indicate whether ammonia charges apply to Trade Effluent from the Discharge Point Variable name: ATI
      l_mo.AMMONIATREATMENTINDICATOR := 0;      
      l_mo.TEFXTREATMENTINDICATOR := 0;
      l_mo.TEFYTREATMENTINDICATOR := 0;
      l_mo.TEFZTREATMENTINDICATOR := 0;
        
    --    !!! D6032 not on fV
        
      --TEFAVAILABILITYDATAX	NUMBER(9,0)	Yes		29	cXâ??~~~D6032 - Trade Effluent Availability Data: Trade Effluent Component X Demand load in kg/day
      --TEFAVAILABILITYDATAY	NUMBER(9,0)	Yes		30	cYâ??~~~D6033 - Trade Effluent AVAILABILITY DATA: Trade Effluent Component Y DEMAND load IN kg/DAY
      --TEFAVAILABILITYDATAZ	NUMBER(9,0)	Yes		31	cZâ??~~~D6034 - Trade Effluent Availability Data: Trade Effluent Component Z Demand load in kg/day
        
      --  !!! Trade Effluent Tariff Band  D6024 - Tariff band for Trade Effluent banded charge  defined as char(32)  but number(2) on PDM
        
      l_mo.TARRIFBAND := NULL;                                                                 -- L smith V 0.02
      IF t_prop(i).T054_DT_END IS NULL THEN
         l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - PCODE';   
         l_mo.TARRIFBAND := 1;
         BEGIN
            SELECT DS_REF_TAB
            INTO   l_t358.DS_REF_TAB
            FROM   BT_SPR_TARIFF_ALGITEM alg,
                   BT_SP_TARIFF_ALGITEM  salg,
                   CIS.TVP358REFTAB      t358
            WHERE  alg.NO_PROPERTY       = t_prop(i).NO_PROPERTY
            AND    alg.NO_SERV_PROV      = t_prop(i).NO_SERV_PROV
            AND    alg.CD_BILL_ALG_ITEM  = 'PCODE'
            AND    salg.CD_TARIFF        = alg.CD_TARIFF
            AND    salg.CD_BILL_ALG_ITEM = alg.CD_BILL_ALG_ITEM            
            AND    t358.TP_REF_TAB       = alg.TP_REF_TAB
            AND    t358.CD_REF_TAB       = alg.CD_REF_TAB
            AND    t358.CD_COMPANY_SYSTEM = 'STW1';
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_mo.TARRIFBAND := NULL;                                                        -- L smith V 0.02
         WHEN TOO_MANY_ROWS THEN
              l_rec_written := FALSE;
              P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for service provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
              l_no_row_exp := l_no_row_exp + 1;
         END; 
      END IF;

      --Dervied from TE Database ??        
      l_mo.SEWERAGEVOLUMEADJMENTHOD := 'DA';
      
      --!!! NOT ON f v
        
      --!!! SECONDADDRESSABLEOBJ	VARCHAR2(100 BYTE)	Yes		34	Secondary Addressable Object~~~D5002 - BS7666 Secondary Addressable OBJECT IF available
      -- !!! PRIMARYADDRESSABLEOBJ	VARCHAR2(100 BYTE)	Yes		35	Primary Addressable Object~~~D5003 - BS7666 Primary Addressable Object if available
      
      l_mo.TREFODCHEMOXYGENDEMAND := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN
         l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - CODSD';   
         BEGIN
          SELECT CD_BILL_ALG_ITEM,
                 NO_VALUE
          INTO   l_alg.CD_BILL_ALG_ITEM,
                 l_alg.NO_VALUE
           FROM 
           (SELECT CD_BILL_ALG_ITEM,
                   nvl(NO_VALUE,0) AS NO_VALUE,
                    CASE 
                      WHEN CD_BILL_ALG_ITEM = 'CODSD'
                         THEN '1'
                      WHEN CD_BILL_ALG_ITEM = 'CODST'
                         THEN '2'
                      WHEN CD_BILL_ALG_ITEM = 'CODAP'            
                        THEN '3'              
                    END AS CD_BILL_ALG_ITEM_SORT
             FROM   BT_SPR_TARIFF_ALGITEM
             WHERE  NO_PROPERTY      = t_prop(i).NO_PROPERTY
             AND    NO_SERV_PROV     = t_prop(i).NO_SERV_PROV       
             AND    CD_BILL_ALG_ITEM IN ( 'CODSD', 'CODST', 'CODAP')
             AND    DT_END           IS NULL
             ORDER BY CD_BILL_ALG_ITEM_SORT )
          WHERE ROWNUM = 1;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_alg.CD_BILL_ALG_ITEM := NULL;
         END;
          
         IF l_alg.CD_BILL_ALG_ITEM IS NOT NULL THEN
            IF l_alg.CD_BILL_ALG_ITEM IN ('CODSD', 'CODST') THEN
               l_mo.TREFODCHEMOXYGENDEMAND := l_alg.NO_VALUE;
            ELSE
              SELECT NVL(AVG(t048.AM_POLLUTANT_CON),0)
              INTO   l_t048.AM_POLLUTANT_CON
              FROM   CIS.TVP035SAMPLEDATA t035,
                     CIS.TVP048SAMPLEPOLL t048
              WHERE  t035.CD_COMPANY_SYSTEM = 'STW1'
              AND    t035.NO_PROPERTY       = t_prop(i).NO_PROPERTY
              AND    t035.NO_SERV_PROV      = t_prop(i).NO_SERV_PROV
              AND    t035.DT_CREATED BETWEEN CURRENT_DATE - 90 + 1
                                        AND CURRENT_DATE
              AND    t048.CD_COMPANY_SYSTEM = t035.CD_COMPANY_SYSTEM
              AND    t048.NO_PROPERTY       = t035.NO_PROPERTY
              AND    t048.NO_SERV_PROV      = t035.NO_SERV_PROV
              AND    t048.NO_BOTTLE         = t035.NO_BOTTLE
              AND    t048.DT_CREATED        = t035.DT_CREATED
              AND    t048.FG_BILLABLE       = 'Y'
              AND    t048.TP_POLLUTANT_389  = '0V0';

              l_mo.TREFODCHEMOXYGENDEMAND := l_t048.AM_POLLUTANT_CON;
            END IF;             
            
         END IF;

      END IF;
                                    
      --Use SSSD (STW Standard  Suspended Solids Strength), if not available, SSST (Agreed Suspended Solids Strength). When neither available, get the SSAP (Suspended Solids Sample Average Period, default 90) and then calculate average Suspended Solids Strength derived from 'Calculate Average Suspended Solids Strength' SQL in 'Extract Criteria'

      l_mo.TREFODCHEMSUSPSOLDEMAND := 0;
      IF t_prop(i).T054_DT_END IS NULL THEN        
         l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM - SSSD';  

         BEGIN
            SELECT CD_BILL_ALG_ITEM,
                   NO_VALUE
            INTO   l_alg.CD_BILL_ALG_ITEM,
                   l_alg.NO_VALUE
             FROM 
             (SELECT CD_BILL_ALG_ITEM,
                     nvl(NO_VALUE,0) as NO_VALUE,
                      CASE 
                        WHEN CD_BILL_ALG_ITEM = 'SSSD'
                           THEN '1'
                        WHEN CD_BILL_ALG_ITEM = 'SSST'
                           THEN '2'
                        WHEN CD_BILL_ALG_ITEM = 'SSAP'            
                          THEN '3'              
                      END AS CD_BILL_ALG_ITEM_SORT
               FROM   BT_SPR_TARIFF_ALGITEM
               WHERE  NO_PROPERTY      = t_prop(i).NO_PROPERTY
               AND    NO_SERV_PROV     = t_prop(i).NO_SERV_PROV       
               AND    CD_BILL_ALG_ITEM IN ( 'SSSD', 'SSST', 'SSAP')
               AND    DT_END           IS NULL
               ORDER BY CD_BILL_ALG_ITEM_SORT )
            WHERE ROWNUM = 1;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_alg.CD_BILL_ALG_ITEM := NULL;
         END;

         IF l_alg.CD_BILL_ALG_ITEM IS NOT NULL THEN
            IF l_alg.CD_BILL_ALG_ITEM IN ('SSSD', 'SSST') THEN
               l_mo.TREFODCHEMSUSPSOLDEMAND := l_alg.NO_VALUE;
            ELSE
              SELECT NVL(AVG(t048.AM_POLLUTANT_CON),0)
              INTO   l_t048.AM_POLLUTANT_CON
              FROM   CIS.TVP035SAMPLEDATA t035,
                     CIS.TVP048SAMPLEPOLL t048
              WHERE  t035.CD_COMPANY_SYSTEM = 'STW1'
              AND    t035.NO_PROPERTY       = t_prop(i).NO_PROPERTY
              AND    t035.NO_SERV_PROV      = t_prop(i).NO_SERV_PROV
              AND    t035.DT_CREATED BETWEEN CURRENT_DATE - 90 + 1
                                        AND CURRENT_DATE
              AND    t048.CD_COMPANY_SYSTEM = t035.CD_COMPANY_SYSTEM
              AND    t048.NO_PROPERTY       = t035.NO_PROPERTY
              AND    t048.NO_SERV_PROV      = t035.NO_SERV_PROV
              AND    t048.NO_BOTTLE         = t035.NO_BOTTLE
              AND    t048.DT_CREATED        = t035.DT_CREATED
              AND    t048.FG_BILLABLE       = 'Y'
              AND    t048.TP_POLLUTANT_389  = '007';

              l_mo.TREFODCHEMSUSPSOLDEMAND := l_t048.AM_POLLUTANT_CON;
            END IF;
         END IF;
         
     END IF;

     l_mo.TREFODCHEMAMONIANITROGENDEMAND := NULL;     
     l_mo.TREFODCHEMCOMPXDEMAND := NULL;
     l_mo.TREFODCHEMCOMPYDEMAND := NULL;
     l_mo.TREFODCHEMCOMPZDEMAND := NULL;

     --DPIDSPECIALAGREEMENTINPLACE, DPIDSPECIALAGREEMENTFACTOR, DPIDSPECIALAGREEMENTREFERENCE
     -- find any special agreements

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
     l_mo.DPIDSPECIALAGREEMENTREFERENCE := l_age.OFWAT_REFERENCE_NUMBER;
        
     IF l_age.SPECIAL_AGREEMENT_FLAG = 'Y' THEN
        l_mo.DPIDSPECIALAGREEMENTINPLACE := 1;
     ELSIF l_age.SPECIAL_AGREEMENT_FLAG = 'N' THEN
        l_mo.DPIDSPECIALAGREEMENTINPLACE := 0;
     END IF;
        
     l_progress := 'INSERT MO_DISCHARGE_POINT ';           
    
     IF l_rec_written THEN
        BEGIN 
          INSERT INTO MO_DISCHARGE_POINT
          (DPID_PK, SPID_PK, SERVICECOMPTYPE, SCEFFECTIVEFROMDATE, DPEFFECTFROMDATE, DPEFFECTTODATE, DISCHARGEPOINTERASEFLAG, EFFECTFROMDATE, EFFECTTODATE, WHOLESALERID,
           TARRIFCODE, CHARGEABLEDAILYVOL, AMMONIANITROCAL, CHEMICALOXYGENDEMAND, SUSPENDEDSOLIDSLOAD, DOMMESTICALLOWANCE, SEASONALFACTOR, PERCENTAGEALLOWANCE, FIXEDALLOWANCE,
           RECEPTIONTREATMENTINDICATOR, PRIMARYTREATMENTINDICATOR, MARINETREATMENTINDICATOR, BIOLOGICALTREATMENTINDICATOR, SLUDGETREATMENTINDICATOR, AMMONIATREATMENTINDICATOR,
           TEFXTREATMENTINDICATOR, TEFYTREATMENTINDICATOR, TEFZTREATMENTINDICATOR, TEFAVAILABILITYDATAX, TEFAVAILABILITYDATAY, TEFAVAILABILITYDATAZ, TARRIFBAND, 
           SEWERAGEVOLUMEADJMENTHOD, SECONDADDRESSABLEOBJ, PRIMARYADDRESSABLEOBJ, TREFODCHEMOXYGENDEMAND, TREFODCHEMSUSPSOLDEMAND, TREFODCHEMAMONIANITROGENDEMAND, 
           TREFODCHEMCOMPXDEMAND, TREFODCHEMCOMPYDEMAND, TREFODCHEMCOMPZDEMAND, DPIDSPECIALAGREEMENTINPLACE, DPIDSPECIALAGREEMENTFACTOR, DPIDSPECIALAGREEMENTREFERENCE)         -- LSmith V0.03
           VALUES
           (l_mo.DPID_PK, l_mo.SPID_PK, l_mo.SERVICECOMPTYPE, l_mo.SCEFFECTIVEFROMDATE, l_mo.DPEFFECTFROMDATE, l_mo.DPEFFECTTODATE, l_mo.DISCHARGEPOINTERASEFLAG, l_mo.EFFECTFROMDATE, l_mo.EFFECTTODATE, l_mo.WHOLESALERID,
            l_mo.TARRIFCODE, l_mo.CHARGEABLEDAILYVOL, l_mo.AMMONIANITROCAL, l_mo.CHEMICALOXYGENDEMAND, l_mo.SUSPENDEDSOLIDSLOAD, l_mo.DOMMESTICALLOWANCE, l_mo.SEASONALFACTOR, l_mo.PERCENTAGEALLOWANCE, l_mo.FIXEDALLOWANCE,
            l_mo.RECEPTIONTREATMENTINDICATOR, l_mo.PRIMARYTREATMENTINDICATOR, l_mo.MARINETREATMENTINDICATOR, l_mo.BIOLOGICALTREATMENTINDICATOR, l_mo.SLUDGETREATMENTINDICATOR, l_mo.AMMONIATREATMENTINDICATOR,
            l_mo.TEFXTREATMENTINDICATOR, l_mo.TEFYTREATMENTINDICATOR, l_mo.TEFZTREATMENTINDICATOR, l_mo.TEFAVAILABILITYDATAX, l_mo.TEFAVAILABILITYDATAY, l_mo.TEFAVAILABILITYDATAZ, l_mo.TARRIFBAND,
            l_mo.SEWERAGEVOLUMEADJMENTHOD, l_mo.SECONDADDRESSABLEOBJ, l_mo.PRIMARYADDRESSABLEOBJ, l_mo.TREFODCHEMOXYGENDEMAND, l_mo.TREFODCHEMSUSPSOLDEMAND, l_mo.TREFODCHEMAMONIANITROGENDEMAND, 
            l_mo.TREFODCHEMCOMPXDEMAND, l_mo.TREFODCHEMCOMPYDEMAND, l_mo.TREFODCHEMCOMPZDEMAND, l_mo.DPIDSPECIALAGREEMENTINPLACE, l_mo.DPIDSPECIALAGREEMENTFACTOR, l_mo.DPIDSPECIALAGREEMENTREFERENCE);
        EXCEPTION 
        WHEN OTHERS THEN 
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));      -- LSmith V0.03
             l_no_row_err := l_no_row_err + 1;
        END;
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