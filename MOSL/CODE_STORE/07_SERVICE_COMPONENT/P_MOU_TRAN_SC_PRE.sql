create or replace
PROCEDURE P_MOU_TRAN_SC_PRE(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                              no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                              return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO Extract
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_SC_PRE.sql
--
-- Subversion $Revision: 6316 $
--
-- CREATED        : 15/04/2016
--
-- DESCRIPTION    : Procedure to create working tables of tariffs/service provisions tariffs
--                  for use in P_MOU_TRAN_SC_UW, _MOU_TRAN_SC_MPW and P_MOU_TRAN_SERVICE_COMPONENT
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      15/04/2016          S.Badhan        Initial Draft
-- V 0.02      20/06/2016          L.Smith         Performance changes
-- V 0.03      04/07/2016          O.Badmus        BT_SPR_TARIFF query is updated to include properties in LU_SPID_OWC_RETAILER (Properties that have switched supplier)
-- V 0.04      21/09/2016          S.Badhan        Performance changes.
-- V 0.05      29/09/2016          S.Badhan        Performance changes.
-- V 0.06      25/10/2016          S.Badhan        New index added on BT_SPR_TARIFF_EXTREF.
-- V 0.07      21/11/2016          D.Cheung        Add BT_MISS_AG_SC to BT_SPR_TARIFF
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_SC_PRE';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_uw                          BT_SC_UW%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;

BEGIN

   -- initial variables

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

  -- start processing

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  l_progress := 'INSERT INTO BT_SP_TARIFF ';

  --1. Service Provision Tariff
  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SP_TARIFF';

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SP_TARIFF (
  CD_SERV_PROV,
  CD_SERVICE_ABBREV,
  DS_SERVICE,
  CD_TARIFF,
  DS_TARIFF,
  CD_REV_CLASS_150,
  DS_REV_CLASS_150 )
  SELECT DISTINCT
     T055.CD_SERVICE_PROV,
     t055.CD_SERVICE_ABBREV,
     T055.DS_SERVICE,
     T322.CD_TARIFF,
     T057.DS_TARIFF,
     T314.CD_REV_CLASS_150,
     R150.DS_REF_TAB
  FROM   CIS.TVP055SERVICE      T055,
         CIS.TVP057TARIFF       T057,
         CIS.TVP314TARACCLSAPPL t314,
         CIS.TVP322TARIFFVER    T322,
         CIS.TVP358REFTAB       R150
  WHERE  T322.CD_COMPANY_SYSTEM = 'STW1'
  AND    T322.CD_TAR_VER_ST_72 = 'R'
  AND    (    T322.dt_effective_to IS NULL
          OR (    t322.dt_effective_to    > SYSDATE
              AND t322.dt_effective_from <> dt_effective_to   )    )
  AND   t057.cd_company_system = T322.CD_COMPANY_SYSTEM
  AND   t057.cd_tariff         = t322.cd_tariff
  AND   t055.cd_company_system = 'STW1'
  AND   t055.cd_service_prov   = t057.cd_service_prov
  AND   T314.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND   T314.CD_TARIFF         = T322.CD_TARIFF
  AND   T314.CD_REV_CLASS_150 NOT IN ('AD   ',--Assessed HH
                                      'R    ',--Measured HH
                                      'RD   ',--Unmeas Arr HH
                                      'ST   ',--Social Tariff HH
                                      'UA   ',--Unmeas Adv HH
                                      'V    ')--Vulnerable HH
  AND   R150.CD_COMPANY_SYSTEM = T314.CD_COMPANY_SYSTEM
  AND   R150.TP_REF_TAB        = 150
  AND   R150.CD_REF_TAB        = T314.CD_REV_CLASS_150
  AND   R150.IND_ACTIVE       <> 'D';

  commit;

  l_progress := 'INSERT INTO BT_SP_TARIFF_ALG ';
  --2. Tariff Algorithms

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SP_TARIFF_ALG';

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SP_TARIFF_ALG
  (CD_SERV_PROV,
   CD_TARIFF,
   CD_ALGORITHM,
   DS_ALGORITHM)
  SELECT DISTINCT
     T055.CD_SERVICE_PROV,
     T322.CD_TARIFF,
     T766.CD_ALGORITHM,
     T759.DS_ALGORITHM
  FROM
     CIS.TVP055SERVICE    T055,
     CIS.TVP057TARIFF     T057,
     CIS.TVP314TARACCLSAPPL t314,
     CIS.TVP322TARIFFVER  T322,
     CIS.TVP358REFTAB     R150,
     CIS.TVP766TVSELEMENT T766,
     CIS.TVP759ALGORITHM  T759
  WHERE
      T322.CD_COMPANY_SYSTEM = 'STW1'
  AND T322.CD_TAR_VER_ST_72 = 'R'
  AND(T322.dt_effective_to is null
       or (t322.dt_effective_to > SYSDATE
               AND t322.dt_effective_from <> t322.dt_effective_to      )    )
  --
  AND   t057.cd_company_system=T322.CD_COMPANY_SYSTEM
  AND   t057.cd_tariff        =t322.cd_tariff
  --
  AND   t055.cd_company_system='STW1'
  AND   t055.cd_service_prov  = t057.cd_service_prov
  --
  AND  T766.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND  T766.NO_COMBINE_322    = T322.NO_COMBINE_322
  --
  AND  T759.CD_COMPANY_SYSTEM = T766.CD_COMPANY_SYSTEM
  AND  T759.CD_ALGORITHM      = T766.CD_ALGORITHM
  --
  AND  T314.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND  T314.CD_TARIFF         = T322.CD_TARIFF
  AND  T314.CD_REV_CLASS_150 NOT IN ('AD   ',--Assessed HH
                                     'R    ',--Measured HH
                                     'RD   ',--Unmeas Arr HH
                                     'ST   ',--Social Tariff HH
                                     'UA   ',--Unmeas Adv HH
                                     'V    ')--Vulnerable HH
  --
  AND  R150.CD_COMPANY_SYSTEM = T314.CD_COMPANY_SYSTEM
  AND  R150.TP_REF_TAB        = 150
  AND  R150.CD_REF_TAB        = T314.CD_REV_CLASS_150
  AND  R150.IND_ACTIVE       <> 'D'
  ;

  commit;

  l_progress := 'INSERT INTO BT_SP_TARIFF_ALGITEM - 1';
  --3. Tariff Algorithm Items as defined in element charges

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SP_TARIFF_ALGITEM';

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SP_TARIFF_ALGITEM
  (CD_SERV_PROV,
   CD_TARIFF,
   CD_ALGORITHM,
   CD_BILL_ALG_ITEM,
   DS_BILL_ALG_ITEM)
  SELECT DISTINCT
     T055.CD_SERVICE_PROV,
     T322.CD_TARIFF,
     T766.CD_ALGORITHM,
     T766.CD_BILL_ALG_ITEM,
     T770.DS_BILL_ALG_ITEM
  FROM
     CIS.TVP055SERVICE    T055,
     CIS.TVP057TARIFF     T057,
     CIS.TVP314TARACCLSAPPL t314,
     CIS.TVP322TARIFFVER  T322,
     CIS.TVP358REFTAB     R150,
     CIS.TVP766TVSELEMENT T766,
     CIS.TVP770BLALGITEMTP T770
  WHERE
      T322.CD_COMPANY_SYSTEM = 'STW1'
  AND T322.CD_TAR_VER_ST_72 = 'R'
  AND(T322.dt_effective_to IS NULL
       or (t322.dt_effective_to > SYSDATE
               AND t322.dt_effective_from <> t322.dt_effective_to      )    )
  --
  AND   t057.cd_company_system=T322.CD_COMPANY_SYSTEM
  AND   t057.cd_tariff        =t322.cd_tariff
  --
  AND   t055.cd_company_system='STW1'
  AND   t055.cd_service_prov  = t057.cd_service_prov
  --
  AND  T766.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND  T766.NO_COMBINE_322    = T322.NO_COMBINE_322
  --
  --
  AND  T766.CD_COMPANY_SYSTEM = T770.CD_COMPANY_SYSTEM
  AND  T766.CD_BILL_ALG_ITEM  = T770.CD_BILL_ALG_ITEM
  --
  AND  T314.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND  T314.CD_TARIFF         = T322.CD_TARIFF
  AND  T314.CD_REV_CLASS_150 NOT IN ('AD   ',--Assessed HH
                                     'R    ',--Measured HH
                                     'RD   ',--Unmeas Arr HH
                                     'ST   ',--Social Tariff HH
                                     'UA   ',--Unmeas Adv HH
                                     'V    ')--Vulnerable HH
  --
  AND  R150.CD_COMPANY_SYSTEM = T314.CD_COMPANY_SYSTEM
  AND  R150.TP_REF_TAB        = 150
  AND  R150.CD_REF_TAB        = T314.CD_REV_CLASS_150
  AND  R150.IND_ACTIVE       <> 'D'
  ;

  commit;

  l_progress := 'INSERT INTO BT_SP_TARIFF_ALGITEM - 2';
  --3b. Tariff Algorithm Items referenced in Algorithm Code / COBOL

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SP_TARIFF_ALGITEM
  (CD_SERV_PROV,
   CD_TARIFF,
   CD_ALGORITHM,
   CD_BILL_ALG_ITEM,
   DS_BILL_ALG_ITEM)
  SELECT * FROM
  (SELECT DISTINCT
     T.CD_SERV_PROV,
     T.CD_TARIFF,
     T.CD_ALGORITHM,
     t770.CD_BILL_ALG_ITEM,
     t770.DS_BILL_ALG_ITEM
  FROM BT_SP_tariff_alg t,
       CIS.TVP770BLALGITEMTP t770
  WHERE
   (
        (T.CD_ALGORITHM='MCH03'	AND t770.CD_BILL_ALG_ITEM='CHGZ' ) 	-- Charging Zone
     OR (T.CD_ALGORITHM='ZRV01'	AND t770.CD_BILL_ALG_ITEM='CHGZ' ) 	-- Charging Zone
     OR (T.CD_ALGORITHM='AUBTA'	AND t770.CD_BILL_ALG_ITEM='CODAP' ) 	-- COD Sample Average Period
     OR (T.CD_ALGORITHM='TWBTA'	AND t770.CD_BILL_ALG_ITEM='CODAP' ) 	-- COD Sample Average Period
     OR (T.CD_ALGORITHM='AUBTA'	AND t770.CD_BILL_ALG_ITEM='CODPC' ) 	-- COD Discount Percentage
     OR (T.CD_ALGORITHM='TWBTA'	AND t770.CD_BILL_ALG_ITEM='CODPC' ) 	-- COD Discount Percentage
     OR (T.CD_ALGORITHM='AUBTA'	AND t770.CD_BILL_ALG_ITEM='CODSD' ) 	-- STW Standard COD Strength
     OR (T.CD_ALGORITHM='TWBTA'	AND t770.CD_BILL_ALG_ITEM='CODSD' ) 	-- STW Standard COD Strength
     OR (T.CD_ALGORITHM='ZRV01'	AND t770.CD_BILL_ALG_ITEM='RV' ) 	-- Rateable Value
     OR (T.CD_ALGORITHM='AUSTA'	AND t770.CD_BILL_ALG_ITEM='SSDP' ) 	-- Suspended Solids Discount Percentage
     OR (T.CD_ALGORITHM='TWSTA'	AND t770.CD_BILL_ALG_ITEM='SSDP' ) 	-- Suspended Solids Discount Percentage
     OR (T.CD_ALGORITHM='AUSTA'	AND t770.CD_BILL_ALG_ITEM='SSSD' ) 	-- STW Standard Suspended Solids Strength
     OR (T.CD_ALGORITHM='TWSTA'	AND t770.CD_BILL_ALG_ITEM='SSSD' ) 	-- STW Standard Suspended Solids Strength
     OR (T.CD_ALGORITHM='VCLUA'	AND t770.CD_BILL_ALG_ITEM='SSSD' ) 	-- STW Standard Suspended Solids Strength
     OR (T.CD_ALGORITHM='AUSTA'	AND t770.CD_BILL_ALG_ITEM='SSST' ) 	-- Agreed Suspended Solids Srength
     OR (T.CD_ALGORITHM='TWSTA'	AND t770.CD_BILL_ALG_ITEM='SSST' ) 	-- Agreed Suspended Solids Srength
     OR (T.CD_ALGORITHM='AUBTA'	AND t770.CD_BILL_ALG_ITEM='AUFC' )  -- TW Fixed Volume to Deduct
     OR (T.CD_ALGORITHM='AUCCA'	AND t770.CD_BILL_ALG_ITEM='AUFC' )  -- TW Fixed Volume to Deduct
     OR (T.CD_ALGORITHM='AURSC'	AND t770.CD_BILL_ALG_ITEM='AUFC' )  -- TW Fixed Volume to Deduct
     OR (T.CD_ALGORITHM='AUSTA'	AND t770.CD_BILL_ALG_ITEM='AUFC' )  -- TW Fixed Volume to Deduct
     OR (T.CD_ALGORITHM='AUVCA'	AND t770.CD_BILL_ALG_ITEM='AUFC' )  -- TW Fixed Volume to Deduct
     OR (T.CD_ALGORITHM='AUCCA'	AND t770.CD_BILL_ALG_ITEM='TWCD' ) 	-- Trade Waste Conveyancing Discount
     OR (T.CD_ALGORITHM='AUCCA'	AND t770.CD_BILL_ALG_ITEM='TWCD' ) 	-- Trade Waste Conveyancing Discount
     OR (T.CD_ALGORITHM='CCLUA'	AND t770.CD_BILL_ALG_ITEM='TWCD' ) 	-- Trade Waste Conveyancing Discount
     OR (T.CD_ALGORITHM='TWCCA'	AND t770.CD_BILL_ALG_ITEM='TWCD' ) 	-- Trade Waste Conveyancing Discount
     OR (T.CD_ALGORITHM='TWCCA'	AND t770.CD_BILL_ALG_ITEM='TWCD' ) 	-- Trade Waste Conveyancing Discount
     OR (T.CD_ALGORITHM='USAC1'	AND t770.CD_BILL_ALG_ITEM='USACB')  -- United Util Site Area Charge Band
     OR (T.CD_ALGORITHM='WSFCA'	AND t770.CD_BILL_ALG_ITEM='WSFC')   -- Water Supply Fixed Charge' )
     OR (T.CD_ALGORITHM='AURSC'	AND t770.CD_BILL_ALG_ITEM='PCODE') 	-- Process code
     OR (T.CD_ALGORITHM='RSCA'	AND t770.CD_BILL_ALG_ITEM='PCODE') 	-- Process code
     OR (T.CD_ALGORITHM='RVCA'	AND t770.CD_BILL_ALG_ITEM='RV' )   	-- Rateable Value
     OR (T.CD_ALGORITHM='FSR'	AND t770.CD_BILL_ALG_ITEM='FSR' )     -- Fixed Sewer Return
     OR (T.CD_ALGORITHM='GSC'	AND t770.CD_BILL_ALG_ITEM='GSC' )     -- Graduated Standing Charge'
     OR (T.CD_ALGORITHM='PPFCA'	AND t770.CD_BILL_ALG_ITEM='PPFC')   -- Portfolio Product Fixed Charge'
  )
  MINUS
  SELECT
   CD_SERV_PROV,
   CD_TARIFF,
   CD_ALGORITHM,
   CD_BILL_ALG_ITEM,
   DS_BILL_ALG_ITEM
  FROM BT_SP_TARIFF_ALGITEM);
  commit;

  l_progress := 'INSERT INTO BT_SP_TARIFF_REFTAB';
  --4. Tariff Ref Tabs used to determine charge

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SP_TARIFF_REFTAB';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SP_TARIFF_REFTAB_IDX1 UNUSABLE'; -- V 0.02

  INSERT /*+ append */                                               -- V 0.02
         INTO BT_SP_TARIFF_REFTAB (
     CD_SERV_PROV,
     CD_TARIFF,
     CD_ALGORITHM,
     TP_EQUIPMENT,
     TP_REF_TAB,
     NM_REF_TAB )
  SELECT DISTINCT
     T055.CD_SERVICE_PROV,
     T322.CD_TARIFF,
     T766.CD_ALGORITHM,
     T766.TP_EQUIPMENT,
     T769.TP_REF_TAB,
     T357.NM_REF_TAB
  FROM
     CIS.TVP055SERVICE    T055,
     CIS.TVP057TARIFF     T057,
     CIS.TVP314TARACCLSAPPL t314,
     CIS.TVP322TARIFFVER  T322,
     CIS.TVP358REFTAB     R150,
     CIS.TVP357REFTABTYPE T357,
     CIS.TVP766TVSELEMENT T766,
     CIS.TVP769TVSELMTCHRG T769
  WHERE
      T322.CD_COMPANY_SYSTEM = 'STW1'
  AND T322.CD_TAR_VER_ST_72 = 'R'
  AND(T322.dt_effective_to IS NULL
       or (t322.dt_effective_to > SYSDATE
               AND t322.dt_effective_from <> t322.dt_effective_to      )    )
  --
  AND   t057.cd_company_system=T322.CD_COMPANY_SYSTEM
  AND   t057.cd_tariff        =t322.cd_tariff
  --
  AND   t055.cd_company_system='STW1'
  AND   t055.cd_service_prov  = t057.cd_service_prov
  --
  AND  T766.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND  T766.NO_COMBINE_322    = T322.NO_COMBINE_322
  --
  AND  T769.CD_COMPANY_SYSTEM  = T766.CD_COMPANY_SYSTEM
  AND  T769.NO_COMBINE_322     = T766.NO_COMBINE_322
  AND  T769.CD_SEASON          = T766.CD_SEASON
  AND  T769.NO_SEQ             = T766.NO_SEQ
  --
  AND  T769.CD_COMPANY_SYSTEM = T357.CD_COMPANY_SYSTEM
  AND  T769.TP_REF_TAB        = T357.TP_REF_TAB
  --
  AND  T314.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
  AND  T314.CD_TARIFF         = T322.CD_TARIFF
  AND  T314.CD_REV_CLASS_150 NOT IN ('AD   ',--Assessed HH
                                     'R    ',--Measured HH
                                     'RD   ',--Unmeas Arr HH
                                     'ST   ',--Social Tariff HH
                                     'UA   ',--Unmeas Adv HH
                                     'V    ')--Vulnerable HH
  --
  AND  R150.CD_COMPANY_SYSTEM = T314.CD_COMPANY_SYSTEM
  AND  R150.TP_REF_TAB        = 150
  AND  R150.CD_REF_TAB        = T314.CD_REV_CLASS_150
  AND  R150.IND_ACTIVE       <> 'D'
  ;

  commit;
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SP_TARIFF_REFTAB_IDX1 REBUILD';  -- V 0.02

  l_progress := 'INSERT INTO BT_SP_TARIFF_EXTREF';
  --5. Tariff external references
  --   Deliberately done as a cartesian join

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SP_TARIFF_EXTREF';

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SP_TARIFF_EXTREF
  (	CD_SERV_PROV,	CD_TARIFF,  CD_ALGORITHM,
    TP_ENTITY_332,  NO_EXT_REFERENCE,  DS_EXT_REFERENCE )
  SELECT
  t.CD_SERV_PROV, t.CD_TARIFF, t.CD_ALGORITHM,
  t702.TP_ENTITY_332, t702.NO_EXT_REFERENCE, t702.DS_EXT_REFERENCE
  FROM BT_SP_tariff_alg t,
       CIS.tvp702externreftp t702
  where t702.tp_entity_332 = 'S'
  AND   t702.ds_ext_reference LIKE '201%Peak'
  AND   t702.ds_ext_reference BETWEEN '2013' AND '2099'
  and   t.cd_algorithm in ('BCAP','PCHG')
  ;

  commit;

  l_progress := 'INSERT INTO BT_SPR_TARIFF';
  -- 6. SPR tariff

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SPR_TARIFF';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX1 UNUSABLE';     -- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX2 UNUSABLE';     -- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX3 UNUSABLE';     -- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX4 UNUSABLE';  
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX5 UNUSABLE';  
  
  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SPR_TARIFF
  (CD_COMPANY_SYSTEM, NO_PROPERTY, NO_SERV_PROV, NO_ACCOUNT, NO_COMBINE_054, CD_SERV_PROV, CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET, DT_START, DT_END)
  SELECT                                                                                         -- V 0.02
  t054.CD_COMPANY_SYSTEM, t054.NO_PROPERTY, t054.NO_SERV_PROV, t054.NO_ACCOUNT, t054.NO_COMBINE_054, trim(t054.CD_SERVICE_PROV), t058.CD_TARIFF,
  t058.NO_TARIFF_GROUP, t058.NO_TARIFF_SET, t058.DT_START, t058.DT_END
  FROM   CIS.TVP058TARIFFASSGN t058,
         BT_TVP054           t054
  WHERE  t058.CD_COMPANY_SYSTEM   = t054.CD_COMPANY_SYSTEM
  and    T058.NO_COMBINE_054      = T054.NO_COMBINE_054
  AND    t058.DT_START            <> to_date('31/12/2099','dd/mm/yyyy') --nvl(t058.DT_END,to_date('31/12/2099','dd/mm/yyyy'))    -- V 0.02
  and    T058.DT_END              is null
  union -- V 0.03  
  SELECT                                                                                   
  t054.CD_COMPANY_SYSTEM, t054.NO_PROPERTY, t054.NO_SERV_PROV, t054.NO_ACCOUNT, t054.NO_COMBINE_054, trim(t054.CD_SERVICE_PROV), t058.CD_TARIFF,
  t058.NO_TARIFF_GROUP, t058.NO_TARIFF_SET, t058.DT_START, t058.DT_END
  from   CIS.TVP058TARIFFASSGN T058,
         BT_TVP054           T054,
         LU_SPID_OWC_RETAILER T2
  WHERE  t058.CD_COMPANY_SYSTEM   = t054.CD_COMPANY_SYSTEM
  AND    t058.NO_COMBINE_054      = t054.NO_COMBINE_054
  and    T058.DT_START            <> TO_DATE('31/12/2099','dd/mm/yyyy') --nvl(t058.DT_END,to_date('31/12/2099','dd/mm/yyyy'))    -- V 0.02
  --and    T058.DT_END              is null 
  and     T2.STWPROPERTYNUMBER_PK = T054.NO_PROPERTY
  union
  SELECT                                                                                         -- V 0.02
  t054.CD_COMPANY_SYSTEM, t054.NO_PROPERTY, t054.NO_SERV_PROV, t054.NO_ACCOUNT, t054.NO_COMBINE_054, trim(t054.CD_SERVICE_PROV), t058.CD_TARIFF,
  t058.NO_TARIFF_GROUP, t058.NO_TARIFF_SET, t058.DT_START, t058.DT_END
  FROM   CIS.TVP058TARIFFASSGN t058,
         BT_MISS_AG_SC           t054
  WHERE  t058.CD_COMPANY_SYSTEM   = t054.CD_COMPANY_SYSTEM
  and    T058.NO_COMBINE_054      = T054.NO_COMBINE_054
  AND    t058.DT_START            <> to_date('31/12/2099','dd/mm/yyyy') --nvl(t058.DT_END,to_date('31/12/2099','dd/mm/yyyy'))    -- V 0.02
  and    T058.DT_END              is null;

  COMMIT;
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX1 REBUILD';     -- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX2 REBUILD';     -- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX3 REBUILD';     -- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX4 REBUILD'; 
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_IDX5 REBUILD'; 
  
  IF USER = 'MOUTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'MOUTRAN', tabname=>'BT_SPR_TARIFF', cascade=>DBMS_STATS.AUTO_CASCADE);
  ELSIF USER = 'SAPTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'SAPTRAN', tabname=>'BT_SPR_TARIFF', cascade=>DBMS_STATS.AUTO_CASCADE);
  END IF;
 
  l_progress := 'INSERT INTO BT_SPR_TARIFF_ALGITEM';
  --7. SPR Tariff Algorithm item

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SPR_TARIFF_ALGITEM';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_ALGITEM_IDX1 UNUSABLE';-- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_ALGITEM_IDX2 UNUSABLE';-- V 0.02

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SPR_TARIFF_ALGITEM
  (CD_COMPANY_SYSTEM, NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, CD_TARIFF, NO_TARIFF_GROUP,NO_TARIFF_SET,
  CD_BILL_ALG_ITEM, DT_START, DT_END, DS_VALUE, DT_VALUE, NO_VALUE, TP_REF_TAB, CD_REF_TAB)
  SELECT
  CD_COMPANY_SYSTEM, NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, CD_TARIFF, NO_TARIFF_GROUP,NO_TARIFF_SET,
  CD_BILL_ALG_ITEM, DT_START, DT_END, DS_VALUE, DT_VALUE, NO_VALUE, TP_REF_TAB, CD_REF_TAB
  FROM (
  SELECT /*+ PARALLEL(t771,60) PARALLEL(tspr,60) */
         tspr.CD_COMPANY_SYSTEM, tspr.NO_PROPERTY, tspr.NO_SERV_PROV, tspr.NO_COMBINE_054, tspr.CD_SERV_PROV, tspr.CD_TARIFF, tspr.NO_TARIFF_GROUP, tspr.NO_TARIFF_SET,
         t771.CD_BILL_ALG_ITEM, t771.DT_START, t771.DT_END, t771.DS_VALUE, t771.DT_VALUE, t771.NO_VALUE, t771.TP_REF_TAB, t771.CD_REF_TAB,
         ROW_NUMBER() OVER ( PARTITION BY tspr.NO_PROPERTY, tspr.NO_COMBINE_054, tspr.CD_TARIFF, tspr.NO_TARIFF_GROUP, tspr.NO_TARIFF_SET, t771.CD_BILL_ALG_ITEM
         ORDER BY tspr.NO_PROPERTY, tspr.NO_SERV_PROV, t771.DT_START desc) AS Record_Nr
  FROM   CIS.TVP771SPRBLALGITEM t771,
         BT_SPR_TARIFF     tspr
  WHERE  t771.CD_COMPANY_SYSTEM = tspr.CD_COMPANY_SYSTEM
  AND    t771.NO_COMBINE_054    = tspr.NO_COMBINE_054  ) x
  WHERE  Record_Nr = 1;

  COMMIT;
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_ALGITEM_IDX1 REBUILD';-- V 0.02
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_ALGITEM_IDX2 REBUILD';-- V 0.02

  IF USER = 'MOUTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'MOUTRAN', tabname=>'BT_SPR_TARIFF_ALGITEM', cascade=>DBMS_STATS.AUTO_CASCADE);
  ELSIF USER = 'SAPTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'SAPTRAN', tabname=>'BT_SPR_TARIFF_ALGITEM', cascade=>DBMS_STATS.AUTO_CASCADE);
  END IF;


  l_progress := 'INSERT INTO BT_SPR_TARIFF_EXTREF';
  --8. SPR Tariff External data

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SPR_TARIFF_EXTREF';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_EXTREF_IDX1 UNUSABLE';  
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_EXTREF_IDX2 UNUSABLE';  
  
  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SPR_TARIFF_EXTREF
  (CD_COMPANY_SYSTEM, NO_PROPERTY, NO_SERV_PROV, NO_COMBINE_054, CD_SERV_PROV, CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET,
   TP_ENTITY_332, DS_EXT_REF_GROUP, NO_EXT_REFERENCE, DS_EXT_REFERENCE, NO_EXT_REF_DEBT, CD_EXT_REF, CD_WORK_ORDER,
   DT_EXT_REF, FG_EXT_REF, NO_EXT_REF, CD_REF_TAB, TP_REF_TAB)
  SELECT /*+ PARALLEL(t702,60) PARALLEL(t703,60)  PARALLEL(t486,60) PARALLEL(tspr,60) */
          tspr.CD_COMPANY_SYSTEM,
          t703.NO_PROPERTY, t703.NO_SERV_PROV,
          tspr.NO_COMBINE_054, tspr.CD_SERV_PROV, tspr.CD_TARIFF, tspr.NO_TARIFF_GROUP, tspr.NO_TARIFF_SET,
          t703.TP_ENTITY,
          t486.DS_EXT_REF_GROUP,
          t702.NO_EXT_REFERENCE,
          t702.DS_EXT_REFERENCE,
          t703.NO_EXT_REF_DEBT,
          t703.CD_EXT_REF,
          t703.CD_WORK_ORDER,
          t703.DT_EXT_REF,
          t703.FG_EXT_REF,
          t703.NO_EXT_REF,
          t703.CD_REF_TAB,
          t703.TP_REF_TAB
   FROM   CIS.TVP702EXTERNREFTP  t702,
          CIS.TVP703EXTERNREFDET t703,
          CIS.TVP486EXTERNREFGRP t486,
          BT_SPR_TARIFF          tspr
   WHERE t486.CD_COMPANY_SYSTEM = tspr.CD_COMPANY_SYSTEM
     AND t486.IND_ACTIVE        <> 'D'
     AND t486.CD_COMPANY_SYSTEM = t702.CD_COMPANY_SYSTEM
     AND t486.TP_ENTITY_332     = t702.TP_ENTITY_332
     AND t486.NO_EXT_REF_GROUP  = t702.NO_EXT_REF_GROUP
     AND t702.IND_ACTIVE       <> 'D'
     AND t702.CD_COMPANY_SYSTEM = tspr.CD_COMPANY_SYSTEM
     AND t702.TP_ENTITY_332     = t703.TP_ENTITY
     AND t703.CD_COMPANY_SYSTEM = t702.CD_COMPANY_SYSTEM
     AND t702.NO_EXT_REFERENCE = t703.NO_EXT_REFERENCE
     AND  (    (    t486.TP_ENTITY_332 = 'S'
                AND tspr.NO_PROPERTY   = t703.NO_PROPERTY
                AND tspr.NO_SERV_PROV  = t703.NO_SERV_PROV)
           OR  (    t486.TP_ENTITY_332 = 'P'
                AND tspr.NO_PROPERTY   = t703.NO_PROPERTY))
  ;

  COMMIT;
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_EXTREF_IDX1 REBUILD';   
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SPR_TARIFF_EXTREF_IDX2 REBUILD';   
  
  IF USER = 'MOUTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'MOUTRAN', tabname=>'BT_SPR_TARIFF_EXTREF', cascade=>DBMS_STATS.AUTO_CASCADE);
  ELSIF USER = 'SAPTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'SAPTRAN', tabname=>'BT_SPR_TARIFF_EXTREF', cascade=>DBMS_STATS.AUTO_CASCADE);
  END IF;

  l_progress := 'INSERT INTO BT_SP_TARIFF_SPLIT';
  --9. SPR Tariff Split

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_SP_TARIFF_SPLIT';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SP_TARIFF_SPLIT_IDX1 UNUSABLE';   
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SP_TARIFF_SPLIT_IDX2 UNUSABLE';   

  INSERT /*+ append */                                             -- V 0.02
         INTO BT_SP_TARIFF_SPLIT
  (CD_SERVICE_PROV, CD_TARIFF, CD_BILL_ALG_ITEM, CD_REF_TAB, CD_SPLIT_TARIFF, DS_SPLIT_TARIFF)
   select distinct
      T055.CD_SERVICE_PROV,
      T322.CD_TARIFF,
      T766.CD_BILL_ALG_ITEM,
      T769.CD_REF_TAB,
      TRIM(T322.CD_TARIFF)||'-'||T769.CD_REF_TAB as CD_SPLIT_TARIFF,
      REGEXP_REPLACE(TRIM(T057.DS_TARIFF),'  +',' ') ||':'|| REPLACE(R769.DS_REF_TAB,' ','') AS DS_SPLIT_TARIFF
   FROM
      CIS.TVP030EQUIPMNTTYPE t030,
      CIS.TVP055SERVICE    T055,
      CIS.TVP057TARIFF     T057,
      CIS.TVP314TARACCLSAPPL t314,
      CIS.TVP322TARIFFVER  T322,
      CIS.TVP358REFTAB     R769,
      CIS.TVP357REFTABTYPE T357,
      CIS.TVP766TVSELEMENT T766,
      CIS.TVP769TVSELMTCHRG T769
   WHERE
       T322.CD_COMPANY_SYSTEM = 'STW1'
   AND T322.CD_TAR_VER_ST_72 = 'R'
   AND T322.CD_TARIFF not like '5%'
   AND(T322.dt_effective_to is null
        or (t322.dt_effective_to > SYSDATE
                and t322.dt_effective_from <> dt_effective_to      )    )
   --
   and   t057.cd_company_system=T322.CD_COMPANY_SYSTEM
   and   t057.cd_tariff        =t322.cd_tariff
   --
   and   t055.cd_company_system='STW1'
   and   t055.cd_service_prov  = t057.cd_service_prov
   --
   AND  T766.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
   AND  T766.NO_COMBINE_322    = T322.NO_COMBINE_322
   and  T766.TP_STEP = 'M'
   AND  T766.CD_BILL_ALG_ITEM IN ('PCODE','CHGZ')
   --
   AND  T769.CD_COMPANY_SYSTEM = T766.CD_COMPANY_SYSTEM
   AND  T769.NO_COMBINE_322    = T766.NO_COMBINE_322
   AND  T769.CD_SEASON         = T766.CD_SEASON
   AND  T769.NO_SEQ            = T766.NO_SEQ
   --
   AND  T769.CD_COMPANY_SYSTEM = R769.CD_COMPANY_SYSTEM(+)
   AND  T769.CD_REF_TAB        = R769.CD_REF_TAB(+)
   AND  T769.TP_REF_TAB        = R769.TP_REF_TAB(+)
   --
   AND  T769.CD_COMPANY_SYSTEM = T357.CD_COMPANY_SYSTEM(+)
   AND  T769.TP_REF_TAB        = T357.TP_REF_TAB (+)
   --
   AND  T766.CD_COMPANY_SYSTEM = T030.CD_COMPANY_SYSTEM(+)
   AND  T766.TP_EQUIPMENT      = T030.TP_EQUIPMENT(+)
   --
   AND  T314.CD_COMPANY_SYSTEM = T322.CD_COMPANY_SYSTEM
   AND  T314.CD_TARIFF         = T322.CD_TARIFF
   AND  T314.CD_REV_CLASS_150 NOT IN ('AD   ',--Assessed HH
                                      'R    ',--Measured HH
                                      'RD   ',--Unmeas Arr HH
                                      'ST   ',--Social Tariff HH
                                      'UA   ',--Unmeas Adv HH
                                      'V    ')--Vulnerable HH
  ;

  commit;
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SP_TARIFF_SPLIT_IDX1 REBUILD'; 
  EXECUTE IMMEDIATE 'ALTER INDEX BT_SP_TARIFF_SPLIT_IDX2 REBUILD';  

  IF USER = 'MOUTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'MOUTRAN', tabname=>'BT_SP_TARIFF_SPLIT', cascade=>DBMS_STATS.AUTO_CASCADE);
  ELSIF USER = 'SAPTRAN' THEN
     DBMS_STATS.GATHER_TABLE_STATS(ownname=>'SAPTRAN', tabname=>'BT_SP_TARIFF_SPLIT', cascade=>DBMS_STATS.AUTO_CASCADE);
  END IF;


  -- write counts
  --l_progress := 'Writing Counts';

  --P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP25', 730, l_no_row_insert, 'Distinct Service Component Type during KEY_GEN stage 2');

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
     RETURN_CODE := -1;
END P_MOU_TRAN_SC_PRE;
/
show error;

--CREATE OR REPLACE PUBLIC SYNONYM P_MOU_TRAN_SC_PRE FOR P_MOU_TRAN_SC_PRE;

exit;
