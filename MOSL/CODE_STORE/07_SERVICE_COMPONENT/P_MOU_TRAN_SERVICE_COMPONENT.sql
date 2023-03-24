create or replace
PROCEDURE P_MOU_TRAN_SERVICE_COMPONENT(no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                       no_job      IN MIG_JOBREF.NO_JOB%TYPE,
                                       return_code IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Service Component MO Extract
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_SERVICE_COMPONENT.sql
--
-- Subversion $Revision: 6380 $
--
-- CREATED        : 17/03/2016
--
-- DESCRIPTION    : Procedure to create the Service Component MO Extract
--                 Will read from the temporary tables containing preproccessed data
--                 for each service component type. Then collate by property each
--                 service component type details.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 1.32      21/11/2016  D.Cheung   Add union to BT_MISS_AG_SC to get dropped services on aggregates
-- V 1.31      18/11/2016  K.Burton   Back out below - need to accommodate little m's
-- V 1.30      17/11/2016  K.Burton   UPPER TARIFFCODE_PK - fix for settlements issue
-- V 1.29      10/11/2016  S.Badhan   New mapping rules and workarounds to eliminate exception on multiple tariffs on same service provision
-- V 1.28      02/11/2016  S.Badhan   I-371. Default assessed volume to 1 if required and not supplied
-- V 1.27      26/08/2016  S.Badhan   On check of other wholesaler give error if no spid exists.
-- V 1.26      23/08/2016  D.Cheung           Amend cursor 2nd part for OWC to force in MPW service type
-- V 1.25      15/08/2016  O.Badmus           Inclusion of properties that switched to OWC IN ORDER TO GET THEIR SPIDS.
-- V 1.24      09/08/2016  S.Badhan   I-329   Set Maximum daily demand to null when not required on tariff.
--                                            For cross border tariffs check wholesaler id is set.
-- V 1.23      26/07/2016  S.Badhan   I-318   Where more than 1 version choose the active tariff.
--                                            Convert warning message to exceptions for
--                                            If volume required for Tariff but SC has no Assessed volumetric rate.
--                                            IF MPW AND Water Daily Reserved Capacity and Water Maxium Daily DEMAND REQUIRED
--                                            but SC has no values.
-- V 1.22      21/07/2016  L.Smith    I-284   To reflect change CR-030
-- V 1.21      18/07/2016  S.Badhan   CR-030  Do not report errors for duplicate tariffs.
-- V 1.20      15/07/2016  S.Badhan   I-301   Check for MPW if tariff has MPWMAXIMUMDEMANDTARIFF whether to generate warning
-- V 1.19      14/07/2016  L.Smith    I-298   Where multiple BT_TVP054 rows exist for a no_property, no_serv_prov with no end_date select latest
--                                            start_date. Note, duplicates will still be reported and filtered.
-- V 1.18      14/07/2016  S.Badhan   I-297.  Set up count of 1 WITH description 'Annual Charge' FOR UW AND US.
-- V 1.17      13/07/2016  S.Badhan   I-296.  Use the mid value between the upper and lower limit for the band for SW.
-- V 1.16      13/07/2016  L.Smith    I-284.  Reconciliation of TE accounts.
-- V 1.15      12/07/2016  S.Badhan   I-289.  Also check for fixed charges on SW.
-- V 1.14      12/07/2016  L.Smith    I-284.  Reconciliation of TE accounts.
-- V 1.13      11/07/2016  S.Badhan   CR-026. New mapping rules and workarounds to eliminate exception on multiple services on different tariffs.
-- V 1.12      11/07/2016  L.Smith    I-280.  Amended cursor cur_sc_with_tariff to prevent duplicates
-- V 1.11      08/07/2016  L.Smith    I-283.  New reconciliation measure.
-- V 1.10      05/07/2016  O.Badmus   I-273.  Updated main query to pull across properties with missing service provision
-- V 1.09      01/07/2016  S.Badhan   I-268.  Prevent duplicate error messages.
-- V 1.08      29/06/2016  S.Badhan   I-259.  Add processing to set up Assessed Water and Sewage data
--                                    and to set the SRFCWATERAREADRAINED.
-- V 1.07      24/06/2016  S.Badhan   MOSL guide line v1.5 - give warning if cross check tariff / values invalid
-- V 1.06      24/06/2016  L.Smith    I-256. Incorrectly counting rows dropped.
-- V 1.05      23/06/2016  S.Badhan   I-251. Look up MO_TARIFF to retrieve servicecomponenttype
-- V 1.04      22/06/2016  L.Smith    I248 Add CP25. Distinct count of each service component type
-- V 1.03      21/06/2016  S.Badhan   MOSL guide line v1.6 - set pipesize for unmeasured water/sewage.
--                                    Remove join to MO_TARIFF to report on missing tariffs.
-- V 1.02      17/06/2016  S.Badhan   Add trim on Tariff code when writing to table to prevent
--                                    FK constraints.
-- V 1.01      14/06/2016  D.Cheung   I-234, CR_50 - set Effective Date to Supply Point Effective From Date
-- V 0.10      10/06/2016  O.Badmus   I-230.Special Agreement Factor = 100 when SpecialAgreement Flag = 0
-- V 0.09      27/05/2016  S.Badhan   Add trim on Tariff code to retrieve all tariffs.
-- V 0.08      18/05/2016  S.Badhan   Set special agreement default values as per latest
--                                    MOSL guidelines.
-- V 0.07      17/05/2016  S.Badhan   Use table MO_TARIFF instead of LU_TARIFF.
-- V 0.06      16/05/2016  S.Badhan   ID42. Amend effective start date to 01-APR-16.
-- V 0.05      27/04/2016  S.Badhan   I-190. Report where there are duplicates of Tarff and Component type
--                                    for SPID.
-- V 0.04      20/04/2016  S.Badhan   Updates to data validation from MOSL Initial Data Upload Req V1.1
--                                    which is also the reason for System Test defect 9
-- V 0.03      18/04/2016  S.Badhan   Allow for SPID to be not found.
-- V 0.02      14/04/2016  S.Badhan   Remove columns METEREDFSTARIFFCODE , METEREDNPWTARIFFCODE ,
--                                    METEREDPWTARIFFCODE ,HWAYDRAINAGETARIFFCODE, ASSESSEDTARIFFCODE,
--                                    SRFCWATERTARRIFCODE, UNMEASUREDTARIFFCODE.
-- V 0.01      17/03/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_SERVICE_COMPONENT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_PROPERTY%TYPE;
  l_prev_tariff                 MO_TARIFF.TARIFFCODE_PK%TYPE;
  l_t314                        CIS.TVP314TARACCLSAPPL%ROWTYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_mo                          MO_SERVICE_COMPONENT%ROWTYPE;
  l_sc                          MO_SERVICE_COMPONENT_TYPE%ROWTYPE;
  l_sp                          MO_SUPPLY_POINT%ROWTYPE;
  l_as                          BT_SC_AS%ROWTYPE;
  l_age                         LU_TARIFF_SPECIAL_AGREEMENTS%ROWTYPE;
  l_spid                        LU_SPID_RANGE%ROWTYPE;
  l_mpw                         BT_SC_MPW%ROWTYPE;
  l_spt                         BT_SP_TARIFF_SPLIT%ROWTYPE;
  l_uw                          BT_SC_UW%ROWTYPE;
  l_mot                         MO_TARIFF%ROWTYPE;
  l_mpwt                        MO_TARIFF_TYPE_MPW%ROWTYPE;
  l_ast                         MO_TARIFF_TYPE_AS%ROWTYPE;
  l_awt                         MO_TARIFF_TYPE_AW%ROWTYPE;
  l_uwt                         MO_TARIFF_TYPE_UW%ROWTYPE;
  l_sw                          MO_TARIFF_TYPE_SW%ROWTYPE;
  l_mtv                         MO_TARIFF_VERSION%ROWTYPE;
  l_ust                         MO_TARIFF_TYPE_US%ROWTYPE;
  l_swt                         MO_SW_AREA_BAND%ROWTYPE;
  l_mwcap                       MO_MPW_STANDBY_MWCAPCHG%ROWTYPE;
  l_spr                         BT_SPR_TARIFF_ALGITEM%ROWTYPE;
  l_tspr                        BT_SPR_TARIFF%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_sc_measure                  NUMBER;
  l_count                       NUMBER;
  l_band                        VARCHAR2(2);
  l_srfcwaterareadrained        MO_SERVICE_COMPONENT.SRFCWATERAREADRAINED%TYPE;
  l_count_spids                 NUMBER:=0;

CURSOR cur_prop (p_no_property_start   BT_TVP054.NO_PROPERTY%TYPE,
                 p_no_property_end     BT_TVP054.NO_PROPERTY%TYPE)
    IS
      SELECT DISTINCT
             t054.NO_PROPERTY,
             t054.NO_SERV_PROV,
             t054.CD_SERVICE_PROV,
             t054.NO_COMBINE_054,
             t054.CORESPID,
             tcat.SUPPLY_POINT_CODE,
             trim(trf.CD_TARIFF) as CD_TARIFF,
             trf.NO_TARIFF_GROUP,
             trf.NO_TARIFF_SET,
             trf.DT_START,
             trf.DT_END,
             mtf.SERVICECOMPONENTTYPE,
             t054.NO_PROPERTY AS NO_PROPERTY_MASTER
      FROM   BT_TVP054           t054,
             LU_SERVICE_CATEGORY tcat,
             BT_SPR_TARIFF       trf,
            (SELECT DISTINCT substr(TARIFFCODE_PK,1,10)  AS TARIFFCODE_PK,
                    SERVICECOMPONENTTYPE
             FROM   MO_TARIFF          ) mtf
      WHERE  t054.NO_PROPERTY BETWEEN p_no_property_start AND p_no_property_end
      AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
      AND    trf.DT_END                 IS NULL
      AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
      AND    trim(trf.CD_TARIFF )       = mtf.TARIFFCODE_PK  (+)
      AND    (   (tcat.SERVICECOMPONENTTYPE  <> 'TE')
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'SU')
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'SW') -- V 1.10
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'US') -- V 1.10
              )
union
      SELECT DISTINCT -- V 1.25  
             t054.NO_PROPERTY,
             t054.NO_SERV_PROV,
             t054.CD_SERVICE_PROV,
             t054.NO_COMBINE_054,
             t054.CORESPID,
             'W' AS SUPPLY_POINT_CODE,
             OWC.TARIFF as CD_TARIFF,
             1 AS NO_TARIFF_GROUP,
             1 AS NO_TARIFF_SET,
             t054.DT_START,
             NULL AS DT_END,
             'MPW' AS SERVICECOMPONENTTYPE,
             t054.NO_PROPERTY AS NO_PROPERTY_MASTER
             from BT_OWC_CUST_SWITCHED_SUPPLIER t054
             JOIN LU_SPID_OWC_RETAILER owc ON owc.STWPROPERTYNUMBER_PK = T054.NO_PROPERTY
             join (SELECT DISTINCT 
                      substr(TARIFFCODE_PK,1,10)  AS TARIFFCODE_PK
                      ,SERVICECOMPONENTTYPE
                  FROM MO_TARIFF) mtf
                  ON OWC.TARIFF = mtf.TARIFFCODE_PK 
UNION
      SELECT DISTINCT
             BT.NO_PROPERTY,
             BT.NO_SERV_PROV,
             BT.CD_SERVICE_PROV,
             BT.NO_COMBINE_054,
             T054.CORESPID,
             tcat.SUPPLY_POINT_CODE,
             trim(trf.CD_TARIFF) as CD_TARIFF,
             trf.NO_TARIFF_GROUP,
             trf.NO_TARIFF_SET,
             trf.DT_START,
             trf.DT_END,
             mtf.SERVICECOMPONENTTYPE,
             BT.NO_PROPERTY_MASTER
      FROM   BT_MISS_AG_SC           BT,
             BT_TVP054      T054,
             LU_SERVICE_CATEGORY tcat,
             BT_SPR_TARIFF       trf,
            (SELECT DISTINCT substr(TARIFFCODE_PK,1,10)  AS TARIFFCODE_PK,
                    SERVICECOMPONENTTYPE
             FROM   MO_TARIFF          ) mtf
      WHERE  BT.NO_PROPERTY_MASTER BETWEEN p_no_property_start AND p_no_property_end
--      WHERE  BT.NO_PROPERTY_MASTER BETWEEN 1 AND 999999999
      AND    BT.NO_PROPERTY_MASTER = T054.NO_PROPERTY
      AND    trf.NO_COMBINE_054         = BT.NO_COMBINE_054
      AND    trf.DT_END                 IS NULL
      AND    tcat.TARGET_SERV_PROV_CODE = BT.CD_SERVICE_PROV
      AND    trim(trf.CD_TARIFF )       = mtf.TARIFFCODE_PK  (+)
      AND    (   (tcat.SERVICECOMPONENTTYPE  <> 'TE')
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'SU')
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'SW') -- V 1.10
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'US') -- V 1.10
              )
  ORDER BY NO_PROPERTY, NO_SERV_PROV, NO_TARIFF_GROUP, NO_TARIFF_SET;

TYPE tab_property IS TABLE OF cur_prop%ROWTYPE INDEX BY PLS_INTEGER;
t_prop  tab_property;


CURSOR cur_sc_with_tariff IS
SELECT servicecomponenttype,
       COUNT(*) type_count
  FROM (
      SELECT DISTINCT
             t054.NO_PROPERTY,
             t054.NO_SERV_PROV,
             t054.CD_SERVICE_PROV,
             t054.NO_COMBINE_054,
             t054.CORESPID,
             tcat.SUPPLY_POINT_CODE,
             trim(trf.CD_TARIFF) as CD_TARIFF,
             trf.NO_TARIFF_GROUP,
             trf.NO_TARIFF_SET,
             trf.DT_START,
             trf.DT_END,
             mtf.SERVICECOMPONENTTYPE
      FROM   BT_TVP054           t054,
             LU_SERVICE_CATEGORY tcat,
             BT_SPR_TARIFF       trf,
            (SELECT DISTINCT substr(TARIFFCODE_PK,1,10)  AS TARIFFCODE_PK,
                    SERVICECOMPONENTTYPE
             FROM   MO_TARIFF          ) mtf
    WHERE  trf.NO_COMBINE_054         = t054.NO_COMBINE_054
      AND    trf.DT_END                 IS NULL
      AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
      AND    trim(trf.CD_TARIFF )       = mtf.TARIFFCODE_PK  (+)
      AND    (   (tcat.SERVICECOMPONENTTYPE  <> 'TE')
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'SU')
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'SW') -- V 1.10
              or (TCAT.SERVICECOMPONENTTYPE  = 'TE' and  TARGET_SERV_PROV_CODE = 'US') -- V 1.10
              )
)
 GROUP BY servicecomponenttype
 ORDER BY servicecomponenttype;

TYPE tab_sc_with_tariff IS TABLE OF cur_sc_with_tariff%ROWTYPE INDEX BY PLS_INTEGER;
t_sc_with_tariff  tab_sc_with_tariff;

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
   l_prev_tariff := NULL;

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

   l_progress := 'processing';

   -- any errors set return code and exit out

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- start processing all records for range supplied

  OPEN cur_prop (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX);

  l_progress := 'loop processing';

  LOOP

    FETCH cur_prop BULK COLLECT INTO t_prop LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_prop.COUNT
    LOOP

      l_err.TXT_KEY := t_prop(i).NO_PROPERTY || ',' || t_prop(i).NO_SERV_PROV || ',' || t_prop(i).SERVICECOMPONENTTYPE || ',' || t_prop(i).CD_TARIFF
                      || ',' || t_prop(i).NO_TARIFF_GROUP || t_prop(i).NO_TARIFF_SET;

      l_mo := NULL;
      l_rec_written := TRUE;

          -- keep count of distinct property
         l_no_row_read := l_no_row_read + 1;

         l_mo.SERVICECOMPONENTREF_PK := t_prop(i).NO_PROPERTY || t_prop(i).NO_SERV_PROV;
         l_mo.TARIFFCODE_PK	:= t_prop(i).CD_TARIFF;

         -- get supply point id

         l_progress := 'SELECT LU_SPID_RANGE';
         BEGIN
           SELECT SPID_PK
           INTO   l_spid.SPID_PK
           FROM   LU_SPID_RANGE
           WHERE  CORESPID_PK     = t_prop(i).CORESPID
           AND    SERVICECATEGORY = t_prop(i).SUPPLY_POINT_CODE;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_spid.SPID_PK := null;
         END;

         l_mo.SPID_PK := l_spid.SPID_PK;
         l_mo.DPID_PK := null;
         --l_mo.STWPROPERTYNUMBER_PK := t_prop(i).NO_PROPERTY;
         l_mo.STWPROPERTYNUMBER_PK := t_prop(i).NO_PROPERTY_MASTER;
         l_mo.STWSERVICETYPE	:= t_prop(i).CD_SERVICE_PROV;

         -- Get service component type for tariff

         l_progress := 'SELECT BT_SP_TARIFF_SPLIT';
         BEGIN
           SELECT TRIM(CD_SPLIT_TARIFF)
           INTO   l_spt.CD_SPLIT_TARIFF
           FROM   BT_SP_TARIFF_SPLIT     spt,
                  BT_SPR_TARIFF_ALGITEM  alg
           WHERE  alg.NO_COMBINE_054   = t_prop(i).NO_COMBINE_054
           AND    alg.CD_TARIFF        = spt.CD_TARIFF
           AND    alg.CD_BILL_ALG_ITEM = spt.CD_BILL_ALG_ITEM
           AND    alg.CD_REF_TAB       = spt.CD_REF_TAB;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_spt.CD_SPLIT_TARIFF := null;
         END;

        IF l_spt.CD_SPLIT_TARIFF IS NOT NULL THEN
           l_mo.TARIFFCODE_PK := l_spt.CD_SPLIT_TARIFF;
        END IF;

        l_progress := 'SELECT MO_TARIFF';
        BEGIN
          SELECT SERVICECOMPONENTTYPE
          INTO   l_mot.SERVICECOMPONENTTYPE
          FROM   MO_TARIFF
          WHERE  TARIFFCODE_PK = l_mo.TARIFFCODE_PK;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
             l_mot.SERVICECOMPONENTTYPE := NULL;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Invalid Tariff',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
             l_no_row_exp := l_no_row_exp + 1;
             l_rec_written := FALSE;
        END;

        l_mo.SERVICECOMPONENTTYPE := l_mot.SERVICECOMPONENTTYPE;
        t_prop(i).SERVICECOMPONENTTYPE := l_mo.SERVICECOMPONENTTYPE;

        -- Tariff version

        IF l_rec_written THEN
           l_progress := 'SELECT MO_TARIFF_VERSION';
           SELECT mtv.TARIFF_VERSION_PK
           INTO   l_mtv.TARIFF_VERSION_PK
           FROM   MO_TARIFF_VERSION   mtv
           WHERE  mtv.TARIFFCODE_PK     = l_mo.TARIFFCODE_PK
           AND    mtv.TARIFFVERSION     = (SELECT MAX(TARIFFVERSION)
                                           FROM   MO_TARIFF_VERSION
                                           WHERE  TARIFFCODE_PK = mtv.TARIFFCODE_PK
                                           AND    TARIFFSTATUS  = 'ACTIVE');
        ELSE
           l_mtv.TARIFF_VERSION_PK := 0;
        END IF;

         -- State of service component

         l_progress := 'SELECT TVP056SERVPROV';
         SELECT CASE  WHEN ST_SERV_PROV IN ('A', 'C', 'G')
                 THEN 1
                 ELSE 0
                END AS D2076_ACTIVE,
                DT_START
         INTO   l_mo.SERVICECOMPONENTENABLED,
                l_mo.EFFECTIVEFROMDATE
         FROM   CIS.TVP056SERVPROV
         WHERE  NO_PROPERTY  = t_prop(i).NO_PROPERTY
         AND    NO_SERV_PROV = t_prop(i).NO_SERV_PROV;

         SELECT MAX(NVL(SUPPLYPOINTEFFECTIVEFROMDATE, to_date('01/04/2016','dd/mm/yyyy')))
         INTO  l_mo.EFFECTIVEFROMDATE
         FROM MO_SUPPLY_POINT
         WHERE SPID_PK = l_mo.SPID_PK;

         -- find any special agreements

         l_progress := 'SELECT LU_TARIFF_SPECIAL_AGREEMENTS';
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
              l_age.SPECIAL_AGREEMENT_FLAG := 'N';
         END;

         IF l_age.SPECIAL_AGREEMENT_FLAG = 'Y' THEN
            l_mo.SPECIALAGREEMENTFLAG := 1;
            l_mo.SPECIALAGREEMENTFACTOR := l_age.SPECIAL_AGREEMENT_FACTOR;
            l_mo.SPECIALAGREEMENTREF := l_age.OFWAT_REFERENCE_NUMBER;
         ELSE
            l_mo.SPECIALAGREEMENTFLAG := 0;
            l_mo.SPECIALAGREEMENTFACTOR := 100;
            l_mo.SPECIALAGREEMENTREF := 'NA';
         END IF;

      -- Metered Sewage (MS)
      IF l_rec_written THEN
         IF t_prop(i).SERVICECOMPONENTTYPE = 'MS' THEN

            -- CR-026.  If Tariffs MS001 and NoMSC found. Use MS001 and reject NoMsc

            IF l_mo.TARIFFCODE_PK = '1STW-NoMSC'  THEN
               l_progress := 'SELECT EXTRA TARIFF - MS';
               BEGIN
                  SELECT trim(trf.CD_TARIFF)
                  INTO   l_tspr.CD_TARIFF
                  FROM   BT_TVP054           t054,
                         LU_SERVICE_CATEGORY tcat,
                         BT_SPR_TARIFF       trf
                  WHERE  t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                  AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
                  AND    trf.DT_END                 is null
                  AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                  AND    trim(trf.CD_TARIFF)        = '1STW-MS001'
                  AND    tcat.SERVICECOMPONENTTYPE  = 'MS'
                  AND    ROWNUM = 1;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_tspr.CD_TARIFF := null;
               END;

               IF l_tspr.CD_TARIFF = '1STW-MS001' THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Record dropped, other Tariff 1STW-MS001 used instead',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF;
            
            -- If services S (Meas Used Water) and SW (TE Other Used Water ) exist for same property choose SW tariff

            IF t_prop(i).CD_SERVICE_PROV = 'S'  THEN            
               l_progress := 'SELECT SW SERVICE PROVISION';
               BEGIN
                  SELECT trim(trf.CD_TARIFF)
                  INTO   l_tspr.CD_TARIFF
                  FROM   BT_TVP054           t054,
                         LU_SERVICE_CATEGORY tcat,
                         BT_SPR_TARIFF       trf
                  WHERE  t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                  AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
                  AND    trf.DT_END                 is null
                  AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                  AND    CD_SERVICE_PROV            = 'SW' 
                  AND    ROWNUM = 1;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_tspr.CD_TARIFF := null;
               END;

               IF l_tspr.CD_TARIFF is not null THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Record dropped, other Service Provision SW tariff used instead',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF; 

                        
          END IF;

      END IF;

      -- Metered Potable Water (MPW)

      l_mpw.D2079_MAXDAILYDMD := null;
      l_mpw.D2080_DLYRESVDCAP := NULL;
      l_mpw.D2056_TARIFFCODE := null;

      IF l_rec_written THEN
          IF t_prop(i).SERVICECOMPONENTTYPE = 'MPW' THEN

            -- Check Tariff values if max demand value required

            l_progress := 'SELECT MO_TARIFF_TYPE_MPW';
            SELECT TARIFF_TYPE_PK,
                   MPWMAXIMUMDEMANDTARIFF
            INTO   l_mpwt.TARIFF_TYPE_PK,
                   l_mpwt.MPWMAXIMUMDEMANDTARIFF
            FROM   MO_TARIFF_TYPE_MPW
            WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;

            l_progress := 'SELECT BT_SC_MPW';
            BEGIN
              SELECT D2079_MAXDAILYDMD,
                     D2080_DLYRESVDCAP,
                     D2056_TARIFFCODE
              INTO   l_mpw.D2079_MAXDAILYDMD,
                     l_mpw.D2080_DLYRESVDCAP,
                     l_mpw.D2056_TARIFFCODE
              FROM   BT_SC_MPW
              WHERE  NO_COMBINE_054 = t_prop(i).NO_COMBINE_054;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 l_mpw.D2079_MAXDAILYDMD := NULL;
                 l_mpw.D2080_DLYRESVDCAP := NULL;
            END;

            IF l_mpwt.MPWMAXIMUMDEMANDTARIFF IS NULL THEN
               l_mpw.D2079_MAXDAILYDMD := NULL;
            ELSE
               IF l_mpw.D2079_MAXDAILYDMD IS NULL THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'D2079_MAXDAILYDMD has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_exp := l_no_row_exp + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF;

            -- check that for standby capacity charges we have provided mandatory single data

            l_count := 1;
            BEGIN
              SELECT CHARGE
              INTO   l_mwcap.CHARGE
              FROM   MO_MPW_STANDBY_MWCAPCHG
              WHERE  TARIFF_TYPE_PK = l_mpwt.TARIFF_TYPE_PK
              AND    ROWNUM = 1;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 l_count := 0;
            END;

            IF l_count > 0 THEN
               IF l_mpw.D2080_DLYRESVDCAP IS NULL THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'D2080_DLYRESVDCAP has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_exp := l_no_row_exp + 1;
                  l_rec_written := FALSE;
               END IF;
            ELSE
               l_mpw.D2080_DLYRESVDCAP := NULL;
            END IF;

            -- CR-026.  If Tariffs MW001 and MW003 found. Use MW003 and reject MW001

            IF l_mo.TARIFFCODE_PK = '1STW-MW001'  THEN
               l_progress := 'SELECT EXTRA TARIFF - MS 1';
               l_tspr.CD_TARIFF := NULL;
               BEGIN
                  SELECT trim(trf.CD_TARIFF)
                  INTO   l_tspr.CD_TARIFF
                  FROM   BT_TVP054           t054,
                         LU_SERVICE_CATEGORY tcat,
                         BT_SPR_TARIFF       trf
                  WHERE  t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                  AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
                  AND    trf.DT_END                 is null
                  AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                  AND    trim(trf.CD_TARIFF)        = '1STW-MW003'
                  AND    tcat.SERVICECOMPONENTTYPE  = 'MPW'
                  AND    ROWNUM                     = 1;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_tspr.CD_TARIFF := null;
               END;

               IF l_tspr.CD_TARIFF = '1STW-MW003' THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Record dropped, other Tariff 1STW-MW003 used instead',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF;

           -- If Tariffs 1STW-SUST and 1STW-SUSC found. Use 1STW-SUST and reject 1STW-SUSC

            IF l_mo.TARIFFCODE_PK = '1STW-SUSC'  THEN
               l_progress := 'SELECT EXTRA TARIFF - MS 2';
               BEGIN
                  SELECT trim(trf.CD_TARIFF)
                  INTO   l_tspr.CD_TARIFF
                  FROM   BT_TVP054           t054,
                         LU_SERVICE_CATEGORY tcat,
                         BT_SPR_TARIFF       trf
                  WHERE  t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                  AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
                  AND    trf.DT_END                 is null
                  AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                  AND    trim(trf.CD_TARIFF)        = '1STW-SUST'
                  AND    tcat.SERVICECOMPONENTTYPE  = 'MPW'
                  AND    ROWNUM = 1;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_tspr.CD_TARIFF := null;
               END;

               IF l_tspr.CD_TARIFF = '1STW-SUST' THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Record dropped, other Tariff 1STW-SUST used instead',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF;

           -- If Tariffs 1STW-LUST and 1STW-LUSC found. Use 1STW-LUST and reject 1STW-LUSC

            IF l_mo.TARIFFCODE_PK = '1STW-LUSC'  THEN
               l_progress := 'SELECT EXTRA TARIFF - MS 3';
               BEGIN
                  SELECT trim(trf.CD_TARIFF)
                  INTO   l_tspr.CD_TARIFF
                  FROM   BT_TVP054           t054,
                         LU_SERVICE_CATEGORY tcat,
                         BT_SPR_TARIFF       trf
                  WHERE  t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                  AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
                  AND    trf.DT_END                 is null
                  AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                  AND    trim(trf.CD_TARIFF)        = '1STW-LUST'
                  AND    tcat.SERVICECOMPONENTTYPE  = 'MPW'
                  AND    ROWNUM = 1;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_tspr.CD_TARIFF := null;
               END;

               IF l_tspr.CD_TARIFF = '1STW-LUST' THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Record dropped, other Tariff 1STW-LUST used instead',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF; 
            
           -- If Tariffs 1STW-IUST and 1STW-IUSC found. Use 1STW-IUST and reject 1STW-IUSC

            IF l_mo.TARIFFCODE_PK = '1STW-IUSC'  THEN
               l_progress := 'SELECT EXTRA TARIFF - MS 4';
               BEGIN
                  SELECT trim(trf.CD_TARIFF)
                  INTO   l_tspr.CD_TARIFF
                  FROM   BT_TVP054           t054,
                         LU_SERVICE_CATEGORY tcat,
                         BT_SPR_TARIFF       trf
                  WHERE  t054.NO_PROPERTY           = t_prop(i).NO_PROPERTY
                  AND    trf.NO_COMBINE_054         = t054.NO_COMBINE_054
                  AND    trf.DT_END                 is null
                  AND    tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
                  AND    trim(trf.CD_TARIFF)        = '1STW-IUST'
                  AND    tcat.SERVICECOMPONENTTYPE  = 'MPW'
                  AND    ROWNUM = 1;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_tspr.CD_TARIFF := null;
               END;

               IF l_tspr.CD_TARIFF = '1STW-IUST' THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Record dropped, other Tariff 1STW-IUST used instead',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_rec_written := FALSE;
               END IF;
            END IF; 
         
         END IF;
      END IF;

      l_mo.METEREDPWMAXDAILYDEMAND := l_mpw.D2079_MAXDAILYDMD;
      l_mo.DAILYRESERVEDCAPACITY := l_mpw.D2080_DLYRESVDCAP;

      -- Metered Non Potable Water (MNPW)

      l_mo.METEREDNPWMAXDAILYDEMAND	:= NULL;
      l_mo.METEREDNPWDAILYRESVDCAPACITY := null;

      -- Metered Foul Sewage (MS)

      l_mo.METEREDFSMAXDAILYDEMAND := null;
      l_mo.METEREDFSDAILYRESVDCAPACITY := NULL;


      -- Highway Drainage (HD)

      l_mo.HWAYSURFACEAREA	:= null;
      l_mo.HWAYCOMMUNITYCONFLAG := 0;

      -- Assessed Sewage(AS) or Assessed Water(AW)

      l_mo.ASSESSEDDVOLUMETRICRATE	:= null;
      l_mo.ASSESSEDCHARGEMETERSIZE := null;
      l_mo.ASSESSEDTARIFBAND := null;

      IF l_rec_written THEN
         IF t_prop(i).SERVICECOMPONENTTYPE = 'AS' THEN

           -- check tariff if volume required
            l_progress := 'SELECT MO_TARIFF_TYPE_AS';
            SELECT TARIFF_TYPE_PK,
                   ASFIXEDCHARGE,
                   ASVOLMETCHARGE
            INTO   l_ast.TARIFF_TYPE_PK,
                   l_ast.ASFIXEDCHARGE,
                   l_ast.ASVOLMETCHARGE
            FROM   MO_TARIFF_TYPE_AS
            WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;

            IF nvl(l_ast.ASVOLMETCHARGE,0) = 0 THEN
               l_mo.ASSESSEDDVOLUMETRICRATE := null;
            ELSE
               l_progress := 'SELECT BT_SC_AS';
               BEGIN
                  SELECT NO_VALUE
                  INTO   l_as.NO_VALUE
                  FROM   BT_SC_AS
                  WHERE  NO_COMBINE_054  = t_prop(i).NO_COMBINE_054
                  AND    TRIM(CD_TARIFF) = t_prop(i).CD_TARIFF
                  AND    NO_TARIFF_GROUP = t_prop(i).NO_TARIFF_GROUP
                  AND    NO_TARIFF_SET   = t_prop(i).NO_TARIFF_SET;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_as.NO_VALUE := 0;
               END;
               l_mo.ASSESSEDDVOLUMETRICRATE	:= l_as.NO_VALUE;

               IF nvl(l_mo.ASSESSEDDVOLUMETRICRATE,0) = 0 THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'ASSESSEDDVOLUMETRICRATE has no value, defaulted to 1',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_mo.ASSESSEDDVOLUMETRICRATE := 1;
               END IF;

            END IF;

         ELSIF t_prop(i).SERVICECOMPONENTTYPE = 'AW' THEN

            -- check Tariff if volumetric rate required
            l_progress := 'SELECT MO_TARIFF_TYPE_AW';
            SELECT TARIFF_TYPE_PK,
                   AWFIXEDCHARGE,
                   AWVOLUMETRICCHARGE
            INTO   l_awt.TARIFF_TYPE_PK,
                   l_awt.AWFIXEDCHARGE,
                   l_awt.AWVOLUMETRICCHARGE
            FROM   MO_TARIFF_TYPE_AW
            WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;

            IF nvl(l_awt.AWVOLUMETRICCHARGE,0) = 0 THEN
               l_mo.ASSESSEDDVOLUMETRICRATE := null;
            ELSE
               l_progress := 'SELECT BT_SC_UW';
               BEGIN
                   SELECT VOLUMETRICRATE
                   INTO   l_uw.VOLUMETRICRATE
                   FROM   BT_SC_UW
                   WHERE  NO_COMBINE_054  = t_prop(i).NO_COMBINE_054
                   AND    TRIM(D2067_TARIFFCODE) = t_prop(i).CD_TARIFF
                   AND    NO_TARIFF_GROUP = t_prop(i).NO_TARIFF_GROUP
                   AND    NO_TARIFF_SET   = t_prop(i).NO_TARIFF_SET;
               EXCEPTION
               WHEN NO_DATA_FOUND THEN
                    l_uw.VOLUMETRICRATE := 0;
               END;

               l_mo.ASSESSEDDVOLUMETRICRATE	:= l_uw.VOLUMETRICRATE;
               IF nvl(l_mo.ASSESSEDDVOLUMETRICRATE,0) = 0 THEN
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'ASSESSEDDVOLUMETRICRATE has no value, defaulted to 1',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                  l_no_row_war := l_no_row_war + 1;
                  l_mo.ASSESSEDDVOLUMETRICRATE := 1;
               END IF;

            END IF;

         END IF;
      END IF;

      -- Surface Water (SW)

      l_mo.SRFCWATERAREADRAINED	:= 0;
      IF l_rec_written THEN
          IF t_prop(i).SERVICECOMPONENTTYPE = 'SW' THEN
             l_progress := 'SELECT BT_SPR_TARIFF_ALGITEM ';

             -- check if tariff has a rateable value

             l_progress := 'SELECT MO_TARIFF_TYPE_SW';
             SELECT TARIFF_TYPE_PK,
                    SWFIXEDCHARGE,
                    SWRVPOUNDAGE
             INTO   l_sw.TARIFF_TYPE_PK,
                    l_sw.SWFIXEDCHARGE,
                    l_sw.SWRVPOUNDAGE
             FROM   MO_TARIFF_TYPE_SW sw
             WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;

             IF nvl(l_sw.SWRVPOUNDAGE,0)  > 0 THEN
                l_mo.SRFCWATERAREADRAINED	:= 0;
             END IF;

             -- no rateable charge then get band charge

             IF nvl(l_sw.SWRVPOUNDAGE,0)  = 0 THEN
                l_srfcwaterareadrained := NULL;
                BEGIN
                  SELECT  REPLACE( REPLACE ( SUBSTR(ds_ref_TAB, INSTR(ds_ref_TAB,' ',-1)), 'm2'), ',') AS val
                  INTO    l_srfcwaterareadrained
                  FROM    BT_SPR_TARIFF_ALGITEM alg,
                          CIS.TVP358REFTAB      t358
                  WHERE   TRIM(alg.CD_TARIFF)  = t_prop(i).CD_TARIFF
                  AND     alg.CD_BILL_ALG_ITEM = 'USACB'
                  AND     alg.NO_COMBINE_054   = t_prop(i).NO_COMBINE_054
                  AND     t358.TP_REF_TAB      = alg.TP_REF_TAB
                  AND     t358.CD_REF_TAB      = alg.CD_REF_TAB;
                EXCEPTION
                WHEN NO_DATA_FOUND THEN
                     l_srfcwaterareadrained := NULL;
                END;

                IF l_srfcwaterareadrained IS NOT NULL THEN
                   l_mo.SRFCWATERAREADRAINED := l_srfcwaterareadrained;
                END IF;

                -- if value not on Target derive from area band

                IF l_srfcwaterareadrained IS NULL THEN
                   l_progress := 'SELECT MO_SW_AREA_BAND';

                   l_band := substr(t_prop(i).CD_TARIFF, LENGTH(t_prop(i).CD_TARIFF) - 1, 2);
                   l_swt.BAND := NULL;

                   IF LENGTH(TRIM(TRANSLATE(l_band, ' +-.0123456789', ' '))) IS NULL THEN
                      BEGIN
                         SELECT BAND,
                                LOWERAREA,
                                UPPERAREA
                         INTO   l_swt.BAND,
                                l_swt.LOWERAREA,
                                l_swt.UPPERAREA
                         FROM   MO_SW_AREA_BAND
                         WHERE  TARIFF_TYPE_PK = l_sw.TARIFF_TYPE_PK
                         AND    BAND           = l_band;
                      EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                            l_swt.BAND := NULL;
                      END;
                   END IF;

                   IF l_swt.BAND IS NULL THEN
                       IF nvl(l_sw.SWFIXEDCHARGE,0)  = 0 THEN
                          P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'SRFCWATERAREADRAINED has no value',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                          l_no_row_war := l_no_row_war + 1;
                      END IF;
                      l_mo.SRFCWATERAREADRAINED := 0;
                   ELSE
                      IF l_swt.UPPERAREA IS NOT NULL THEN
                         l_mo.SRFCWATERAREADRAINED := ((l_swt.UPPERAREA - l_swt.LOWERAREA )/2) + l_swt.LOWERAREA;
                      ELSE
                         l_mo.SRFCWATERAREADRAINED := l_swt.LOWERAREA;
                      END IF;
                   END IF;

                 END IF;
             END IF;

           END IF;
      END IF;

      l_mo.SRFCWATERCOMMUNITYCONFLAG := 0;

      -- Unmeasured Water(UW)

      l_uw.D2018_TYPEACOUNT := null;
      l_uw.D2019_TYPEBCOUNT := null;
      l_uw.D2020_TYPECCOUNT := null;
      l_uw.D2021_TYPEDCOUNT := null;
      l_uw.D2022_TYPEECOUNT := null;
      l_uw.D2024_TYPEFCOUNT := null;
      l_uw.D2046_TYPEGCOUNT := null;
      l_uw.D2048_TYPEHCOUNT := null;
      l_uw.D2058_TYPEADESCRIPTION := null;
      l_uw.D2059_TYPEBDESCRIPTION := NULL;
      l_uw.D2060_TYPECDESCRIPTION := NULL;
      l_uw.D2061_TYPEDDESCRIPTION := null;
      l_uw.D2062_TYPEEDESCRIPTION := null;
      l_uw.D2064_TYPEFDESCRIPTION := null;
      l_uw.D2065_TYPEGDESCRIPTION := null;
      l_uw.D2069_TYPEHDESCRIPTION := NULL;
      l_uw.D2067_TARIFFCODE := NULL;

      IF l_rec_written THEN
        IF t_prop(i).SERVICECOMPONENTTYPE = 'UW' THEN
 --          l_progress := 'SELECT BT_SC_UW ';

           -- target data
--           BEGIN
--               SELECT D2018_TYPEACOUNT,
--                      D2019_TYPEBCOUNT,
--                      D2020_TYPECCOUNT,
--                      D2021_TYPEDCOUNT,
--                      D2022_TYPEECOUNT,
--                      D2024_TYPEFCOUNT,
--                      D2046_TYPEGCOUNT,
--                      D2048_TYPEHCOUNT,
--                      D2058_TYPEADESCRIPTION,
--                      D2059_TYPEBDESCRIPTION,
--                      D2060_TYPECDESCRIPTION,
--                      D2061_TYPEDDESCRIPTION,
--                      D2062_TYPEEDESCRIPTION,
--                      D2064_TYPEFDESCRIPTION,
--                      D2065_TYPEGDESCRIPTION,
--                      D2069_TYPEHDESCRIPTION,
--                      D2067_TARIFFCODE
--               INTO  l_uw.D2018_TYPEACOUNT,
--                     l_uw.D2019_TYPEBCOUNT,
--                     l_uw.D2020_TYPECCOUNT,
--                     l_uw.D2021_TYPEDCOUNT,
--                     l_uw.D2022_TYPEECOUNT,
--                     l_uw.D2024_TYPEFCOUNT,
--                     l_uw.D2046_TYPEGCOUNT,
--                     l_uw.D2048_TYPEHCOUNT,
--                     l_uw.D2058_TYPEADESCRIPTION,
--                     l_uw.D2059_TYPEBDESCRIPTION,
--                     l_uw.D2060_TYPECDESCRIPTION,
--                     l_uw.D2061_TYPEDDESCRIPTION,
--                     l_uw.D2062_TYPEEDESCRIPTION,
--                     l_uw.D2064_TYPEFDESCRIPTION,
--                     l_uw.D2065_TYPEGDESCRIPTION,
--                     l_uw.D2069_TYPEHDESCRIPTION,
--                     l_uw.D2067_TARIFFCODE
--              FROM   BT_SC_UW
--              WHERE  NO_COMBINE_054  = t_prop(i).NO_COMBINE_054
--              AND    NO_TARIFF_GROUP = t_prop(i).NO_TARIFF_GROUP
--              AND    NO_TARIFF_SET   = t_prop(i).NO_TARIFF_SET;
--           EXCEPTION
--           WHEN NO_DATA_FOUND THEN
--                null;
--           END;

            -- Check Tariff values
            l_progress := 'SELECT MO_TARIFF_TYPE_UW';
            SELECT TARIFF_TYPE_PK,
                   UWFIXEDCHARGE,
                   UWRVPOUNDAGE,
                   UWRVTHRESHOLD,
                   UWRVMAXCHARGE,
                   UWRVMINCHARGE,
                   UWMISCTYPEACHARGE,
                   UWMISCTYPEBCHARGE,
                   UWMISCTYPECCHARGE,
                   UWMISCTYPEDCHARGE,
                   UWMISCTYPEECHARGE,
                   UWMISCTYPEFCHARGE,
                   UWMISCTYPEGCHARGE,
                   UWMISCTYPEHCHARGE
            INTO   l_uwt.TARIFF_TYPE_PK,
                   l_uwt.UWFIXEDCHARGE,
                   l_uwt.UWRVPOUNDAGE,
                   l_uwt.UWRVTHRESHOLD,
                   l_uwt.UWRVMAXCHARGE,
                   l_uwt.UWRVMINCHARGE,
                   l_uwt.UWMISCTYPEACHARGE,
                   l_uwt.UWMISCTYPEBCHARGE,
                   l_uwt.UWMISCTYPECCHARGE,
                   l_uwt.UWMISCTYPEDCHARGE,
                   l_uwt.UWMISCTYPEECHARGE,
                   l_uwt.UWMISCTYPEFCHARGE,
                   l_uwt.UWMISCTYPEGCHARGE,
                   l_uwt.UWMISCTYPEHCHARGE
            FROM   MO_TARIFF_TYPE_UW
            WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;

            IF l_uwt.UWMISCTYPEACHARGE IS NULL THEN
               l_uw.D2018_TYPEACOUNT := NULL;
               l_uw.D2058_TYPEADESCRIPTION := NULL;
            ELSE
               l_uw.D2018_TYPEACOUNT := 1;
               l_uw.D2058_TYPEADESCRIPTION := 'Annual Charge';
            END IF;

            IF l_uwt.UWMISCTYPEBCHARGE IS NULL THEN
               l_uw.D2019_TYPEBCOUNT := NULL;
               l_uw.D2059_TYPEBDESCRIPTION := null;
            ELSE
               l_uw.D2019_TYPEBCOUNT := 1;
               l_uw.D2059_TYPEBDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_uwt.UWMISCTYPECCHARGE IS NULL THEN
               l_uw.D2020_TYPECCOUNT := NULL;
               l_uw.D2060_TYPECDESCRIPTION := null;
            ELSE
               l_uw.D2020_TYPECCOUNT := 1;
               l_uw.D2060_TYPECDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_uwt.UWMISCTYPEDCHARGE IS NULL THEN
               l_uw.D2021_TYPEDCOUNT := NULL;
               l_uw.D2061_TYPEDDESCRIPTION := null;
            ELSE
               l_uw.D2021_TYPEDCOUNT := 1;
               l_uw.D2061_TYPEDDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_uwt.UWMISCTYPEECHARGE IS NULL THEN
               l_uw.D2022_TYPEECOUNT := NULL;
               l_uw.D2062_TYPEEDESCRIPTION := NULL;
            ELSE
               l_uw.D2022_TYPEECOUNT := 1;
               l_uw.D2062_TYPEEDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_uwt.UWMISCTYPEFCHARGE IS NULL THEN
               l_uw.D2024_TYPEFCOUNT := NULL;
               l_uw.D2064_TYPEFDESCRIPTION := null;
            ELSE
               l_uw.D2024_TYPEFCOUNT := 1;
               l_uw.D2064_TYPEFDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_uwt.UWMISCTYPEGCHARGE IS NULL THEN
               l_uw.D2046_TYPEGCOUNT := NULL;
               l_uw.D2065_TYPEGDESCRIPTION := NULL;
            ELSE
               l_uw.D2046_TYPEGCOUNT := 1;
               l_uw.D2065_TYPEGDESCRIPTION := 'Annual Charge';
            END IF;

           IF l_uwt.UWMISCTYPEHCHARGE IS NULL THEN
              l_uw.D2048_TYPEHCOUNT := NULL;
              l_uw.D2069_TYPEHDESCRIPTION := null;
           ELSE
              l_uw.D2048_TYPEHCOUNT := 1;
              l_uw.D2069_TYPEHDESCRIPTION := 'Annual Charge';
           END IF;

        END IF;

      END IF;

      -- Unmeasured Sewage(US)

      IF l_rec_written THEN
         IF t_prop(i).SERVICECOMPONENTTYPE = 'US' THEN
            l_progress := 'SELECT MO_TARIFF_TYPE_US';
            SELECT TARIFF_TYPE_PK,
                   USFIXEDCHARGE,
                   USRVPOUNDAGE,
                   USRVTHRESHOLD,
                   USMISCTYPEACHARGE,
                   USMISCTYPEBCHARGE,
                   USMISCTYPECCHARGE,
                   USMISCTYPEDCHARGE,
                   USMISCTYPEECHARGE,
                   USMISCTYPEFCHARGE,
                   USMISCTYPEGCHARGE,
                   USMISCTYPEHCHARGE
            INTO   l_ust.TARIFF_TYPE_PK,
                   l_ust.USFIXEDCHARGE,
                   l_ust.USRVPOUNDAGE,
                   l_ust.USRVTHRESHOLD,
                   l_ust.USMISCTYPEACHARGE,
                   l_ust.USMISCTYPEBCHARGE,
                   l_ust.USMISCTYPECCHARGE,
                   l_ust.USMISCTYPEDCHARGE,
                   l_ust.USMISCTYPEECHARGE,
                   l_ust.USMISCTYPEFCHARGE,
                   l_ust.USMISCTYPEGCHARGE,
                   l_ust.USMISCTYPEHCHARGE
            FROM   MO_TARIFF_TYPE_US
            WHERE  TARIFF_VERSION_PK  = l_mtv.TARIFF_VERSION_PK;

            IF l_ust.USMISCTYPEACHARGE IS NULL THEN
               l_uw.D2018_TYPEACOUNT := NULL;
               l_uw.D2058_TYPEADESCRIPTION := null;
            ELSE
               l_uw.D2018_TYPEACOUNT := 1;
               l_uw.D2058_TYPEADESCRIPTION := 'Annual Charge';
            END IF;

            IF l_ust.USMISCTYPEBCHARGE IS NULL THEN
               l_uw.D2019_TYPEBCOUNT := NULL;
               l_uw.D2059_TYPEBDESCRIPTION := null;
            ELSE
               l_uw.D2019_TYPEBCOUNT := 1;
               l_uw.D2059_TYPEBDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_ust.USMISCTYPECCHARGE IS NULL THEN
               l_uw.D2020_TYPECCOUNT := NULL;
               l_uw.D2060_TYPECDESCRIPTION := null;
            ELSE
               l_uw.D2020_TYPECCOUNT := 1;
               l_uw.D2060_TYPECDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_ust.USMISCTYPEDCHARGE IS NULL THEN
               l_uw.D2021_TYPEDCOUNT := NULL;
               l_uw.D2061_TYPEDDESCRIPTION := null;
            ELSE
               l_uw.D2021_TYPEDCOUNT := 1;
               l_uw.D2061_TYPEDDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_ust.USMISCTYPEECHARGE IS NULL THEN
               l_uw.D2022_TYPEECOUNT := NULL;
               l_uw.D2062_TYPEEDESCRIPTION := null;
            ELSE
               l_uw.D2022_TYPEECOUNT := 1;
               l_uw.D2062_TYPEEDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_ust.USMISCTYPEFCHARGE IS NULL THEN
               l_uw.D2024_TYPEFCOUNT := NULL;
               l_uw.D2064_TYPEFDESCRIPTION := null;
            ELSE
               l_uw.D2024_TYPEFCOUNT := 1;
               l_uw.D2064_TYPEFDESCRIPTION := 'Annual Charge';
            END IF;

            IF l_ust.USMISCTYPEGCHARGE IS NULL THEN
               l_uw.D2046_TYPEGCOUNT := NULL;
               l_uw.D2065_TYPEGDESCRIPTION := NULL;
            ELSE
               l_uw.D2046_TYPEGCOUNT := 1;
               l_uw.D2065_TYPEGDESCRIPTION := 'Annual Charge';
            END IF;

           IF l_ust.USMISCTYPEHCHARGE IS NULL THEN
              l_uw.D2048_TYPEHCOUNT := NULL;
              l_uw.D2069_TYPEHDESCRIPTION := null;
           ELSE
              l_uw.D2048_TYPEHCOUNT := 1;
              l_uw.D2069_TYPEHDESCRIPTION := 'Annual Charge';
           END IF;

         END IF;

      END IF;

      l_mo.UNMEASUREDTYPEACOUNT	:= l_uw.D2018_TYPEACOUNT;
      l_mo.UNMEASUREDTYPEBCOUNT := l_uw.D2019_TYPEBCOUNT;
      l_mo.UNMEASUREDTYPECCOUNT := l_uw.D2020_TYPECCOUNT;
      l_mo.UNMEASUREDTYPEDCOUNT	:= l_uw.D2021_TYPEDCOUNT;
      l_mo.UNMEASUREDTYPEECOUNT := l_uw.D2022_TYPEECOUNT;
      l_mo.UNMEASUREDTYPEFCOUNT := l_uw.D2024_TYPEFCOUNT;
      l_mo.UNMEASUREDTYPEGCOUNT := l_uw.D2046_TYPEGCOUNT;
      l_mo.UNMEASUREDTYPEHCOUNT := l_uw.D2048_TYPEHCOUNT;
      l_mo.UNMEASUREDTYPEADESCRIPTION := l_uw.D2058_TYPEADESCRIPTION;
      l_mo.UNMEASUREDTYPEBDESCRIPTION := l_uw.D2059_TYPEBDESCRIPTION;
      l_mo.UNMEASUREDTYPECDESCRIPTION := l_uw.D2060_TYPECDESCRIPTION;
      l_mo.UNMEASUREDTYPEDDESCRIPTION := l_uw.D2061_TYPEDDESCRIPTION;
      l_mo.UNMEASUREDTYPEEDESCRIPTION := l_uw.D2062_TYPEEDESCRIPTION;
      l_mo.UNMEASUREDTYPEFDESCRIPTION := l_uw.D2064_TYPEFDESCRIPTION;
      l_mo.UNMEASUREDTYPEGDESCRIPTION := l_uw.D2065_TYPEGDESCRIPTION;
      l_mo.UNMEASUREDTYPEHDESCRIPTION := l_uw.D2069_TYPEHDESCRIPTION;

      IF t_prop(i).SERVICECOMPONENTTYPE IN ('US', 'UW') THEN
         l_mo.PIPESIZE := 0;
      ELSE
         l_mo.PIPESIZE := NULL;
      END IF;

      -- If cross border tariff check wholesaler id is set

      IF l_rec_written THEN
         IF substr(l_mo.TARIFFCODE_PK,1,4) <> '1STW' THEN

            l_progress := 'SELECT MO_SUPPLY_POINT';
            BEGIN
              SELECT WHOLESALERID_PK
              INTO   l_sp.WHOLESALERID_PK
              FROM   MO_SUPPLY_POINT
              WHERE  SPID_PK = l_mo.SPID_PK;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Supply Point does not exist',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                 l_no_row_exp := l_no_row_exp + 1;
                 l_rec_written := FALSE;
            END;

            IF l_rec_written THEN 
              CASE   substr(l_mo.TARIFFCODE_PK,1,4)
                WHEN '1ANG' THEN
                      IF l_sp.WHOLESALERID_PK <> 'ANGLIAN-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
                WHEN '1NWE' THEN
                      IF l_sp.WHOLESALERID_PK <> 'UNITED-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
                WHEN '1THA' THEN
                      IF l_sp.WHOLESALERID_PK <> 'THAMES-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
               WHEN '1WEL' THEN
                      IF l_sp.WHOLESALERID_PK <> 'DWRCYMRU-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
               WHEN '1YOR' THEN
                      IF l_sp.WHOLESALERID_PK <> 'YORKSHIRE-W' THEN
                         P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'OWC TARIFF AND WHOLESALERID IS INCORRECT',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                         l_no_row_exp := l_no_row_exp + 1;
                         l_rec_written := FALSE;
                      END IF;
               ELSE
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'UNKNOWN TARIFF WATER COMPANY',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                   l_no_row_exp := l_no_row_exp + 1;
                   l_rec_written := FALSE;
              END CASE;
           END IF;   
         END IF;
      END IF;


      IF t_prop(i).NO_PROPERTY  <> t_prop(i).NO_PROPERTY_MASTER THEN 
         l_mo.STWPROPERTYNUMBER_PK := t_prop(i).NO_PROPERTY_MASTER;
      END IF;
         
      l_progress := 'INSERT MO_SERVICE_COMPONENT';

      IF l_rec_written THEN
        BEGIN
          INSERT INTO MO_SERVICE_COMPONENT
          (SERVICECOMPONENTREF_PK, TARIFFCODE_PK, SPID_PK, DPID_PK, STWPROPERTYNUMBER_PK, STWSERVICETYPE,
           SERVICECOMPONENTTYPE, SERVICECOMPONENTENABLED, EFFECTIVEFROMDATE,
           SPECIALAGREEMENTFACTOR, SPECIALAGREEMENTFLAG, SPECIALAGREEMENTREF,
           METEREDPWMAXDAILYDEMAND, DAILYRESERVEDCAPACITY,
           METEREDNPWMAXDAILYDEMAND, METEREDNPWDAILYRESVDCAPACITY,
           METEREDFSMAXDAILYDEMAND, METEREDFSDAILYRESVDCAPACITY,
           HWAYSURFACEAREA, HWAYCOMMUNITYCONFLAG,
           ASSESSEDDVOLUMETRICRATE, ASSESSEDCHARGEMETERSIZE, ASSESSEDTARIFBAND,
           SRFCWATERAREADRAINED, SRFCWATERCOMMUNITYCONFLAG,
           UNMEASUREDTYPEACOUNT, UNMEASUREDTYPEBCOUNT, UNMEASUREDTYPECCOUNT, UNMEASUREDTYPEDCOUNT,
           UNMEASUREDTYPEECOUNT, UNMEASUREDTYPEFCOUNT, UNMEASUREDTYPEGCOUNT, UNMEASUREDTYPEHCOUNT,
           UNMEASUREDTYPEADESCRIPTION, UNMEASUREDTYPEBDESCRIPTION, UNMEASUREDTYPECDESCRIPTION,
           UNMEASUREDTYPEDDESCRIPTION, UNMEASUREDTYPEEDESCRIPTION, UNMEASUREDTYPEFDESCRIPTION,
           UNMEASUREDTYPEGDESCRIPTION, UNMEASUREDTYPEHDESCRIPTION, PIPESIZE)
           VALUES
           (l_mo.SERVICECOMPONENTREF_PK, l_mo.TARIFFCODE_PK, l_mo.SPID_PK, l_mo.DPID_PK, l_mo.STWPROPERTYNUMBER_PK, l_mo.STWSERVICETYPE, -- V1.30/1.31
           l_mo.SERVICECOMPONENTTYPE, l_mo.SERVICECOMPONENTENABLED, l_mo.EFFECTIVEFROMDATE,
           l_mo.SPECIALAGREEMENTFACTOR, l_mo.SPECIALAGREEMENTFLAG, l_mo.SPECIALAGREEMENTREF,
           l_mo.METEREDPWMAXDAILYDEMAND, l_mo.DAILYRESERVEDCAPACITY,
           l_mo.METEREDNPWMAXDAILYDEMAND, l_mo.METEREDNPWDAILYRESVDCAPACITY,
           l_mo.METEREDFSMAXDAILYDEMAND, l_mo.METEREDFSDAILYRESVDCAPACITY,
           l_mo.HWAYSURFACEAREA, l_mo.HWAYCOMMUNITYCONFLAG,
           l_mo.ASSESSEDDVOLUMETRICRATE, l_mo.ASSESSEDCHARGEMETERSIZE, l_mo.ASSESSEDTARIFBAND,
           l_mo.SRFCWATERAREADRAINED, l_mo.SRFCWATERCOMMUNITYCONFLAG,
           l_mo.UNMEASUREDTYPEACOUNT, l_mo.UNMEASUREDTYPEBCOUNT, l_mo.UNMEASUREDTYPECCOUNT, l_mo.UNMEASUREDTYPEDCOUNT,
           l_mo.UNMEASUREDTYPEECOUNT, l_mo.UNMEASUREDTYPEFCOUNT, l_mo.UNMEASUREDTYPEGCOUNT, l_mo.UNMEASUREDTYPEHCOUNT,
           l_mo.UNMEASUREDTYPEADESCRIPTION, l_mo.UNMEASUREDTYPEBDESCRIPTION, l_mo.UNMEASUREDTYPECDESCRIPTION,
           l_mo.UNMEASUREDTYPEDDESCRIPTION, l_mo.UNMEASUREDTYPEEDESCRIPTION, l_mo.UNMEASUREDTYPEFDESCRIPTION,
           l_mo.UNMEASUREDTYPEGDESCRIPTION, l_mo.UNMEASUREDTYPEHDESCRIPTION, l_mo.PIPESIZE);
        EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
             l_rec_written := FALSE;

             IF (   t_prop(i).NO_TARIFF_GROUP <> 1
                 OR t_prop(i).NO_TARIFF_SET   <> 1)
             THEN
                -- More than one active tariff for service component but is the same
                IF l_prev_tariff = l_mo.TARIFFCODE_PK THEN
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Duplicate tariff Service Provision, record dropped',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                   l_no_row_war := l_no_row_war + 1;
                ELSE
                  P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for Service Provision',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                  l_no_row_exp := l_no_row_exp + 1;
                END IF;
             ELSE
                BEGIN
                    SELECT TARIFFCODE_PK
                    INTO   l_prev_tariff
                    FROM   MO_SERVICE_COMPONENT
                    WHERE  SPID_PK              = l_mo.SPID_PK
                    AND    SERVICECOMPONENTTYPE = l_mo.SERVICECOMPONENTTYPE
                    AND    TARIFFCODE_PK        = l_mo.TARIFFCODE_PK;
                EXCEPTION
                WHEN NO_DATA_FOUND THEN
                     l_prev_tariff := NULL;
                END;

                IF l_prev_tariff IS NOT NULL THEN
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'W', 'Duplicate tariff Service Component, record dropped',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress || ',' || l_mo.TARIFFCODE_PK,1,100));
                   l_no_row_war := l_no_row_war + 1;
                ELSE
                   P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'More than 1 active tariff for Service Component',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                   l_no_row_exp := l_no_row_exp + 1;
                END IF;
             END IF;

        WHEN OTHERS THEN
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;
             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_err := l_no_row_err + 1;
        END;
      END IF;

        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
        ELSE
            -- if tolearance limit has een exceeded, set error message and exit out
           l_no_row_dropped := l_no_row_dropped + 1;
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
        l_prev_tariff := l_mo.TARIFFCODE_PK;

    END LOOP;

    IF t_prop.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;

  CLOSE cur_prop;

  -- Recon count of service component types per property loaded into bt_tvp054 with a tariff.
  OPEN cur_sc_with_tariff;
  LOOP

    FETCH cur_sc_with_tariff BULK COLLECT INTO t_sc_with_tariff LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_sc_with_tariff.COUNT
    LOOP

      l_err.TXT_KEY := t_sc_with_tariff(i).servicecomponenttype;

      CASE t_sc_with_tariff(i).servicecomponenttype
           WHEN 'MPW' THEN
              l_sc_measure := 700;
           WHEN 'MNPW' THEN
              l_sc_measure := 710;
           WHEN 'AW' THEN
              l_sc_measure := 720;
           WHEN 'UW' THEN
              l_sc_measure := 730;
           WHEN 'MS' THEN
              l_sc_measure := 740;
           WHEN 'AS' THEN
              l_sc_measure := 750;
           WHEN 'US' THEN
              l_sc_measure := 760;
           WHEN 'SW' THEN
              l_sc_measure := 770;
           WHEN 'HD' THEN
              l_sc_measure := 780;
           WHEN 'TE' THEN
              l_sc_measure := 790;
           WHEN 'WCA' THEN
              l_sc_measure := 800;
           WHEN 'SCA' THEN
              l_sc_measure := 810;
           ELSE
              l_sc_measure := 811;
      END CASE;

      P_MIG_BATCH.FN_RECONLOG(no_batch,
                              l_job.NO_INSTANCE,
                              'CP25',
                              l_sc_measure,
                              t_sc_with_tariff(i).type_count,
                              'Distinct Service Component Type '
                               || t_sc_with_tariff(i).servicecomponenttype
                             || ' during KEY_GEN stage 2'
                             );

    END LOOP;

    IF t_sc_with_tariff.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;
  CLOSE cur_sc_with_tariff;

  -- write counts
  l_progress := 'Writing Counts';

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1030, l_no_row_read,    'Read in to transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1040, l_no_row_dropped, 'Dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1050, l_no_row_insert,  'Written to Table ');

  SELECT COUNT(DISTINCT spid_pk)
    INTO l_count_spids
    FROM MO_SERVICE_COMPONENT;
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP30', 1051, l_count_spids,  'Number of SPIDs in Table ');

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
END P_MOU_TRAN_SERVICE_COMPONENT;
/
show errors;

exit;