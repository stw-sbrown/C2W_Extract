create or replace
PROCEDURE P_MOU_TRAN_KEY_GEN(no_batch    IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                             no_job      IN MIG_JOBREF.NO_JOB%TYPE,
                             return_code IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Eligibility Key Generation
--
-- AUTHOR         : Surinder Badhan
--
-- FILENAME       : P_MOU_TRAN_KEY_GEN.sql
--
-- Subversion $Revision: 6462 $
--
-- CREATED        : 24/02/2016
--
-- DESCRIPTION    : Procedure to generate the keys for each eligible Premises
--                 On the Eligiblity_Control_table.  This procedure is intended for use by the MOSL
--                 initial Upload Migration.
-- NOTES  :
-- This package must be run each time the transform batch is run.  It should generate all keys
-- need for each of the Field and value mapping documents (Property, Primary LE, Supply Point,
--  Service Component, Discharge Point, Meter, Meter reading).
---------------------------- Modification History --------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 2.20      06/12/2016  D.Cheung   Remove FG_MO_LOADED check
--                                    Add 'M' to ST_SERV_PROV values JUST for phase 4 (Note: Does NOT apply to SAP)
-- V 2.19      21/11/2016  D.Cheung   Update BT_MISS_AG_SC to pick up missing TE services due to aggregates
-- V 2.18      29/09/2016  S.Badhan   Update for new index BT_TVP054_IDX4.
-- V 2.17      28/09/2016  S.Badhan   Change OWC bt query so condition before left join 
-- V 2.16      27/09/2016  O.Badmus   Fix to OWC bt table
-- V 2.15      26/09/2016  S.Badhan   Performance changes - add parallel processing.
-- V 2.14      16/09/2016  S.Badhan   Add SAP ready flags.
-- V 2.13      13/09/2016  S.Badhan   Select properties based on MO ready flag.
-- V 2.12      18/08/2016  O.Badmus   Appending properties in LU_SPID_OWC_RETAILER
-- V 2.11      17/08/2016  D.Cheung   CR_034 - Exclude ANGLIAN-W (ID_OWC) from METER keygen
-- V 2.10      10/08/2016  O.Badmus   V 2.04 revisited. Null dt_end for properties in LU_SPID_OWC_RETAILER (So that properties that have switched suppliers can flow through)
-- V 2.09      04/08/2016  D.Cheung   I-299 - Change Aggregate-Network logic based on new AGG_NET values
-- V 2.08      03/08/2016  D.Cheung   I-326 - Add Order by NO_LEGAL_ENTITY to partition logic to force consistent results
-- V 2.07      08/07/2016  S.Badhan   I-282. Retrieve all aggregrate properties.
-- V 2.06      07/07/2016  S.Badhan   I-278. For aggregrate properties use services from master property.
--                                    Write service provision missing from aggregate property to BT_MISS_AG_SC.
-- V 2.05      05/07/2016  D.Cheung   CR_021 - Move processing for deleting AGGREGATE properties from BT_TVP054 to ABOVE INSERT Aggregates
-- V 2.04      04/07/2016  O.Badmus   Update statments added to null dt_end for properties in LU_SPID_OWC_RETAILER (So that properties that have switched suppliers can flow through)
-- V 2.03      28/06/2016  S.Badhan   CR_021  Add new columns from ECT to BT_TVP054 and BT_TVP163
-- V 2.02      27/06/2016  L.Smith    Performance changes
-- V 2.01      06/06/2016  D.Cheung   CR_018 - Add Lookup for SAP FLOCA Number
-- V 1.08      18/05/2016  L.Smith    Force a direct load when inserting.
-- V 1.07      28/04/2016  S.Badhan   I-161 and I-162. Add reconciliation counts.
-- V 1.06      15/04/2016  S.Badhan   Populate FG_CONSOLIDATED from ECT.
--                                    ECT now in schema CIS. Use service provision from
--                                    TVMNHHDTL.
-- V 1.05      13/04/2016  S.Badhan   Added columns AGG_NET, FG_CONSOLIDATED to
--                                    BT_TVP054 and BT_TVP163. Removed Meter Info from
--                                    BT_TVP054.
--                                    Added selection to choose valid service provision
--                                    statuses. Sort partition by DT_END instead of DT_START.
-- V 1.04      06/04/2016  D.Cheung   Added new code to generate BT_TVP163 Meter KeyGen table
-- V 1.03      23/03/2016  S.Badhan   Change to just adding to BT_TVP054 rather than
--                                    creating table as well.
-- V 1.20      17/03/2016  S.Badhan   Meter information added from TVMNHHDTL.
-- V 1.10      15/03/2016  S.Badhan   Table TEMP_TVP054 renamed to BT_TVP054.
-- V 1.00      10/03/2016  S.Badhan   Amended to remove spaces on CD_SERVICE_PROV
-- V 0.02      09/03/2016  S.Badhan   Amended to select latest Legal Entity
-- V 0.01      24/02/2016  S.Badhan   Initial Draft
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_KEY_GEN';
  l_key                         VARCHAR2(30);
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_jobref                      MIG_JOBREF%ROWTYPE;
  l_ph                          MIG_PHASE_KEYGEN%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_water_sp                    NUMBER(9);
  l_sewage_sp                   NUMBER(9);
  l_le                          NUMBER(9);
  l_orig_ins_tvp054             NUMBER(9);
  l_dom_delete                  NUMBER(9);
  l_agg_del_tvp054              NUMBER(9);
  l_agg_tvp054                  NUMBER(9);

BEGIN

   l_progress := 'Start of key gen';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_insert := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
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

   l_progress := 'truncating table - BT_TVP054';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_TVP054';
   EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX1 UNUSABLE';
   EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX2 UNUSABLE';
   EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX3 UNUSABLE';
   EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX4 UNUSABLE';
   
  l_progress := 'INSERT BT_TVP054';

  INSERT /*+ append */ INTO BT_TVP054
   (CD_COMPANY_SYSTEM, NO_ACCOUNT, NO_PROPERTY, CD_SERVICE_PROV, NO_COMBINE_054, NO_SERV_PROV, DT_START, DT_END,
   NM_LOCAL_SERVICE, CD_PROPERTY_USE, NO_COMBINE_024, TP_CUST_ACCT_ROLE, NO_LEGAL_ENTITY, nc024_DT_START, nc024_DT_END,
   IND_LEGAL_ENTITY,
   FG_TOO_HARD, CD_PROPERTY_USE_ORIG,
   CD_PROPERTY_USE_CURR, CD_PROPERTY_USE_FUT, UDPRN, UPRN, VOA_REFERENCE, SAP_FLOC, CORESPID, AGG_NET, FG_CONSOLIDATED,
   FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM,
   FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED)
  SELECT CD_COMPANY_SYSTEM,
       NO_ACCOUNT,
       NO_PROPERTY,
       CD_SERVICE_PROV,
       NO_COMBINE_054,
       NO_SERV_PROV,
       DT_START,
       DT_END,
       NM_LOCAL_SERVICE,
       CD_PROPERTY_USE,
       NO_COMBINE_024,
       TP_CUST_ACCT_ROLE,
       NO_LEGAL_ENTITY,
       nc024_DT_START,
       nc024_DT_END,
       IND_LEGAL_ENTITY,
       FG_TOO_HARD,
       CD_PROPERTY_USE_ORIG,
	     CD_PROPERTY_USE_CURR,
	     CD_PROPERTY_USE_FUT,
	     UDPRN,
	     UPRN,
       VOA_REFERENCE,
       SAP_FLOC,
       CORESPID,
       AGG_NET,
       FG_CONSOLIDATED,
       FG_TE,
       FG_MECOMS_RDY,
       NO_PROPERTY_MASTER,
       FG_NMM,
       FG_MO_RDY,
       FG_MO_LOADED,
       TS_MO_LOADED
FROM
(SELECT /*+ PARALLEL(tnhh,60) PARALLEL(telg,60) PARALLEL(lsf,60) */
       tnhh.CD_COMPANY_SYSTEM,
       tnhh.NO_ACCOUNT,
       tnhh.NO_PROPERTY,
       trim(tnhh.CD_SERVICE_PROV) as CD_SERVICE_PROV,
       tnhh.NO_COMBINE_054,
       tnhh.NO_SERV_PROV,
       tnhh.DT_START,
       tnhh.DT_END,
       tnhh.NM_LOCAL_SERVICE,
       tnhh.CD_PROPERTY_USE,
       tnhh.NO_COMBINE_024,
       tnhh.TP_CUST_ACCT_ROLE,
       tnhh.NO_LEGAL_ENTITY,
       tnhh.nc024_DT_START,
       tnhh.nc024_DT_END,
       tnhh.IND_LEGAL_ENTITY,
       tnhh.FG_TOO_HARD,
       telg.CD_PROPERTY_USE_ORIG,
	     telg.CD_PROPERTY_USE_CURR,
	     telg.CD_PROPERTY_USE_FUT,
	     telg.UDPRN,
	     telg.UPRN,
       telg.VOA_REFERENCE,
       lsf.SAPFLOCNUMBER AS SAP_FLOC,
       telg.CORESPID,
       telg.AGG_NET,
       telg.FG_CONSOLIDATED,
       telg.FG_TE,
       telg.FG_MECOMS_RDY,
       telg.NO_PROPERTY_MASTER,
       telg.FG_NMM,
       tnhh.FG_MO_RDY,
       tnhh.FG_MO_LOADED,
       tnhh.TS_MO_LOADED,
       ROW_NUMBER() OVER ( PARTITION BY tnhh.NO_PROPERTY, tnhh.NO_SERV_PROV ORDER BY tnhh.NO_PROPERTY, tnhh.NO_SERV_PROV, tnhh.DT_END desc NULLS FIRST, nc024_DT_START desc, tnhh.IND_LEGAL_ENTITY ) AS Record_Nr
FROM   TVMNHHDTL tnhh,
       CIS.ELIGIBILITY_CONTROL_TABLE telg,
       LU_SAP_FLOCA lsf
WHERE  tnhh.CD_COMPANY_SYSTEM = 'STW1'
AND    tnhh.TP_CUST_ACCT_ROLE = 'P'
AND    tnhh.ST_SERV_PROV      in ('A','C','E','G','V','M')    --V2.20 - 'M' ONLY used for phase 4 (Note: Does NOT apply to SAP)
AND    tnhh.FG_MO_RDY         in (SELECT PHASE
                                  FROM   MIG_PHASE_KEYGEN
                                  WHERE  FG_INLC_BATCH = 'Y'
                                  AND    FG_KEYGEN_PRC = 'N')
--AND    tnhh.FG_MO_LOADED      = 'N'   --V2.20
AND    telg.CD_COMPANY_SYSTEM = tnhh.CD_COMPANY_SYSTEM
AND    telg.NO_PROPERTY       = tnhh.NO_PROPERTY
AND    telg.NO_PROPERTY       = lsf.STWPROPERTYNUMBER_PK(+)
and    tnhh.DT_START         <> nvl(tnhh.DT_END,to_date('31/12/2099','dd/mm/yyyy')) ) x
WHERE  Record_Nr = 1;

  l_orig_ins_tvp054 := SQL%ROWCOUNT;

  COMMIT;
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX1 REBUILD';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX2 REBUILD';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX3 REBUILD';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX4 REBUILD';

-- V 2.10
   l_progress := 'truncating - BT_OWC_CUST_SWITCHED_SUPPLIER';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_OWC_CUST_SWITCHED_SUPPLIER';
   EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_OWC_1 UNUSABLE';
   EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_OWC_2 UNUSABLE';

  l_progress := 'INSERT BT_OWC_CUST_SWITCHED_SUPPLIER';

   -- Defaulting ST_SERV_PROV to A and DT_END to NULL

   INSERT /*+ append */ INTO BT_OWC_CUST_SWITCHED_SUPPLIER
   select CD_COMPANY_SYSTEM,	NO_ACCOUNT,	NO_PROPERTY,	 CD_SERVICE_PROV,	NO_COMBINE_054,	NO_SERV_PROV,'A' ST_SERV_PROV,	DT_START,NULL	DT_END,	NM_LOCAL_SERVICE
   ,	CD_PROPERTY_USE,	NO_COMBINE_024,	TP_CUST_ACCT_ROLE,	NO_LEGAL_ENTITY,	NC024_DT_START,	NC024_DT_END,	IND_LEGAL_ENTITY,	FG_TOO_HARD,	CD_PROPERTY_USE_ORIG
   ,	CD_PROPERTY_USE_CURR,	CD_PROPERTY_USE_FUT,	UDPRN,	UPRN,	VOA_REFERENCE,	 SAP_FLOC,	CORESPID,	AGG_NET,	FG_CONSOLIDATED,	FG_TE,	FG_MECOMS_RDY,	NO_PROPERTY_MASTER,	FG_NMM
   ,  FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED, FG_SAP_RDY, FG_SAP_LOADED, TS_SAP_LOADED
   from (
   SELECT /*+ PARALLEL(t1,60) PARALLEL(t2,60)  */
   t1.CD_COMPANY_SYSTEM,	t1.NO_ACCOUNT,	t1.NO_PROPERTY,	trim(t1.CD_SERVICE_PROV) as CD_SERVICE_PROV,	t1.NO_COMBINE_054,	t1.NO_SERV_PROV,	t1.ST_SERV_PROV
   ,t1.DT_START,	t1.DT_END,	t1.NM_LOCAL_SERVICE,	t1.CD_PROPERTY_USE,	t1.NO_COMBINE_024,	t1.TP_CUST_ACCT_ROLE,	t1.NO_LEGAL_ENTITY,	t1.NC024_DT_START,	t1.NC024_DT_END
   ,t1.IND_LEGAL_ENTITY,	t1.FG_TOO_HARD,	t3.CD_PROPERTY_USE_ORIG,	t3.CD_PROPERTY_USE_CURR,	t3.CD_PROPERTY_USE_FUT,	t3.UDPRN,	t3.UPRN,	t3.VOA_REFERENCE,	t4.SAPFLOCNUMBER AS SAP_FLOC
   ,t3.CORESPID,	t3.AGG_NET,	t3.FG_CONSOLIDATED,	t3.FG_TE,	t3.FG_MECOMS_RDY,	t3.NO_PROPERTY_MASTER,	t3.FG_NMM
   ,t1.FG_MO_RDY, t1.FG_MO_LOADED, t1.TS_MO_LOADED, NULL AS FG_SAP_RDY, NULL AS FG_SAP_LOADED, NULL AS TS_SAP_LOADED
   ,ROW_NUMBER() OVER ( PARTITION BY t1.NO_PROPERTY
   ORDER BY t1.NO_PROPERTY, t1.NO_SERV_PROV, t1.DT_END desc NULLS FIRST, t1.DT_START desc, t1.IND_LEGAL_ENTITY ) AS Record_Nr
   from RECEPTION.TVMNHHDTL t1
   join LU_SPID_OWC_RETAILER T2 on (T1.NO_PROPERTY = T2.STWPROPERTYNUMBER_PK and trim(T1.CD_SERVICE_PROV) = T2.STWSERVICETYPE
   AND T1.NO_LEGAL_ENTITY = T2.NO_LEGAL_ENTITY)
   and    t1.DT_START  <> nvl(t1.DT_END,to_date('31/12/2099','dd/mm/yyyy'))
   AND    t1.FG_MO_RDY           in (SELECT PHASE
                                     FROM   MIG_PHASE_KEYGEN
                                     WHERE  FG_INLC_BATCH = 'Y'
                                     AND    FG_KEYGEN_PRC = 'N')
--   AND    t1.FG_MO_LOADED  = 'N'    --V2.20
   join CIS.ELIGIBILITY_CONTROL_TABLE T3 on T3.CD_COMPANY_SYSTEM = T1.CD_COMPANY_SYSTEM and  T3.NO_PROPERTY  = T1.NO_PROPERTY
   LEFT join LU_SAP_FLOCA t4 on    t3.NO_PROPERTY       = t4.STWPROPERTYNUMBER_PK  -- V 2.16 
   ) x
   where Record_Nr = 1;

    COMMIT;
    EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_OWC_1 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX IND_BT_OWC_2 REBUILD';

-- V 2.10
    --delete matching records in BT_TVP054
  l_progress := 'DELETE BT_TVP054 - OWC';

   -- EXECUTE IMMEDIATE
    DELETE FROM BT_TVP054
    WHERE (NO_PROPERTY,TRIM(CD_SERVICE_PROV)) IN (SELECT NO_PROPERTY
                                              ,TRIM(CD_SERVICE_PROV)
                                              FROM BT_OWC_CUST_SWITCHED_SUPPLIER);
    commit;

-- V 2.10
    --INSERT RECORDS FROM BT_OWC_CUST_SWITCHED_SUPPLIER INTO KEYGEN
    l_progress := 'INSERT BT_TVP054 from BT_OWC_CUST_SWITCHED_SUPPLIER';

    INSERT /*+ append */ INTO BT_TVP054
    (CD_COMPANY_SYSTEM, NO_ACCOUNT, NO_PROPERTY, CD_SERVICE_PROV, NO_COMBINE_054, NO_SERV_PROV, ST_SERV_PROV, DT_START, DT_END,
     NM_LOCAL_SERVICE, CD_PROPERTY_USE, NO_COMBINE_024, TP_CUST_ACCT_ROLE, NO_LEGAL_ENTITY, nc024_DT_START, nc024_DT_END,
     IND_LEGAL_ENTITY, FG_TOO_HARD, CD_PROPERTY_USE_ORIG,
     CD_PROPERTY_USE_CURR, CD_PROPERTY_USE_FUT, UDPRN, UPRN, VOA_REFERENCE, SAP_FLOC, CORESPID, AGG_NET, FG_CONSOLIDATED,
     FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM,
     FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED)
    select CD_COMPANY_SYSTEM,	NO_ACCOUNT,	NO_PROPERTY, CD_SERVICE_PROV,	NO_COMBINE_054,	NO_SERV_PROV,	ST_SERV_PROV,	DT_START, NULL,
    NM_LOCAL_SERVICE,	CD_PROPERTY_USE,	NO_COMBINE_024,	TP_CUST_ACCT_ROLE,	NO_LEGAL_ENTITY,	NC024_DT_START,	NC024_DT_END,
    IND_LEGAL_ENTITY,	FG_TOO_HARD,	CD_PROPERTY_USE_ORIG,
    CD_PROPERTY_USE_CURR,	CD_PROPERTY_USE_FUT,	UDPRN,	UPRN,	VOA_REFERENCE, SAP_FLOC, CORESPID, AGG_NET,	FG_CONSOLIDATED,
    FG_TE, FG_MECOMS_RDY,	NO_PROPERTY_MASTER,	FG_NMM,
    FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED
   from BT_OWC_CUST_SWITCHED_SUPPLIER;


    l_progress := 'truncating BT_TVP163';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_TVP163';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX1 UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX2 UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX3 UNUSABLE';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX4 UNUSABLE';


    l_progress := 'INSERT BT_TVP163';

        -- Current or Last SPR meters
      INSERT /*+ append */ INTO BT_TVP163
        (cd_company_system, no_account, no_property, cd_service_prov, no_combine_054, no_serv_prov, dt_start_054, dt_end_054,
            nm_local_service, no_combine_034, dt_start_034, dt_start_lr_034, dt_end_034, FG_ADD_SUBTRACT, NO_UTL_EQUIP,NO_EQUIPMENT,
            no_combine_043, NO_REGISTER, TP_EQUIPMENT, CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET, PC_USAGE_SPLIT, AM_BILL_MULTIPLIER,
            ST_METER_REG_115, no_combine_163_inst, no_property_inst, tvp202_no_serv_prov_inst, IND_Market_Prop_Inst, fg_too_hard, CORESPID, AGG_NET, FG_CONSOLIDATED,
            CD_PROPERTY_USE, FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM,
            FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED)
      SELECT /*+ PARALLEL(telg,60) PARALLEL(keygen,60) PARALLEL(t202,60) */
            DISTINCT
            t034.cd_company_system,
            keygen.no_account,
            keygen.no_property,
            keygen.cd_service_prov,
            keygen.no_combine_054,
            keygen.no_serv_prov,
            keygen.dt_start as dt_start_054,
            keygen.dt_end as dt_end_054,
            keygen.nm_local_service,
            t034.no_combine_034,
            t034.dt_start as dt_start_034,
            t034.DT_START_LR as dt_start_lr_034,
            t034.dt_end   as dt_end_034,
            t034.FG_ADD_SUBTRACT,
            t063.NO_UTL_EQUIP,
            t043.NO_EQUIPMENT,
            t043.no_combine_043,
            t043.NO_REGISTER,
            t034.TP_EQUIPMENT,
            t034.CD_TARIFF,
            t034.NO_TARIFF_GROUP,
            t034.NO_TARIFF_SET,
            t034.PC_USAGE_SPLIT,
            t043.AM_BILL_MULTIPLIER,
            t043.ST_METER_REG_115,
            t202.no_combine_163 as no_combine_163_inst,
            t202.no_property as no_property_inst,
            t202.no_serv_prov as tvp202_no_serv_prov_inst,
            case when t202.no_property <> keygen.no_property
                 then NVL((select 'Y' from BT_TVP054 m
                    WHERE m.NO_PROPERTY = t202.no_property
                    AND ROWNUM = 1),'N')
                else 'Y'
            end as IND_Market_Prop_Inst,
            cast( null as char(1)) fg_too_hard,
            keygen.CORESPID,
            keygen.AGG_NET,
            keygen.FG_CONSOLIDATED,
            keygen.CD_PROPERTY_USE,
            keygen.FG_TE,
            keygen.FG_MECOMS_RDY,
            keygen.NO_PROPERTY_MASTER,
            keygen.FG_NMM,
            keygen.FG_MO_RDY,
            keygen.FG_MO_LOADED,
            keygen.TS_MO_LOADED
        FROM CIS.tvp202servproveqp  t202,
            CIS.tvp163equipinst    t163,
            CIS.tvp043meterreg     t043,
            CIS.tvp034instregassgn t034,
            cis.tvp063equipment    t063,
            (SELECT distinct
                no_combine_054, no_account,
                no_property, cd_service_prov, no_serv_prov, nm_local_service,
                dt_start, dt_end, CORESPID, AGG_NET, FG_CONSOLIDATED,
                CD_PROPERTY_USE, FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM,
                FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED
              FROM BT_TVP054
             ) keygen,
             CIS.ELIGIBILITY_CONTROL_TABLE telg
        WHERE
            -- Get Tariff logical registers for Eligable SPRs. On end dated SPRs, so will the register be.
            t034.NO_COMBINE_054    = keygen.no_combine_054
        AND ( t034.DT_END IS NULL or (keygen.dt_end is not null and t034.DT_END = keygen.dt_end) )
        --AND t034.CD_COMPANY_SYSTEM = 'STW1'
            -- Get Physical Meter using register
        AND t043.NO_COMBINE_043    = t034.NO_COMBINE_043
        AND t043.CD_COMPANY_SYSTEM = t034.cd_company_system
            -- get Equipment Info
        AND t063.NO_EQUIPMENT      = t043.NO_EQUIPMENT
        AND t063.CD_COMPANY_SYSTEM = t043.cd_company_system
            -- Get Equipment Locations (logical and physical)
        AND T163.NO_EQUIPMENT      = t043.NO_EQUIPMENT
        AND T163.ST_EQUIP_INST     = 'A'
        AND T163.CD_COMPANY_SYSTEM = t043.cd_company_system
            -- Get Physical Location
        AND T202.NO_COMBINE_163     = t163.NO_COMBINE_163
        AND t202.CD_COMPANY_SYSTEM = t163.cd_company_system
        and t202.ind_inst_at_prop  = 'Y'
            --EXCLUDE ID-OWC = ANGLIAN-W
        AND telg.CD_COMPANY_SYSTEM = t043.CD_COMPANY_SYSTEM
        AND telg.NO_PROPERTY       = keygen.NO_PROPERTY
        AND telg.ID_OWC            NOT IN ('ANGLIAN-W')
        union -- V 2.12
        select cd_company_system, no_account, no_property, cd_service_prov, no_combine_054, no_serv_prov, dt_start_054, dt_end_054,
            nm_local_service, no_combine_034, dt_start_034, dt_start_lr_034, dt_end_034, FG_ADD_SUBTRACT, NO_UTL_EQUIP,NO_EQUIPMENT,
            no_combine_043, NO_REGISTER, TP_EQUIPMENT, CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET, PC_USAGE_SPLIT, AM_BILL_MULTIPLIER,
            ST_METER_REG_115, no_combine_163_inst, no_property_inst, tvp202_no_serv_prov_inst, IND_Market_Prop_Inst, fg_too_hard, CORESPID, AGG_NET, FG_CONSOLIDATED,
            CD_PROPERTY_USE, FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM,
            FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED
        from (
            SELECT /*+ PARALLEL(telg,60) PARALLEL(keygen,60)  */
            DISTINCT
            t034.cd_company_system,
            keygen.no_account,
            keygen.no_property,
            keygen.cd_service_prov,
            keygen.no_combine_054,
            keygen.no_serv_prov,
            keygen.dt_start as dt_start_054,
            keygen.dt_end as dt_end_054,
            keygen.nm_local_service,
            t034.no_combine_034,
            t034.dt_start as dt_start_034,
            t034.DT_START_LR as dt_start_lr_034,
            t034.dt_end   as dt_end_034,
            t034.FG_ADD_SUBTRACT,
            t063.NO_UTL_EQUIP,
            t043.NO_EQUIPMENT,
            t043.no_combine_043,
            t043.NO_REGISTER,
            t034.TP_EQUIPMENT,
            t034.CD_TARIFF,
            t034.NO_TARIFF_GROUP,
            t034.NO_TARIFF_SET,
            t034.PC_USAGE_SPLIT,
            t043.AM_BILL_MULTIPLIER,
            t043.ST_METER_REG_115,
            t202.no_combine_163 as no_combine_163_inst,
            t202.no_property as no_property_inst,
            t202.no_serv_prov as tvp202_no_serv_prov_inst,
            case when t202.no_property <> keygen.no_property
                 then NVL((select 'Y' from BT_TVP054 m
                    WHERE m.NO_PROPERTY = t202.no_property
                    AND ROWNUM = 1),'N')
                else 'Y'
            end as IND_Market_Prop_Inst,
            cast( null as char(1)) fg_too_hard,
            keygen.CORESPID,
            keygen.AGG_NET,
            keygen.FG_CONSOLIDATED,
            keygen.CD_PROPERTY_USE,
            keygen.FG_TE,
            keygen.FG_MECOMS_RDY,
            keygen.NO_PROPERTY_MASTER,
            keygen.FG_NMM,
            keygen.FG_MO_RDY,
            keygen.FG_MO_LOADED,
            keygen.TS_MO_LOADED
            ,ROW_NUMBER() OVER (PARTITION BY keygen.NO_PROPERTY
             ORDER BY keygen.NO_PROPERTY, T2.STWSERVICETYPE,  keygen.dt_end desc NULLS FIRST,  keygen.dt_start desc ) AS Record_Nr
        FROM CIS.tvp202servproveqp  t202,
            CIS.tvp163equipinst    t163,
            CIS.tvp043meterreg     t043,
            CIS.tvp034instregassgn t034,
            cis.tvp063equipment    t063,
            (SELECT distinct
                no_combine_054, no_account,
                no_property, cd_service_prov, no_serv_prov, nm_local_service,
                dt_start, dt_end, CORESPID, AGG_NET, FG_CONSOLIDATED,
                CD_PROPERTY_USE, FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM,
                FG_MO_RDY, FG_MO_LOADED, TS_MO_LOADED
              FROM BT_TVP054
             ) keygen,
             CIS.ELIGIBILITY_CONTROL_TABLE telg,
           LU_SPID_OWC_RETAILER T2
        WHERE
            -- Get Tariff logical registers for Eligable SPRs. On end dated SPRs, so will the register be.
            t034.NO_COMBINE_054    = keygen.no_combine_054
            -- LU joining key
       and (keygen.NO_PROPERTY = T2.STWPROPERTYNUMBER_PK and trim(keygen.CD_SERVICE_PROV) = T2.STWSERVICETYPE)
--        AND ( t034.DT_END IS NULL or (keygen.dt_end is not null and t034.DT_END = keygen.dt_end) )
        --AND t034.CD_COMPANY_SYSTEM = 'STW1'
            -- Get Physical Meter using register
        AND t043.NO_COMBINE_043    = t034.NO_COMBINE_043
        AND t043.CD_COMPANY_SYSTEM = t034.cd_company_system
            -- get Equipment Info
        AND t063.NO_EQUIPMENT      = t043.NO_EQUIPMENT
        AND t063.CD_COMPANY_SYSTEM = t043.cd_company_system
            -- Get Equipment Locations (logical and physical)
        AND T163.NO_EQUIPMENT      = t043.NO_EQUIPMENT
        --AND T163.ST_EQUIP_INST     = 'A'
        AND T163.CD_COMPANY_SYSTEM = t043.cd_company_system
            -- Get Physical Location
        AND T202.NO_COMBINE_163     = t163.NO_COMBINE_163
        AND t202.CD_COMPANY_SYSTEM = t163.cd_company_system
        and t202.ind_inst_at_prop  = 'Y'
            --EXCLUDE ID-OWC = ANGLIAN-W
        AND telg.CD_COMPANY_SYSTEM = t043.CD_COMPANY_SYSTEM
        AND telg.NO_PROPERTY       = keygen.NO_PROPERTY
        AND telg.ID_OWC            NOT IN ('ANGLIAN-W'))x
        where Record_Nr = 1;


    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP1', 1, SQL%ROWCOUNT, 'Records Written (163)');

    COMMIT;
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX1 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX2 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX3 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP163_IDX4 REBUILD';

  -- Write any missing service provisions from master write
-- *** V2.19
  l_progress := 'INSERT BT_MISS_AG_SC';

  EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_MISS_AG_SC';

  INSERT /*+ append */ INTO BT_MISS_AG_SC
   (CD_COMPANY_SYSTEM, NO_ACCOUNT, NO_PROPERTY, CD_SERVICE_PROV, NO_COMBINE_054, NO_SERV_PROV, DT_START, DT_END
   , NM_LOCAL_SERVICE, CD_PROPERTY_USE, NO_COMBINE_024, TP_CUST_ACCT_ROLE, NO_LEGAL_ENTITY, nc024_DT_START, nc024_DT_END
   , IND_LEGAL_ENTITY, FG_TOO_HARD, CD_PROPERTY_USE_ORIG
   , CD_PROPERTY_USE_CURR, CD_PROPERTY_USE_FUT, UDPRN, UPRN, VOA_REFERENCE, SAP_FLOC, CORESPID, AGG_NET, FG_CONSOLIDATED
   , FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM)
   SELECT
   CD_COMPANY_SYSTEM, NO_ACCOUNT, NO_PROPERTY, CD_SERVICE_PROV, NO_COMBINE_054, NO_SERV_PROV, DT_START, DT_END
   , NM_LOCAL_SERVICE, CD_PROPERTY_USE, NO_COMBINE_024, TP_CUST_ACCT_ROLE, NO_LEGAL_ENTITY, nc024_DT_START, nc024_DT_END
   , IND_LEGAL_ENTITY, FG_TOO_HARD, CD_PROPERTY_USE_ORIG
   , CD_PROPERTY_USE_CURR, CD_PROPERTY_USE_FUT, UDPRN, UPRN, VOA_REFERENCE
   , SAP_FLOC, CORESPID, AGG_NET, FG_CONSOLIDATED,
   FG_TE, FG_MECOMS_RDY, NO_PROPERTY_MASTER, FG_NMM
   FROM   BT_TVP054 T054
   WHERE T054.AGG_NET = 'A'
   AND T054.NO_PROPERTY_MASTER IS NOT NULL
   AND ( T054.NO_PROPERTY_MASTER, T054.CD_SERVICE_PROV) NOT IN (
      SELECT T054B.NO_PROPERTY_MASTER, T054B.CD_SERVICE_PROV
      FROM   BT_TVP054 T054B
      WHERE  T054B.NO_PROPERTY_MASTER IS NOT NULL
      AND T054B.NO_PROPERTY = T054B.NO_PROPERTY_MASTER
  );
  COMMIT;

  -- remove accounts which have a aggregated account

  l_progress := 'DELETE FROM BT_TVP054 Accounts which are Aggregated';

--**** v2.09 new AGG_NET logic for Aggregates and networks
  DELETE FROM BT_TVP054
--  WHERE  NO_PROPERTY_MASTER IS NOT NULL;
  WHERE AGG_NET = 'A' AND NO_PROPERTY <> NO_PROPERTY_MASTER AND NO_PROPERTY_MASTER IS NOT NULL;

  l_agg_del_tvp054 := SQL%ROWCOUNT;

  COMMIT;

  l_agg_tvp054 := 0;

  -- remove from BT_TVP054 domestic properties (was intilally required to build BT_TVP163)

   l_progress := 'DELETE FROM BT_TVP054 - domestic properties';

   DELETE FROM BT_TVP054
   WHERE  (   CORESPID IS NULL
           OR FG_NMM = 'Y' );
   l_dom_delete := SQL%ROWCOUNT;

  COMMIT;

  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX1 REBUILD';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX2 REBUILD';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX3 REBUILD';
  EXECUTE IMMEDIATE 'ALTER INDEX BT_TVP054_IDX4 REBUILD';

  DBMS_STATS.GATHER_TABLE_STATS(ownname=>'MOUTRAN', tabname=>'BT_TVP054', cascade=>DBMS_STATS.AUTO_CASCADE);
  DBMS_STATS.GATHER_TABLE_STATS(ownname=>'MOUTRAN', tabname=>'BT_TVP163', cascade=>DBMS_STATS.AUTO_CASCADE);

  COMMIT;

  l_progress := 'UPDATE MIG_PHASE_KEYGEN';
  l_job.NO_BATCH := no_batch;

  UPDATE MIG_PHASE_KEYGEN
  SET    FG_KEYGEN_PRC = 'Y',
         TS_KEYGEN     = current_date,
         NO_BATCH      = l_job.NO_BATCH
  WHERE  FG_INLC_BATCH = 'Y'
  AND    FG_KEYGEN_PRC = 'N';

  l_progress := 'Add Reconciliation Counts ';

   --CP1	number of rows on BT_TVO54

   l_orig_ins_tvp054 := l_orig_ins_tvp054 - l_dom_delete + l_agg_tvp054 - l_agg_del_tvp054;
   P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP1', 1, l_orig_ins_tvp054, 'Records Written (054)');

   -- commented  out for phase 2
   
--   IF l_orig_ins_tvp054 = 0 THEN
--      l_job.IND_STATUS := 'ERR';
--      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'KEYGEN is empty',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
--      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
--      COMMIT;
--      return_code := -1;
--      RETURN;
--   END IF;

   --CP22	number of Legal Entities

  SELECT COUNT( DISTINCT NO_LEGAL_ENTITY)
  INTO   l_le
  FROM   BT_TVP054;

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP22', 670, l_le,  'Distinct Legal Entities during KEY_GEN stage 2');

   --CP23	number of Eligible Properties with water supply points

  SELECT COUNT( DISTINCT NO_PROPERTY)
  INTO   l_water_sp
  FROM   BT_TVP054           t054,
         LU_SERVICE_CATEGORY tcat
  WHERE  tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
  AND    tcat.SUPPLY_POINT_CODE     = 'W';

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP23', 680, l_water_sp,  'Water Service Provisions');

--CP24	number of Eligible Properties with sewage supply points

  SELECT COUNT( DISTINCT NO_PROPERTY)
  INTO   l_sewage_sp
  FROM   BT_TVP054           t054,
         LU_SERVICE_CATEGORY tcat
  WHERE  tcat.TARGET_SERV_PROV_CODE = t054.CD_SERVICE_PROV
  AND    tcat.SUPPLY_POINT_CODE     = 'S';

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP23', 690, l_sewage_sp,  'Sewerage Service Provisions');

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  COMMIT;

EXCEPTION

WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA  || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;

END P_MOU_TRAN_KEY_GEN;
/
show error;

exit;

