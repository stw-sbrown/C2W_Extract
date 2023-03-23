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
-- Subversion $Revision: 4023 $
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
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_water_sp                    NUMBER(9);
  l_sewage_sp                   NUMBER(9);
  l_le                          NUMBER(9);
  
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

   l_progress := 'truncating table';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_TVP054';


  l_progress := 'populating table';
   
  EXECUTE IMMEDIATE
  'INSERT /*+ append */ INTO BT_TVP054
   (CD_COMPANY_SYSTEM, NO_ACCOUNT, NO_PROPERTY, CD_SERVICE_PROV, NO_COMBINE_054, NO_SERV_PROV, DT_START, DT_END,
   NM_LOCAL_SERVICE, CD_PROPERTY_USE, NO_COMBINE_024, TP_CUST_ACCT_ROLE, NO_LEGAL_ENTITY, nc024_DT_START, nc024_DT_END,
   IND_LEGAL_ENTITY, 
   --NO_COMBINE_163, IND_INST_AT_PROP, TVP202_NO_PROPERTY, TVP202_NO_SERV_PROV, 
   FG_TOO_HARD, CD_PROPERTY_USE_ORIG,
   CD_PROPERTY_USE_CURR, CD_PROPERTY_USE_FUT, UDPRN, UPRN, VOA_REFERENCE, SAP_FLOC, CORESPID, AGG_NET, FG_CONSOLIDATED)   
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
   --    NO_COMBINE_163,
  --     IND_INST_AT_PROP, 
  --     TVP202_NO_PROPERTY, 
  --     TVP202_NO_SERV_PROV,
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
       FG_CONSOLIDATED       
FROM       
(SELECT  tnhh.CD_COMPANY_SYSTEM,
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
--       tnhh.NO_COMBINE_163,
--       tnhh.IND_INST_AT_PROP, 
--       tnhh.TVP202_NO_PROPERTY, 
--       tnhh.TVP202_NO_SERV_PROV,
       tnhh.FG_TOO_HARD,
       telg.CD_PROPERTY_USE_ORIG,
	     telg.CD_PROPERTY_USE_CURR, 
	     telg.CD_PROPERTY_USE_FUT,
	     telg.UDPRN,
	     telg.UPRN,
       telg.VOA_REFERENCE,
       telg.SAP_FLOC,
       telg.CORESPID, 
       telg.AGG_NET, 
       telg.FG_CONSOLIDATED,
       ROW_NUMBER() OVER ( PARTITION BY tnhh.NO_PROPERTY, tnhh.NO_SERV_PROV ORDER BY tnhh.NO_PROPERTY, tnhh.NO_SERV_PROV, tnhh.DT_END desc NULLS FIRST, nc024_DT_START desc ) AS Record_Nr
FROM   TVMNHHDTL tnhh,
       CIS.ELIGIBILITY_CONTROL_TABLE telg
WHERE  tnhh.CD_COMPANY_SYSTEM = ''STW1''
AND    tnhh.TP_CUST_ACCT_ROLE = ''P''
AND    tnhh.ST_SERV_PROV      in (''A'',''C'',''E'',''G'',''V'')
AND    telg.CD_COMPANY_SYSTEM = tnhh.CD_COMPANY_SYSTEM
AND    telg.NO_PROPERTY       = tnhh.NO_PROPERTY    
and    tnhh.DT_START  <> nvl(tnhh.DT_END,to_date(''31/12/2099'',''dd/mm/yyyy'')) ) x
WHERE  Record_Nr = 1';                                  

  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP1', 1, SQL%ROWCOUNT, 'Records Written (054)');
   
  --l_job.IND_STATUS := 'END';
  --P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);  

  COMMIT;


    l_progress := 'truncating METER KeyGen table';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_TVP163';

    l_progress := 'populating METER KeyGen table';
   
    EXECUTE IMMEDIATE
        -- Current or Last SPR meters 
      'INSERT /*+ append */ INTO BT_TVP163
        (cd_company_system, no_account, no_property, cd_service_prov, no_combine_054, no_serv_prov, dt_start_054, dt_end_054,
            nm_local_service, no_combine_034, dt_start_034, dt_start_lr_034, dt_end_034, FG_ADD_SUBTRACT, NO_UTL_EQUIP,NO_EQUIPMENT, 
            no_combine_043, NO_REGISTER, TP_EQUIPMENT, CD_TARIFF, NO_TARIFF_GROUP, NO_TARIFF_SET, PC_USAGE_SPLIT, AM_BILL_MULTIPLIER, 
            ST_METER_REG_115, no_combine_163_inst, no_property_inst, tvp202_no_serv_prov_inst, IND_Market_Prop_Inst, fg_too_hard, CORESPID, AGG_NET, FG_CONSOLIDATED)
        select  
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
                 then NVL((select ''Y'' from BT_TVP054 m          
                    WHERE m.NO_PROPERTY = t202.no_property
                    AND ROWNUM = 1),''N'')
                else ''Y''
            end as IND_Market_Prop_Inst,
            cast( null as char(1)) fg_too_hard,
            keygen.CORESPID,
            keygen.AGG_NET, 
            keygen.FG_CONSOLIDATED
        FROM CIS.tvp202servproveqp  t202, 
            CIS.tvp163equipinst    t163, 
            CIS.tvp043meterreg     t043, 
            CIS.tvp034instregassgn t034,
            cis.tvp063equipment    t063,
            (SELECT distinct 
                no_combine_054, no_account, 
                no_property, cd_service_prov, no_serv_prov, nm_local_service,
                dt_start, dt_end, CORESPID, AGG_NET, FG_CONSOLIDATED
              FROM BT_TVP054           
             ) keygen
        WHERE 
            -- Get Tariff logical registers for Eligable SPRs. On end dated SPRs, so will the register be.
            t034.NO_COMBINE_054    = keygen.no_combine_054
        AND ( t034.DT_END IS NULL or (keygen.dt_end is not null and t034.DT_END = keygen.dt_end) )
        --AND t034.CD_COMPANY_SYSTEM = ''STW1''
            -- Get Physical Meter using register
        AND t043.NO_COMBINE_043    = t034.NO_COMBINE_043
        AND t043.CD_COMPANY_SYSTEM = t034.cd_company_system
            -- get Equipment Info
        AND t063.NO_EQUIPMENT      = t043.NO_EQUIPMENT
        AND t063.CD_COMPANY_SYSTEM = t043.cd_company_system
            -- Get Equipment Locations (logical and physical)
        AND T163.NO_EQUIPMENT      = t043.NO_EQUIPMENT
        AND T163.ST_EQUIP_INST     = ''A''
        AND T163.CD_COMPANY_SYSTEM = t043.cd_company_system
            -- Get Physical Location
        AND T202.NO_COMBINE_163     = t163.NO_COMBINE_163
        AND t202.CD_COMPANY_SYSTEM = t163.cd_company_system
        and t202.ind_inst_at_prop  = ''Y''';


    P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP1', 1, SQL%ROWCOUNT, 'Records Written (163)');
   
    l_job.IND_STATUS := 'END';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);  

    COMMIT;
  
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

