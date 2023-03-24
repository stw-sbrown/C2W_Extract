--
-- Creates key information for all properties on Eligibility Control tables
--
-- Subversion $Revision: 5284 $	
--
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.07      25/08/2016  S.Badhan   I-320 - Updated with Phils latest code-version. Change to insert rather than create table.
--                                    FC6062_LOAD_KEYGEN (minus delete agg subs and phase parts) 
-- V 1.06      04/08/2016  D.Cheung   I-299 - Updated with Phils latest code-version - FC6062_LOAD_KEYGEN (minus delete agg subs and phase parts)
-- V 1.05      28/06/2016  S.Badhan   CR_021  Change selection criteria and add new columns from ECT.
-- V 1.01      18/04/2016  S.Badhan   Amended to create and populate table
-- V 1.00      12/04/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------
SET VERIFY OFF
SET HEADING OFF
select 'start', to_char(sysdate,'yyyymmdd_hh24miss') from dual;
SET HEADING ON
SET TERMOUT ON
SET TIMING ON

ALTER SESSION ENABLE PARALLEL DML;

PROMPT Truncate TVMNHHDTL

TRUNCATE TABLE RECEPTION.TVMNHHDTL;

PROMPT Load TVMNHHDTL


/*------------------------------------------------------------------------------*/
/* TVMNHHDTL holds detail of all SPRs and custacctroles for eligible Properties */
/*------------------------------------------------------------------------------*/

INSERT /*+ append */                              
INTO RECEPTION.TVMNHHDTL
(cd_company_system,
no_account,
no_property,
cd_service_prov,
st_serv_prov,
no_combine_054,
no_serv_prov,
dt_start,
dt_end,
nm_local_service,
cd_property_use,
no_combine_024,
tp_cust_acct_role,
no_legal_entity,
nc024_dt_start,
nc024_dt_end,
ind_legal_entity,
no_combine_163,
ind_inst_at_prop,
tvp202_no_property,
tvp202_no_serv_prov,
fg_too_hard,
phase,
corespid,
agg_net,
no_property_master,
id_owc,
supply_point_category)
SELECT /*+ PARALLEL(t054,12) PARALLEL(ect,12) PARALLEL(t024,12) PARALLEL(t202,12)*/
       DISTINCT t054.cd_company_system
     , t054.no_account
     , t054.no_property
     , t056.cd_service_prov
     , t056.st_serv_prov
     , t054.no_combine_054
     , t054.no_serv_prov
     , t054.dt_start
     , t054.dt_end
     , t054.nm_local_service
     , t046.cd_property_use
     , t024.no_combine_024
     , t024.tp_cust_acct_role
     , t024.no_legal_entity
     , t024.dt_start 
     , t024.dt_end   
     , t036.ind_legal_entity
     , t202.no_combine_163
     , t202.ind_inst_at_prop
     , NULL 
     , NULL  
     , NULL  
     , ect.fg_mecoms_rdy 
     , ect.corespid
     , ect.agg_net
     , ect.no_property_master 
     , ect.id_owc 
     , CASE
          WHEN(t056.cd_service_prov IN('W','UW')) 
               THEN 'W' 
          WHEN(t056.cd_service_prov LIKE 'XW%')
               THEN 'W'
          ELSE 'S'
        END 
    FROM CIS.tvp054servprovresp t054
       LEFT JOIN CIS.tvp202servproveqp t202
              ON (t054.no_property       = t202.no_property
              AND t054.no_serv_prov      = t202.no_serv_prov
              AND t054.cd_company_system = t202.cd_company_system)
     , CIS.tvp024custacctrole t024
     , CIS.tvp046property t046
     , CIS.tvp036legalentity t036
     , CIS.tvp056servprov t056
     , CIS.eligibility_control_table ect
------------------------------------------------------
 WHERE t054.no_property       = ect.no_property
   AND t054.cd_company_system = ect.cd_company_system
   AND t054.cd_company_system = 'STW1'
   AND ect.fg_mecoms_rdy      in ('1','2','3')
   AND ect.corespid           is not null
------------------------------------------------------
   AND t054.no_property       = t056.no_property
   AND t054.no_serv_prov      = t056.no_serv_prov
   AND t054.cd_company_system = t056.cd_company_system
------------------------------------------------------
   AND t054.no_property       = t046.no_property
   AND t054.cd_company_system = t046.cd_company_system
------------------------------------------------------
   AND t054.no_account        = t024.no_account
   AND t054.cd_company_system = t024.cd_company_system
------------------------------------------------------
   AND t024.no_legal_entity   = t036.no_legal_entity
   AND t024.cd_company_system = t036.cd_company_system
UNION
SELECT /*+ PARALLEL(t202,12) PARALLEL(ect,12) PARALLEL(ect2,12) PARALLEL(t054,12) PARALLEL(t024,12) PARALLEL(t043,12) PARALLEL(t034,12)*/
       DISTINCT t054.cd_company_system
     , t054.no_account
     , t054.no_property
     , t056.cd_service_prov
     , t056.st_serv_prov
     , t054.no_combine_054
     , t054.no_serv_prov
     , t054.dt_start
     , t054.dt_end
     , t054.nm_local_service
     , t046.cd_property_use
     , t024.no_combine_024
     , t024.tp_cust_acct_role
     , t024.no_legal_entity
     , t024.dt_start 
     , t024.dt_end   
     , t036.ind_legal_entity
     , t202.no_combine_163
     , t202.ind_inst_at_prop
     , t202.no_property  
     , t202.no_serv_prov 
     , NULL 
     , ect2.fg_mecoms_rdy 
     , ect2.corespid
     , ect2.agg_net 
     , ect2.no_property_master
     , ect2.id_owc 
     , CASE
          WHEN(t056.cd_service_prov IN('W','UW'))  
               THEN 'W' 
          WHEN(t056.cd_service_prov LIKE 'XW%')
               THEN 'W'
          ELSE 'S'
        END      
  FROM CIS.tvp054servprovresp t054
     , CIS.tvp202servproveqp t202
     , CIS.tvp163equipinst t163
     , CIS.tvp043meterreg t043
     , CIS.tvp034instregassgn t034
     , CIS.tvp024custacctrole t024
     , CIS.tvp046property t046
     , CIS.tvp036legalentity t036
     , CIS.tvp056servprov t056
     , CIS.tvp202servproveqp t202_spr
     , CIS.tvp163equipinst t163_spr
     , CIS.eligibility_control_table ect
     , CIS.eligibility_control_table ect2
-----------------------------------------------------
 WHERE t202.no_property       = ect.no_property
   AND t202.cd_company_system = ect.cd_company_system
   AND t202.cd_company_system = 'STW1'
   AND t202.ind_inst_at_prop  = 'Y'
   AND ect.fg_mecoms_rdy      in ('1','2','3')
   AND ect.corespid           is not null
------------------------------------------------------
   AND t202.no_combine_163    = t163.no_combine_163
   AND t202.cd_company_system = t163.cd_company_system
   AND t163.st_equip_inst     = 'A' --Available
------------------------------------------------------
   AND t163.no_equipment      = t043.no_equipment
   AND t163.cd_company_system = t043.cd_company_system
------------------------------------------------------
   AND t043.no_combine_043    = t034.no_combine_043
   AND t043.cd_company_system = t034.cd_company_system
------------------------------------------------------
   AND t034.no_combine_054    = t054.no_combine_054
   AND t034.cd_company_system = t054.cd_company_system
   AND t034.fg_add_subtract   = '+'
   AND t034.dt_end            is null -- only want 'active' aggregations
   AND t054.no_property      <> t202.no_property
-----------------------------------------------------------
   AND t054.no_property        = t202_spr.no_property
   AND t054.cd_company_system  = t202_spr.cd_company_system
   AND t054.dt_end             is null -- only want 'active' aggregations
   AND t202_spr.no_combine_163 = t202.no_combine_163
--------------------------------------------------------------
   AND t202_spr.no_combine_163    = t163_spr.no_combine_163
   AND t202_spr.cd_company_system = t163_spr.cd_company_system
   AND t163_spr.st_equip_inst    <> 'X' --Exchanged
--------------------------------------------------------------
   AND t054.no_property       = t046.no_property
   AND t054.cd_company_system = t046.cd_company_system
   AND t046.no_property = ect2.no_property
   AND ect2.fg_mecoms_rdy in ('1','2','3')
   AND ect2.corespid is not null
   AND ect2.cd_company_system = t046.cd_company_system
------------------------------------------------------
   AND t054.no_property       = t056.no_property
   AND t054.cd_company_system = t056.cd_company_system
   AND t054.no_serv_prov      = t056.no_serv_prov
------------------------------------------------------
   AND t054.no_account        = t024.no_account
   AND t054.cd_company_system = t024.cd_company_system
------------------------------------------------------
   AND t024.no_legal_entity   = t036.no_legal_entity
   AND t024.cd_company_system = t036.cd_company_system
UNION
SELECT /*+ PARALLEL(t056,12) PARALLEL(ect,12) PARALLEL(t202,12)*/
       DISTINCT ect.cd_company_system
     , NULL
     , t056.no_property
     , t056.cd_service_prov
     , t056.st_serv_prov
     , NULL
     , t056.no_serv_prov
     , t056.dt_status
     , NULL
     , 'VOID'
     , t046.cd_property_use
     , NULL
     , 'V'
     , 0
     , NULL 
     , NULL  
     , 'V'
     , t202.no_combine_163
     , t202.ind_inst_at_prop
     , NULL 
     , NULL  
     , NULL  
     , ect.fg_mecoms_rdy 
     , ect.corespid
     , ect.agg_net
     , ect.no_property_master
     , ect.id_owc 
     , CASE
          WHEN(t056.cd_service_prov IN('W','UW')) 
               THEN 'W' 
          WHEN(t056.cd_service_prov LIKE 'XW%')
               THEN 'W'
          ELSE 'S'
        END     
  FROM CIS.tvp056servprov t056
       LEFT JOIN CIS.tvp202servproveqp t202
              ON (t056.no_property       = t202.no_property
              AND t056.no_serv_prov      = t202.no_serv_prov
              AND t056.cd_company_system = t202.cd_company_system)
     , CIS.tvp046property t046
     , CIS.eligibility_control_table ect
------------------------------------------------------
 WHERE t056.no_property       = ect.no_property
   AND t056.cd_company_system = ect.cd_company_system
   AND t056.cd_company_system = 'STW1'
   AND t056.st_serv_prov      in ('A','G','V')
   AND ect.fg_mecoms_rdy      in ('1','2','3')
   AND ect.corespid           is not null
------------------------------------------------------
   AND t056.no_property       = t046.no_property
   AND t056.cd_company_system = t046.cd_company_system
------------------------------------------------------
   AND NOT EXISTS
   ( SELECT t054.no_combine_054 from CIS.tvp054servprovresp t054
   WHERE t056.no_property      = t054.no_property
   AND t056.no_serv_prov       = t054.no_serv_prov
   AND t056.cd_company_system  = t054.cd_company_system
   AND (t054.dt_end is null
      OR (t054.dt_end is not null AND
          t054.dt_start <> t054.dt_end)
       )
   )
   ;

COMMIT;

PROMPT Gather Stats TVMNHHDTL

EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname=>'RECEPTION', tabname=>'TVMNHHDTL', cascade=>DBMS_STATS.AUTO_CASCADE);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*-----------------------------------------------------------------*/
/* Update a number of identified invalid SPR entires as 'too hard' */
/*-----------------------------------------------------------------*/

PROMPT set too hard

Update RECEPTION.TVMNHHDTL
set fg_too_hard = 'X'
where no_combine_054 in
(536081436,
536224471,
536224470,
536081436,
536301292,
547085407,
536301291,
547085408,
547085409,
536224471,
536342407,
536315365,
536315366,
536246329,
536010177,
536089395,
536233526,
536233525,
536321906,
536321905,
536334228,
536334229,
467070300,
748005337,
799033757,
332003730,
748005336
)
;

COMMIT
;
/*-----------------------------------------------------------------*/
/* Need to ensure that accounts are not present in multiple phases */
/*-----------------------------------------------------------------*/

PROMPT align active Phase 3

update RECEPTION.TVMNHHDTL a
set a.phase = '3'
where  A.CD_COMPANY_SYSTEM = 'STW1'
               AND A.FG_TOO_HARD IS NULL
               AND (A.PHASE = '1'
               or a.phase = '2')
               AND A.DT_END IS NULL
                AND A.ST_SERV_PROV IN ('A','G','V')
                AND A.TP_CUST_ACCT_ROLE = 'P'
                AND A.NC024_DT_END IS NULL
AND EXISTS
(select c.no_account from RECEPTION.TVMNHHDTL c
where c.cd_COMPANY_SYSTEM = 'STW1'
               AND c.FG_TOO_HARD IS NULL
               AND c.PHASE = '3'
               and c.no_account = a.no_account
               AND c.DT_END IS NULL
                 AND c.ST_SERV_PROV IN ('A','G','V')
                AND c.TP_CUST_ACCT_ROLE = 'P'
                AND c.NC024_DT_END IS NULL
               )
;

COMMIT
;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

PROMPT align inactive Phase 3

update RECEPTION.TVMNHHDTL a
set a.phase = '3'
where  A.CD_COMPANY_SYSTEM = 'STW1'
               AND A.FG_TOO_HARD IS NULL
               AND (A.PHASE = '1'
               or a.phase = '2')
AND EXISTS
(select c.no_property from RECEPTION.TVMNHHDTL c
where c.cd_COMPANY_SYSTEM = 'STW1'
               AND c.FG_TOO_HARD IS NULL
               AND c.PHASE = '3'
               and c.no_property = a.no_property
               )
;

COMMIT
;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


PROMPT align active Phase 2

update RECEPTION.TVMNHHDTL a
set a.phase = '2'
where  A.CD_COMPANY_SYSTEM = 'STW1'
               AND A.FG_TOO_HARD IS NULL
               AND A.PHASE = '1'
                AND A.DT_END IS NULL
                AND A.ST_SERV_PROV IN ('A','G','V')
                AND A.TP_CUST_ACCT_ROLE = 'P'
                AND A.NC024_DT_END IS NULL
AND EXISTS
(select c.no_account from RECEPTION.TVMNHHDTL c
where c.cd_COMPANY_SYSTEM = 'STW1'
               AND c.FG_TOO_HARD IS NULL
               AND c.PHASE = '2'
               and c.no_account = a.no_account
               AND c.DT_END IS NULL
                 AND c.ST_SERV_PROV IN ('A','G','V')
                AND c.TP_CUST_ACCT_ROLE = 'P'
                AND c.NC024_DT_END IS NULL
               )
;

COMMIT
;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROMPT align inactive Phase 2

update RECEPTION.TVMNHHDTL a
set a.phase = '2'
where  A.CD_COMPANY_SYSTEM = 'STW1'
               AND A.FG_TOO_HARD IS NULL
               AND A.PHASE = '1'
AND EXISTS
(select c.no_property from RECEPTION.TVMNHHDTL c
where c.cd_COMPANY_SYSTEM = 'STW1'
               AND c.FG_TOO_HARD IS NULL
               AND c.PHASE = '2'
               and c.no_property = a.no_property
               )
;

COMMIT
;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROMPT align active Phase 3 - AGAIN

update RECEPTION.TVMNHHDTL a
set a.phase = '3'
where  A.CD_COMPANY_SYSTEM = 'STW1'
               AND A.FG_TOO_HARD IS NULL
               AND (A.PHASE = '1'
               or a.phase = '2')
               AND A.DT_END IS NULL
                AND A.ST_SERV_PROV IN ('A','G','V')
                AND A.TP_CUST_ACCT_ROLE = 'P'
                AND A.NC024_DT_END IS NULL
AND EXISTS
(select c.no_account from RECEPTION.TVMNHHDTL c
where c.cd_COMPANY_SYSTEM = 'STW1'
               AND c.FG_TOO_HARD IS NULL
               AND c.PHASE = '3'
               and c.no_account = a.no_account
               AND c.DT_END IS NULL
                 AND c.ST_SERV_PROV IN ('A','G','V')
                AND c.TP_CUST_ACCT_ROLE = 'P'
                AND c.NC024_DT_END IS NULL
               )
;

COMMIT
;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

PROMPT align inactive Phase 3 - AGAIN

update RECEPTION.TVMNHHDTL a
set a.phase = '3'
where  A.CD_COMPANY_SYSTEM = 'STW1'
               AND A.FG_TOO_HARD IS NULL
               AND (A.PHASE = '1'
               or a.phase = '2')
AND EXISTS
(select c.no_property from RECEPTION.TVMNHHDTL c
where c.cd_COMPANY_SYSTEM = 'STW1'
               AND c.FG_TOO_HARD IS NULL
               AND c.PHASE = '3'
               and c.no_property = a.no_property
               )
;

COMMIT
;


SET TIMING OFF
SET HEADING OFF
select 'end',to_char(sysdate,'yyyymmdd_hh24miss') from dual;
SET HEADING ON

exit;