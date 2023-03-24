--
-- Creates key information for all properties on Eligibility Control tables
--
-- Subversion $Revision: 4023 $	
--
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.01      18/04/2016  S.Badhan   Amended to create and populate table
-- V 1.00      12/04/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------

ALTER SESSION ENABLE PARALLEL DML;
commit;

drop table TVMNHHDTL cascade constraints purge
;
--drop public synonym TVMNHHDTL
--;

select 'start', to_char(sysdate,'yyyymmdd_hh24miss') from dual;

CREATE TABLE TVMNHHDTL AS
select /*+ PARALLEL(a,auto) PARALLEL(b,auto) */ 
DISTINCT
A.cd_company_system, A.no_account ,A.no_property, F.cd_service_prov, f.st_serv_prov, A.no_combine_054, A.no_serv_prov,
A.dt_start, A.dt_end, A.nm_local_service, c.cd_property_use, B.no_combine_024, B.tp_cust_acct_role, B.NO_LEGAL_ENTITY,
B.dt_start AS nc024_DT_START, B.dt_end AS nc024_DT_END, D.ind_legal_entity, E.no_combine_163, E.ind_inst_at_prop,
NULL AS tvp202_no_property , NULL AS tvp202_no_serv_prov, CAST( NULL AS CHAR(1)) fg_too_hard
FROM CIS.tvp054servprovresp A LEFT JOIN CIS.tvp202servproveqp E 
     ON  A.no_property        = E.no_property 
     AND A.no_serv_prov      = E.no_serv_prov 
     AND A.cd_company_system = E.cd_company_system, 
     CIS.ELIGIBILITY_CONTROL_TABLE Y, 
     CIS.TVP024CUSTACCTROLE B,
     CIS.tvp046property C, 
     CIS.tvp036legalentity D,
     CIS.tvp056servprov F
WHERE A.no_property       = y.no_property
AND   A.cd_company_system = Y.cd_company_system
AND Y.FG_MECOMS_RDY IN ('1','2','3')
AND   Y.CD_PROPERTY_USE_FUT  NOT IN ('X', 'E', 'D', 'H')
AND   Y.VALIDATED_FLAG = 'Y' 
And   Y.CORESPID IS NOT NULL
and A.cd_company_system = 'STW1'
and B.no_account = A.no_account 
and B.cd_company_system = A.cd_company_system
and C.no_property = A.no_property
and C.cd_company_system = A.cd_company_system
and D.no_legal_entity = B.no_legal_entity
and D.cd_company_system = B.cd_company_system
and F.no_property = A.no_property 
and F.cd_company_system = A.cd_company_system
and F.no_serv_prov = A.no_serv_prov
UNION
select /*+ PARALLEL(a,auto) PARALLEL(b,auto) */ 
DISTINCT A.cd_company_system,A.no_account,A.no_property,F.cd_service_prov,f.st_serv_prov,A.no_combine_054,A.no_serv_prov,
A.dt_start,A.dt_end,A.nm_local_service, c.cd_property_use,B.no_combine_024,B.tp_cust_acct_role,B.NO_LEGAL_ENTITY,
B.dt_start AS nc024_DT_START,B.dt_end AS nc024_DT_END, D.ind_legal_entity, E.no_combine_163, E.ind_inst_at_prop,
e.no_property AS tvp202_no_property, e.no_serv_prov AS tvp202_no_serv_prov, CAST( NULL AS CHAR(1)) fg_too_hard
FROM CIS.tvp054servprovresp A, 
     CIS.ELIGIBILITY_CONTROL_TABLE Y,
     CIS.tvp202servproveqp E, 
     CIS.tvp163equipinst G, 
     CIS.tvp043meterreg H,
     CIS.tvp034instregassgn I,
     CIS.TVP024CUSTACCTROLE B,
     CIS.tvp046property c,
     CIS.tvp036legalentity D,
     CIS.tvp056servprov F,
     CIS.TVP202SERVPROVEQP J,
     CIS.TVP163EQUIPINST K
where E.NO_PROPERTY = y.no_property
AND E.cd_company_system = Y.cd_company_system  
--and Y.fg_mecoms_rdy = 'Y'
AND Y.FG_MECOMS_RDY IN ('1','2','3')
AND   Y.CD_PROPERTY_USE_FUT  NOT IN ('X', 'E', 'D', 'H')
AND   Y.VALIDATED_FLAG = 'Y' 
And   Y.CORESPID IS NOT NULL
AND E.CD_COMPANY_SYSTEM = 'STW1'
and E.ind_inst_at_prop = 'Y'
and G.NO_COMBINE_163 = E.NO_COMBINE_163
AND G.CD_COMPANY_SYSTEM = E.CD_COMPANY_SYSTEM
AND G.ST_EQUIP_INST = 'A'
AND H.NO_EQUIPMENT = G.NO_EQUIPMENT
AND H.CD_COMPANY_SYSTEM = G.CD_COMPANY_SYSTEM
AND I.NO_COMBINE_043 = H.NO_COMBINE_043
AND I.CD_COMPANY_SYSTEM = H.CD_COMPANY_SYSTEM
and A.no_COMBINE_054 = I.NO_COMBINE_054  
and A.cd_company_system = I.CD_COMPANY_SYSTEM 
and A.no_property <> E.no_property
and J.no_property = A.no_property
and J.cd_company_system = A.CD_COMPANY_SYSTEM 
and J.no_combine_163 = E.no_combine_163
and K.no_combine_163 = J.no_combine_163
and K.cd_company_system = J.CD_COMPANY_SYSTEM 
and K.st_equip_inst <> 'X'
and B.no_account = A.no_account 
and B.cd_company_system = A.cd_company_system
and c.no_property = a.no_property
and c.cd_company_system = a.cd_company_system
and c.no_property in 
(SELECT /*+ PARALLEL(x,auto) */ x.no_property 
from  CIS.ELIGIBILITY_CONTROL_TABLE x
WHERE A.no_property = x.no_property 
AND x.FG_MECOMS_RDY IN ('1','2','3')
AND   x.CD_PROPERTY_USE_FUT  NOT IN ('X', 'E', 'D', 'H')
AND   x.VALIDATED_FLAG = 'Y' 
And   x.CORESPID IS NOT NULL
 --AND x.fg_mecoms_rdy = 'Y'
 ) 
and D.no_legal_entity = B.no_legal_entity
and D.cd_company_system = B.cd_company_system
and F.no_property = A.no_property 
AND F.cd_company_system = A.cd_company_system
AND F.no_serv_prov = A.no_serv_prov;


--create public synonym TVMNHHDTL for TVMNHHDTL;
--grant select,insert,delete,update on TVMNHHDTL to vporua;

CREATE INDEX TVMNHHDTL_IDX1 ON TVMNHHDTL
(NO_PROPERTY,CD_COMPANY_SYSTEM)
COMPUTE STATISTICS;

CREATE INDEX TVMNHHDTL_IDX2 ON TVMNHHDTL
(NO_ACCOUNT,CD_COMPANY_SYSTEM)
COMPUTE STATISTICS;

CREATE INDEX TVMNHHDTL_IDX3 ON TVMNHHDTL
(NO_LEGAL_ENTITY,CD_COMPANY_SYSTEM)
COMPUTE STATISTICS;

commit;

select 'end',to_char(sysdate,'yyyymmdd_hh24miss') from dual;

exit;