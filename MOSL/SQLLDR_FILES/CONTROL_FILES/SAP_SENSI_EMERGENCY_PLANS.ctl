--
-- Subversion $Revision: 4849 $	
--
LOAD data
into table SAP_SENSI_EMERGENCY_PLANS
fields terminated by "|"
(
STWPROPERTYNUMBER_PK,
NONPUBHEALTHRELSITE,
NONPUBHEALTHRELSITEDSC,
PUBHEALTHRELSITEARR,
PUBHEALTHRELSITEDSC
)
