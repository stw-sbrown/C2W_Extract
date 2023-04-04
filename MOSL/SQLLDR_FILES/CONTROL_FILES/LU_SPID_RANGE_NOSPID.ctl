--
-- Subversion $Revision: 5958 $	
--
load data
into table LU_SPID_RANGE_NOSPID
fields terminated by "|"
(
NOSPID_SPID,
SPID_PK,
CORESPID_PK
)