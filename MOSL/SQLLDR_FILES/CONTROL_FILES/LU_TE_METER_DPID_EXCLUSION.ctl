--
-- Subversion $Revision: 6324 $	
--
OPTIONS (SKIP=1)
load data
into table LU_TE_METER_DPID_EXCLUSION
fields terminated by "|" TRAILING NULLCOLS
(
    DPID_PK,
    NM_PREFERRED,
    STWACCOUNTNUMBER,
    STWPROPERTYNUMBER_PK,
    MANUFACTURER_PK               "UPPER(:MANUFACTURER_PK)",
    MANUFACTURERSERIALNUM_PK      "UPPER(:MANUFACTURERSERIALNUM_PK)"
)
