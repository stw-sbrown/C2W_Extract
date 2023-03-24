--
-- Subversion $Revision: 4023 $	
--
load data
into table LU_CROSSBORDER
fields terminated by "|"
(
STWPROPERTYNUMBER_PK,
    CORESPID_PK,
    SPID_PK,
    SERVICECATEGORY,
    RETAILERID_PK,
    WHOLESALERID_PK 
)
