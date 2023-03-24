--
-- Subversion $Revision: 5600 $	
--
load data
into table LU_OWC_TARIFF
fields terminated by "|"
(
TARIFF_TYPE,
OWCTARIFFCODE_PK,
STWTARIFFCODE_PK,
DESCRIPTION,
WHOLESALERID_PK
)