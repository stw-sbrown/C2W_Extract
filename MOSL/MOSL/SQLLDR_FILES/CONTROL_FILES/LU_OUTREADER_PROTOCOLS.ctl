--
-- Subversion $Revision: 4023 $	
--
load data
into table LU_OUTREADER_PROTOCOLS
fields terminated by "|"
(
MANUFACTURER_PK, 
READMETHOD_PK,
OUTREADERPROTOCOL
)