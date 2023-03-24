--
-- Subversion $Revision: 5458 $	
--
LOAD data append into table OWC_METER_SUPPLY_POINT
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
SPID_PK,
SAPEQUIPMENT,
OWC                  CONSTANT 'THAMES'
)
