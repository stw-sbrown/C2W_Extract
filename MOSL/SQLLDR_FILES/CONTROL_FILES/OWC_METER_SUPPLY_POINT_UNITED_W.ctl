--
-- Subversion $Revision: 5800 $	
--
LOAD data append into table OWC_METER_SUPPLY_POINT_W
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
SPID_PK,
SAPEQUIPMENT,
OWC                  CONSTANT 'UNITED-W'
)
