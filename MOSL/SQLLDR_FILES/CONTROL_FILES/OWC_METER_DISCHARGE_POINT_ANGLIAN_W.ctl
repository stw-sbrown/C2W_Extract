--
-- Subversion $Revision: 5661 $	
--
LOAD data append into table OWC_METER_DISCHARGE_POINT_W
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
DPID_PK,
PERCENTAGEDISCHARGE,
SAPEQUIPMENT,
OWC                  CONSTANT 'ANGLIAN-W'
)
