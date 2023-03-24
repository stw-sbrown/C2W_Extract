--
-- Subversion $Revision: 5285 $	
--
LOAD data append into table SAP_METER_SUPPLY_POINT
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
SPID_PK,
SAPEQUIPMENT,
OWC               CONSTANT 'DWRCYMRU'
)
