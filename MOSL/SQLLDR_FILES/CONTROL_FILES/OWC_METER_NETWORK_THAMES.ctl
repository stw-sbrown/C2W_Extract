--
-- Subversion $Revision: 5458 $	
--
LOAD data append into table OWC_METER_NETWORK
fields terminated by "|" TRAILING NULLCOLS
(
SPID_PK,
MAINMANUFACTURERSERIALNUM,
MAINMANUFACTURER,
MAINMETERTREATMENT,
SUBMANUFACTURERSERIALNUM,
SUBMANUFACTURER,
SUBMETERTREATMENT,
MAINNONMARKETFLAG,
MAINSAPEQUIPMENT,
SUBSAPEQUIPMENT,
OWC                  CONSTANT 'THAMES'
)