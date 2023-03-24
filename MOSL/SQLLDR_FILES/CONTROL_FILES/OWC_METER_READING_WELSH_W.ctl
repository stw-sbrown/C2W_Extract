--
-- Subversion $Revision: 5666 $	
--
LOAD data append into table OWC_METER_READING_W
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
METERREAD,
METERREADMETHOD,
METERREADDATE                     "to_date(:METERREADDATE,'DD/MM/YYYY')",                                                                                                                                                                             
ROLLOVERINDICATOR,
ESTIMATEDREADREASONCODE,
ESTIMATEDREADREMEDIALWORKIND,
METERREADTYPE,
PREVIOUSMETERREADING,
METERREF,
PREVMETERREF,
SAPEQUIPMENT,
OWC                  CONSTANT 'DWRCYMRU-W'
)