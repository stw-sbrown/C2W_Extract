--
-- Subversion $Revision: 5575 $	
--
LOAD data append into table OWC_METER_READING
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
METERREAD,
METERREADMETHOD,
METERREADDATE                     "to_date(:METERREADDATE,'YYYY-MM-DD')",                                                                                                                                                                             
ROLLOVERINDICATOR,
ESTIMATEDREADREASONCODE,
ESTIMATEDREADREMEDIALWORKIND,
METERREADTYPE,
PREVIOUSMETERREADING,
METERREF,
PREVMETERREF,
SAPEQUIPMENT,
OWC                  CONSTANT 'SOUTHSTAFF-W'
)