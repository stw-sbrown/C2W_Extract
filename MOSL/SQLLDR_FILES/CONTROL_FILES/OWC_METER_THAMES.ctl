--
-- Subversion $Revision: 5575 $	
--
LOAD data append into table OWC_METER
fields terminated by "|" TRAILING NULLCOLS
(
MANUFACTURERSERIALNUM_PK,
MANUFACTURER_PK,
NUMBEROFDIGITS,
MEASUREUNITATMETER,
MEASUREUNITFREEDESCRIPTOR,
PHYSICALMETERSIZE,
METERREADFREQUENCY,
INITIALMETERREADDATE                     "to_date(:INITIALMETERREADDATE,'DD/MM/YYYY')",     
RETURNTOSEWER,
WATERCHARGEMETERSIZE,
SEWCHARGEABLEMETERSIZE,
DATALOGGERWHOLESALER,
DATALOGGERNONWHOLESALER,
GPSX,
GPSY,
METERLOCFREEDESCRIPTOR,
METEROUTREADERGPSX,
METEROUTREADERGPSY,
OUTREADERLOCFREEDES,
METEROUTREADERLOCCODE,
METERTREATMENT,
METERLOCATIONCODE,
COMBIMETERFLAG,
YEARLYVOLESTIMATE,
REMOTEREADFLAG,
REMOTEREADTYPE,
OUTREADERID,
OUTREADERPROTOCOL,
LOCATIONFREETEXTDESCRIPTOR,
SECONDADDRESABLEOBJECT,
PRIMARYADDRESSABLEOBJECT,
ADDRESSLINE01,
ADDRESSLINE02,
ADDRESSLINE03,
ADDRESSLINE04,
ADDRESSLINE05,
POSTCODE,
PAFADDRESSKEY,
SPID,
NONMARKETMETERFLAG,
SAPEQUIPMENT,
SAPFLOCNUMBER,
OWC                  CONSTANT 'THAMES-W'
)
