--
-- Subversion $Revision: 5666 $	
--
LOAD data append into table OWC_DISCHARGE_POINT
fields terminated by "|" TRAILING NULLCOLS
(
SPID_PK,
DPID_PK,
TARRIFCODE,
TARRIFBAND,
TREFODCHEMOXYGENDEMAND,
TREFODCHEMSUSPSOLDEMAND,
TREFODCHEMAMONIANITROGENDEMAND,
TREFODCHEMCOMPXDEMAND,
TREFODCHEMCOMPYDEMAND,
TREFODCHEMCOMPZDEMAND,
SEWERAGEVOLUMEADJMENTHOD,
RECEPTIONTREATMENTINDICATOR,
PRIMARYTREATMENTINDICATOR,
MARINETREATMENTINDICATOR,
BIOLOGICALTREATMENTINDICATOR,
SLUDGETREATMENTINDICATOR,
AMMONIATREATMENTINDICATOR,
TEFXTREATMENTINDICATOR,
TEFYTREATMENTINDICATOR,
TEFZTREATMENTINDICATOR,
TEFAVAILABILITYDATAX,
TEFAVAILABILITYDATAY,
TEFAVAILABILITYDATAZ,
CHARGEABLEDAILYVOL,
CHEMICALOXYGENDEMAND,
SUSPENDEDSOLIDSLOAD,
AMMONIANITROCAL,
FIXEDALLOWANCE,
PERCENTAGEALLOWANCE,
DOMMESTICALLOWANCE,
SEASONALFACTOR,
DPIDSPECIALAGREEMENTINPLACE,
DPIDSPECIALAGREEMENTFACTOR,
DPIDSPECIALAGREEMENTREFERENCE,
FREETEXTDESCRIPTOR,
SECONDADDRESSABLEOBJ,
PRIMARYADDRESSABLEOBJ,
ADDRESSLINE01,
ADDRESSLINE02,
ADDRESSLINE03,
ADDRESSLINE04,
ADDRESSLINE05,
POSTCODE,
PAFADDRESSKEY,
VALIDTETARIFFCODE                  CONSTANT 1,
TARIFFBANDCOUNT                  CONSTANT 0,
AMMONIACALNITROGEN                  CONSTANT 0,
XCOMP                  CONSTANT 0,
YCOMP                  CONSTANT 0,
ZCOMP                  CONSTANT 0,
STWIWCS,
SAPFLOCNUMBER,
STWCONSENTNUMBER,
OWC                  CONSTANT 'ANGLIAN-W'
)
