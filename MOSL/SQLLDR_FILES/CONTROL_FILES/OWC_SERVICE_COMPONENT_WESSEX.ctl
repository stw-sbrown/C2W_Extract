--
-- Subversion $Revision: 5600 $	
--
LOAD data append into table OWC_SERVICE_COMPONENT
fields terminated by "|" TRAILING NULLCOLS
(
SPID_PK,
METEREDPWTARIFFCODE,
MPWSPECIALAGREEMENTFLAG,
MPWSPECIALAGREEMENTFACTOR,
MPWSPECIALAGREEMENTREF,
METEREDPWMAXDAILYDEMAND,
MPWMAXIMUMDEMANDTARIFF,
DAILYRESERVEDCAPACITY,
--MPWDAILYSTANDBYUSAGEVOLCHARGE,
METEREDNPWTARIFFCODE,
MNPWSPECIALAGREEMENTFLAG,
MNPWSPECIALAGREEMENTFACTOR,
MNPWSPECIALAGREEMENTREF,
METEREDNPWMAXDAILYDEMAND,
--MNPWMAXIMUMDEMANDTARIFF,
--METEREDNPWDAILYRESVDCAPACITY,
--MNPWDAILYSTANDBYUSAGEVOLCHARGE,
AWASSESSEDTARIFFCODE,
AWSPECIALAGREEMENTFLAG,
AWSPECIALAGREEMENTFACTOR,
AWSPECIALAGREEMENTREF,
AWASSESSEDCHARGEMETERSIZE,
AWASSESSEDDVOLUMETRICRATE,
AWASSESSEDTARIFBAND,
--AWFIXEDCHARGE,
--AWVOLUMETRICCHARGE,
--AWTARIFFBAND,
UWUNMEASUREDTARIFFCODE,
UWSPECIALAGREEMENTFLAG,
UWSPECIALAGREEMENTFACTOR,
UWSPECIALAGREEMENTREF,
UWUNMEASUREDTYPEACOUNT,
UWUNMEASUREDTYPEADESCRIPTION,
UWUNMEASUREDTYPEBCOUNT,
UWUNMEASUREDTYPEBDESCRIPTION,
UWUNMEASUREDTYPECCOUNT,
UWUNMEASUREDTYPECDESCRIPTION,
UWUNMEASUREDTYPEDCOUNT,
UWUNMEASUREDTYPEDDESCRIPTION,
UWUNMEASUREDTYPEECOUNT,
UWUNMEASUREDTYPEEDESCRIPTION,
UWUNMEASUREDTYPEFCOUNT,
UWUNMEASUREDTYPEFDESCRIPTION,
UWUNMEASUREDTYPEGCOUNT,
UWUNMEASUREDTYPEGDESCRIPTION,
UWUNMEASUREDTYPEHCOUNT,
UWUNMEASUREDTYPEHDESCRIPTION,
UWPIPESIZE,
WADJCHARGEADJTARIFFCODE,
METEREDFSTARIFFCODE,
MFSSPECIALAGREEMENTFLAG,
MFSSPECIALAGREEMENTFACTOR,
MFSSPECIALAGREEMENTREF,
ASASSESSEDTARIFFCODE,
ASSPECIALAGREEMENTFLAG,
ASSPECIALAGREEMENTFACTOR,
ASSPECIALAGREEMENTREF,
ASASSESSEDCHARGEMETERSIZE,
ASASSESSEDDVOLUMETRICRATE,
ASASSESSEDTARIFBAND,
--ASFIXEDCHARGE,
--ASVOLMETCHARGE,
--ASTARIFFBAND,
USUNMEASUREDTARIFFCODE,
USSPECIALAGREEMENTFLAG,
USSPECIALAGREEMENTFACTOR,
USSPECIALAGREEMENTREF,
USUNMEASUREDTYPEACOUNT,
USUNMEASUREDTYPEADESCRIPTION,
USUNMEASUREDTYPEBCOUNT,
USUNMEASUREDTYPEBDESCRIPTION,
USUNMEASUREDTYPECCOUNT,
USUNMEASUREDTYPECDESCRIPTION,
USUNMEASUREDTYPEDCOUNT,
USUNMEASUREDTYPEDDESCRIPTION,
USUNMEASUREDTYPEECOUNT,
USUNMEASUREDTYPEEDESCRIPTION,
USUNMEASUREDTYPEFCOUNT,
USUNMEASUREDTYPEFDESCRIPTION,
USUNMEASUREDTYPEGCOUNT,
USUNMEASUREDTYPEGDESCRIPTION,
USUNMEASUREDTYPEHCOUNT,
USUNMEASUREDTYPEHDESCRIPTION,
USPIPESIZE,
SADJCHARGEADJTARIFFCODE,
SRFCWATERTARRIFCODE,
SRFCWATERAREADRAINED,
SRFCWATERCOMMUNITYCONFLAG,
SWSPECIALAGREEMENTFLAG,
SWSPECIALAGREEMENTFACTOR,
SWSPECIALAGREEMENTREF,
HWAYDRAINAGETARIFFCODE,
HWAYSURFACEAREA,
HWAYCOMMUNITYCONFLAG,
HDSPECIALAGREEMENTFLAG,
HDSPECIALAGREEMENTFACTOR,
HDSPECIALAGREEMENTREF,
SAPFLOCNUMBER,
OWC                  CONSTANT 'WESSEX-W'
)
