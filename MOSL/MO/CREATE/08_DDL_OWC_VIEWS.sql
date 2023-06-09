--
-- Creates views for OWC Service Component Import
--
-- Subversion $Revision: 5883 $	
--
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      26/09/2016  K.Burton   Intial
-- V 0.02      06/10/2016  S.Badhan   Change to use standard MO Tables from OWC tables.
-- V 0.03      18/10/2016  K.Burton   Transfer from RECEPTION area to TRAN so can be used for SAP also
-- V 0.04      18/10/2016  K.Burton   Get effective from date from supply point
----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_MPW_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.METEREDPWTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'W' STWSERVICETYPE,
      'MPW' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.MPWSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.MPWSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.MPWSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      SC.METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      SC.DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.METEREDPWTARIFFCODE IS NOT NULL;

CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_AW_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.AWASSESSEDTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'UW' STWSERVICETYPE,
      'AW' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.AWSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.AWSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.AWSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      SC.AWASSESSEDDVOLUMETRICRATE ASSESSEDDVOLUMETRICRATE,
      SC.AWASSESSEDCHARGEMETERSIZE ASSESSEDCHARGEMETERSIZE,
      SC.AWASSESSEDTARIFBAND ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.AWASSESSEDTARIFFCODE IS NOT NULL;  
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_MNPW_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.METEREDNPWTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'W' STWSERVICETYPE,
      'MNPW' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.MNPWSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.MNPWSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.MNPWSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      SC.METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      SC.METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.METEREDNPWTARIFFCODE IS NOT NULL; 
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_UW_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.UWUNMEASUREDTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'UW' STWSERVICETYPE,
      'UW' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.UWSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.UWSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.UWSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      SC.UWUNMEASUREDTYPEACOUNT UNMEASUREDTYPEACOUNT,
      SC.UWUNMEASUREDTYPEBCOUNT UNMEASUREDTYPEBCOUNT,
      SC.UWUNMEASUREDTYPECCOUNT UNMEASUREDTYPECCOUNT,
      SC.UWUNMEASUREDTYPEDCOUNT UNMEASUREDTYPEDCOUNT,
      SC.UWUNMEASUREDTYPEECOUNT UNMEASUREDTYPEECOUNT,
      SC.UWUNMEASUREDTYPEFCOUNT UNMEASUREDTYPEFCOUNT,
      SC.UWUNMEASUREDTYPEGCOUNT UNMEASUREDTYPEGCOUNT,
      SC.UWUNMEASUREDTYPEHCOUNT UNMEASUREDTYPEHCOUNT,
      SC.UWUNMEASUREDTYPEADESCRIPTION UNMEASUREDTYPEADESCRIPTION,
      SC.UWUNMEASUREDTYPEBDESCRIPTION UNMEASUREDTYPEBDESCRIPTION,
      SC.UWUNMEASUREDTYPECDESCRIPTION UNMEASUREDTYPECDESCRIPTION,
      SC.UWUNMEASUREDTYPEDDESCRIPTION UNMEASUREDTYPEDDESCRIPTION,
      SC.UWUNMEASUREDTYPEEDESCRIPTION UNMEASUREDTYPEEDESCRIPTION,
      SC.UWUNMEASUREDTYPEFDESCRIPTION UNMEASUREDTYPEFDESCRIPTION,
      SC.UWUNMEASUREDTYPEGDESCRIPTION UNMEASUREDTYPEGDESCRIPTION,
      SC.UWUNMEASUREDTYPEHDESCRIPTION UNMEASUREDTYPEHDESCRIPTION,
      SC.UWPIPESIZE PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.UWUNMEASUREDTARIFFCODE IS NOT NULL;    
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_MS_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.METEREDFSTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'W' STWSERVICETYPE,
      'MS' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.MFSSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.MFSSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.MFSSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.METEREDFSTARIFFCODE IS NOT NULL;  
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_US_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.USUNMEASUREDTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'US' STWSERVICETYPE,
      'US' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.USSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.USSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.USSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      SC.USUNMEASUREDTYPEACOUNT UNMEASUREDTYPEACOUNT,
      SC.USUNMEASUREDTYPEBCOUNT UNMEASUREDTYPEBCOUNT,
      SC.USUNMEASUREDTYPECCOUNT UNMEASUREDTYPECCOUNT,
      SC.USUNMEASUREDTYPEDCOUNT UNMEASUREDTYPEDCOUNT,
      SC.USUNMEASUREDTYPEECOUNT UNMEASUREDTYPEECOUNT,
      SC.USUNMEASUREDTYPEFCOUNT UNMEASUREDTYPEFCOUNT,
      SC.USUNMEASUREDTYPEGCOUNT UNMEASUREDTYPEGCOUNT,
      SC.USUNMEASUREDTYPEHCOUNT UNMEASUREDTYPEHCOUNT,
      SC.USUNMEASUREDTYPEADESCRIPTION UNMEASUREDTYPEADESCRIPTION,
      SC.USUNMEASUREDTYPEBDESCRIPTION UNMEASUREDTYPEBDESCRIPTION,
      SC.USUNMEASUREDTYPECDESCRIPTION UNMEASUREDTYPECDESCRIPTION,
      SC.USUNMEASUREDTYPEDDESCRIPTION UNMEASUREDTYPEDDESCRIPTION,
      SC.USUNMEASUREDTYPEEDESCRIPTION UNMEASUREDTYPEEDESCRIPTION,
      SC.USUNMEASUREDTYPEFDESCRIPTION UNMEASUREDTYPEFDESCRIPTION,
      SC.USUNMEASUREDTYPEGDESCRIPTION UNMEASUREDTYPEGDESCRIPTION,
      SC.USUNMEASUREDTYPEHDESCRIPTION UNMEASUREDTYPEHDESCRIPTION,
      SC.USPIPESIZE PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.USUNMEASUREDTARIFFCODE IS NOT NULL;   
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_SW_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.SRFCWATERTARRIFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'W' STWSERVICETYPE,
      'SW' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.SWSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.SWSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.SWSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      SC.SRFCWATERAREADRAINED,
      SC.SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.SRFCWATERTARRIFCODE IS NOT NULL;   
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_HD_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.HWAYDRAINAGETARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'W' STWSERVICETYPE,
      'HD' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.HDSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.HDSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.HDSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      SC.HWAYSURFACEAREA,
      SC.HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.HWAYDRAINAGETARIFFCODE IS NOT NULL;  
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_AS_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.ASASSESSEDTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'SU' STWSERVICETYPE,
      'AS' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      SC.ASSPECIALAGREEMENTFACTOR SPECIALAGREEMENTFACTOR,
      SC.ASSPECIALAGREEMENTFLAG SPECIALAGREEMENTFLAG,
      SC.ASSPECIALAGREEMENTREF SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      SC.ASASSESSEDDVOLUMETRICRATE ASSESSEDDVOLUMETRICRATE,
      SC.ASASSESSEDCHARGEMETERSIZE ASSESSEDCHARGEMETERSIZE,
      SC.ASASSESSEDTARIFBAND ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.ASASSESSEDTARIFFCODE IS NOT NULL;  
  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_WCA_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.WADJCHARGEADJTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'W' STWSERVICETYPE,
      'WCA' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      NULL SPECIALAGREEMENTFACTOR,
      NULL SPECIALAGREEMENTFLAG,
      NULL SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.WADJCHARGEADJTARIFFCODE IS NOT NULL;  

  
CREATE OR REPLACE VIEW OWC_SERVICE_COMPONENT_SCA_V AS
  SELECT NULL SERVICECOMPONENTREF_PK,
      SC.SADJCHARGEADJTARIFFCODE TARIFFCODE_PK,
      SC.SPID_PK,
      NULL DPID_PK,
      SP.STWPROPERTYNUMBER_PK,
      'S' STWSERVICETYPE,
      'SCA' SERVICECOMPONENTTYPE,
      1 SERVICECOMPONENTENABLED,
      SP.SUPPLYPOINTEFFECTIVEFROMDATE EFFECTIVEFROMDATE,
      NULL SPECIALAGREEMENTFACTOR,
      NULL SPECIALAGREEMENTFLAG,
      NULL SPECIALAGREEMENTREF,
      NULL METEREDFSMAXDAILYDEMAND,
      NULL METEREDPWMAXDAILYDEMAND,
      NULL METEREDNPWMAXDAILYDEMAND,
      NULL METEREDFSDAILYRESVDCAPACITY,
      NULL METEREDNPWDAILYRESVDCAPACITY,
      NULL DAILYRESERVEDCAPACITY,
      NULL HWAYSURFACEAREA,
      NULL HWAYCOMMUNITYCONFLAG,
      NULL ASSESSEDDVOLUMETRICRATE,
      NULL ASSESSEDCHARGEMETERSIZE,
      NULL ASSESSEDTARIFBAND,
      NULL SRFCWATERAREADRAINED,
      NULL SRFCWATERCOMMUNITYCONFLAG,
      NULL UNMEASUREDTYPEACOUNT,
      NULL UNMEASUREDTYPEBCOUNT,
      NULL UNMEASUREDTYPECCOUNT,
      NULL UNMEASUREDTYPEDCOUNT,
      NULL UNMEASUREDTYPEECOUNT,
      NULL UNMEASUREDTYPEFCOUNT,
      NULL UNMEASUREDTYPEGCOUNT,
      NULL UNMEASUREDTYPEHCOUNT,
      NULL UNMEASUREDTYPEADESCRIPTION,
      NULL UNMEASUREDTYPEBDESCRIPTION,
      NULL UNMEASUREDTYPECDESCRIPTION,
      NULL UNMEASUREDTYPEDDESCRIPTION,
      NULL UNMEASUREDTYPEEDESCRIPTION,
      NULL UNMEASUREDTYPEFDESCRIPTION,
      NULL UNMEASUREDTYPEGDESCRIPTION,
      NULL UNMEASUREDTYPEHDESCRIPTION,
      NULL PIPESIZE,
      SC.OWC
  FROM RECEPTION.OWC_SERVICE_COMPONENT SC,
       MO_SUPPLY_POINT SP
  WHERE SP.SPID_PK = SC.SPID_PK
  AND SC.SADJCHARGEADJTARIFFCODE IS NOT NULL;     

ALTER VIEW OWC_SERVICE_COMPONENT_MPW_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_MNPW_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_UW_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_WCA_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_SCA_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_MS_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_US_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_SW_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_HD_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_AS_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_AW_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_WCA_V COMPILE;
ALTER VIEW OWC_SERVICE_COMPONENT_SCA_V COMPILE;

commit;
/
exit;
