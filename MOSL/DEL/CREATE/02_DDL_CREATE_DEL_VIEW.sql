------------------------------------------------------------------------------
-- TASK				: 	MOSL DELIVERY CREATION 
--
-- AUTHOR         		: 	Kevin Burton
--
-- FILENAME       		: 	02_DDL_CREATE_DEL_VIEW.sql
--
-- CREATED        		: 	18/04/2016
--
-- Subversion $Revision: 4023 $
--	
-- DESCRIPTION 		   	: 	Creates all database views for initial MOSL upload
--
-- NOTES  			:	
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS 	 	:	01_DDL_CREATE_DEL.sql
--                                      03_DDL_CREATE_DEL_TRIGGERS.sql
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date        	 	Author         		Description
-- ---------      	---------------  	-------            	------------------------------------
-- V0.01	       	18/04/2016   	 	K.Burton	     	Initial version
-- V0.02		19/04/2016		K.Burton		Added join to DEL_SUPPLY_POINT to
--									Service Component views
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_MPW_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.METEREDPWMAXDAILYDEMAND,
         TT.MPWMAXIMUMDEMANDTARIFF,
         SC.DAILYRESERVEDCAPACITY,
         TT.MPWDAILYSTANDBYUSAGEVOLCHARGE
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       MOUTRAN.MO_TARIFF_TYPE_MPW TT,
       MOUTRAN.MO_TARIFF_VERSION TV,
       DEL_SUPPLY_POINT SP
  WHERE SC.SERVICECOMPONENTTYPE = 'MPW'
  AND SC.SPID_PK = SP.SPID_PK
  AND SC.TARIFFCODE_PK = TV.TARIFFCODE_PK(+)
  AND TV.TARIFF_VERSION_PK = TT.TARIFF_VERSION_PK(+)
  AND TV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) 
                          FROM MOUTRAN.MO_TARIFF_VERSION 
                          WHERE TARIFFCODE_PK = SC.TARIFFCODE_PK); 
  
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_MNPW_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.METEREDNPWMAXDAILYDEMAND,
         TT.MNPWMAXIMUMDEMANDTARIFF,
         SC.METEREDNPWDAILYRESVDCAPACITY,
         TT.MNPWDAILYSTANDBYUSAGEVOLCHARGE
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       MOUTRAN.MO_TARIFF_TYPE_MNPW TT,
       MOUTRAN.MO_TARIFF_VERSION TV,
       DEL_SUPPLY_POINT SP
  WHERE SC.SERVICECOMPONENTTYPE = 'MNPW'
  AND SC.SPID_PK = SP.SPID_PK
  AND SC.TARIFFCODE_PK = TV.TARIFFCODE_PK(+)
  AND TV.TARIFF_VERSION_PK = TT.TARIFF_VERSION_PK(+)
  AND TV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) 
                          FROM MOUTRAN.MO_TARIFF_VERSION 
                          WHERE TARIFFCODE_PK = SC.TARIFFCODE_PK);    
  
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_UW_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SPECIALAGREEMENTREF,
         SC.UNMEASUREDTYPEACOUNT,
         SC.UNMEASUREDTYPEADESCRIPTION,
         SC.UNMEASUREDTYPEBCOUNT,
         SC.UNMEASUREDTYPEBDESCRIPTION,
         SC.UNMEASUREDTYPECCOUNT,
         SC.UNMEASUREDTYPECDESCRIPTION,
         SC.UNMEASUREDTYPEDCOUNT,
         SC.UNMEASUREDTYPEDDESCRIPTION,
         SC.UNMEASUREDTYPEECOUNT,
         SC.UNMEASUREDTYPEEDESCRIPTION,
         SC.UNMEASUREDTYPEFCOUNT,
         SC.UNMEASUREDTYPEFDESCRIPTION,
         SC.UNMEASUREDTYPEGCOUNT,
         SC.UNMEASUREDTYPEGDESCRIPTION,
         SC.UNMEASUREDTYPEHCOUNT,
         SC.UNMEASUREDTYPEHDESCRIPTION,
         SC.PIPESIZE
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       DEL_SUPPLY_POINT SP  
  WHERE SERVICECOMPONENTTYPE = 'UW'
  AND SC.SPID_PK = SP.SPID_PK;

CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_WCA_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         WCA.ADJUSTMENTSCHARGEADJTARIFFCODE,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       MOUTRAN.MO_SERVICE_COMPONENT_VOL_ADJ WCA,
       DEL_SUPPLY_POINT SP      
  WHERE SC.SERVICECOMPONENTTYPE = 'WCA'
  AND SC.SERVICECOMPONENTREF_PK = WCA.SERVICECOMPONENTREF_PK;  
  
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_SCA_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SCA.ADJUSTMENTSCHARGEADJTARIFFCODE,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       MOUTRAN.MO_SERVICE_COMPONENT_VOL_ADJ SCA,
       DEL_SUPPLY_POINT SP
  WHERE SC.SERVICECOMPONENTTYPE = 'SCA'
  AND SC.SPID_PK = SP.SPID_PK
  AND SC.SERVICECOMPONENTREF_PK = SCA.SERVICECOMPONENTREF_PK;    

CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_MS_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       DEL_SUPPLY_POINT SP
  WHERE SERVICECOMPONENTTYPE = 'MS'
  AND SC.SPID_PK = SP.SPID_PK;
  
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_US_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.UNMEASUREDTYPEACOUNT,
         SC.UNMEASUREDTYPEADESCRIPTION,
         SC.UNMEASUREDTYPEBCOUNT,
         SC.UNMEASUREDTYPEBDESCRIPTION,
         SC.UNMEASUREDTYPECCOUNT,
         SC.UNMEASUREDTYPECDESCRIPTION,
         SC.UNMEASUREDTYPEDCOUNT,
         SC.UNMEASUREDTYPEDDESCRIPTION,
         SC.UNMEASUREDTYPEECOUNT,
         SC.UNMEASUREDTYPEEDESCRIPTION,
         SC.UNMEASUREDTYPEFCOUNT,
         SC.UNMEASUREDTYPEFDESCRIPTION,
         SC.UNMEASUREDTYPEGCOUNT,
         SC.UNMEASUREDTYPEGDESCRIPTION,
         SC.UNMEASUREDTYPEHCOUNT,
         SC.UNMEASUREDTYPEHDESCRIPTION,
         SC.PIPESIZE
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       DEL_SUPPLY_POINT SP
  WHERE SERVICECOMPONENTTYPE = 'US'
  AND SC.SPID_PK = SP.SPID_PK;

CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_SW_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.SRFCWATERAREADRAINED,
         SC.SRFCWATERCOMMUNITYCONFLAG
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       DEL_SUPPLY_POINT SP
  WHERE SERVICECOMPONENTTYPE = 'SW'
  AND SC.SPID_PK = SP.SPID_PK;
  
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_HD_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.HWAYSURFACEAREA,
         SC.HWAYCOMMUNITYCONFLAG
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       DEL_SUPPLY_POINT SP
  WHERE SERVICECOMPONENTTYPE = 'HD'
  AND SC.SPID_PK = SP.SPID_PK;

CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_AS_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.ASSESSEDCHARGEMETERSIZE,
         SC.ASSESSEDDVOLUMETRICRATE,
         SC.ASSESSEDTARIFBAND,
         TT.ASFIXEDCHARGE,
         TT.ASVOLMETCHARGE,
         (SELECT COUNT(*) FROM MOUTRAN.MO_AS_BAND_CHARGE WHERE TARIFF_TYPE_PK = TT.TARIFF_TYPE_PK) ASTARIFFBAND
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       MOUTRAN.MO_TARIFF_TYPE_AS TT,
       MOUTRAN.MO_TARIFF_VERSION TV,
       DEL_SUPPLY_POINT SP
  WHERE SC.SERVICECOMPONENTTYPE = 'AS'
  AND SC.SPID_PK = SP.SPID_PK
  AND SC.TARIFFCODE_PK = TV.TARIFFCODE_PK(+)
  AND TV.TARIFF_VERSION_PK = TT.TARIFF_VERSION_PK(+)
  AND TV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) 
                          FROM MOUTRAN.MO_TARIFF_VERSION 
                          WHERE TARIFFCODE_PK = SC.TARIFFCODE_PK); 
  
CREATE OR REPLACE VIEW DEL_SERVICE_COMPONENT_AW_V AS
  SELECT SC.SERVICECOMPONENTREF_PK,
         SC.TARIFFCODE_PK,
         SC.SPID_PK,
         SC.DPID_PK,
         SC.STWPROPERTYNUMBER_PK,
         SC.SPECIALAGREEMENTFACTOR,
         SC.SPECIALAGREEMENTFLAG,
         SC.SPECIALAGREEMENTREF,
         SC.ASSESSEDCHARGEMETERSIZE,
         SC.ASSESSEDDVOLUMETRICRATE,
         SC.ASSESSEDTARIFBAND,
         TT.AWFIXEDCHARGE,
         TT.AWVOLUMETRICCHARGE,
         (SELECT COUNT(*) FROM MOUTRAN.MO_AW_BAND_CHARGE WHERE TARIFF_TYPE_PK = TT.TARIFF_TYPE_PK) AWTARIFFBAND
  FROM MOUTRAN.MO_SERVICE_COMPONENT SC,
       MOUTRAN.MO_TARIFF_TYPE_AW TT,
       MOUTRAN.MO_TARIFF_VERSION TV,
       DEL_SUPPLY_POINT SP
  WHERE SC.SERVICECOMPONENTTYPE = 'AW'
  AND SC.SPID_PK = SP.SPID_PK
  AND SC.TARIFFCODE_PK = TV.TARIFFCODE_PK(+)
  AND TV.TARIFF_VERSION_PK = TT.TARIFF_VERSION_PK(+)
  AND TV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) 
                          FROM MOUTRAN.MO_TARIFF_VERSION 
                          WHERE TARIFFCODE_PK = SC.TARIFFCODE_PK);

CREATE OR REPLACE VIEW DEL_DISCHARGE_POINT_TARIFF_V AS
( 
  SELECT T.TARIFFCODE_PK,
         COUNT(T.TARIFFCODE_PK) VALIDTETARIFFCODE,
         COUNT(TB.BAND) TARIFFBANDCOUNT,
         COUNT(TE.TECHARGECOMPAO) AMMONIACALNITROGEN,
         COUNT(TE.TECHARGECOMPXO) XCOMP,
         COUNT(TE.TECHARGECOMPYO) YCOMP,
         COUNT(TE.TECHARGECOMPZO) ZCOMP
  FROM MOUTRAN.MO_TARIFF T,
       MOUTRAN.MO_TARIFF_VERSION TV,
       MOUTRAN.MO_TARIFF_TYPE_TE TE,
       MOUTRAN.MO_TE_BAND_CHARGE TB
  WHERE T.SERVICECOMPONENTTYPE = 'TE' 
  AND TV.TARIFFCODE_PK          = T.TARIFFCODE_PK
  AND TV.TARIFF_VERSION_PK      = TE.TARIFF_VERSION_PK
  AND TE.TARIFF_TYPE_PK         = TB.TARIFF_TYPE_PK(+)
  AND TV.TARIFFVERSION          = (SELECT MAX(TARIFFVERSION)
                                   FROM MOUTRAN.MO_TARIFF_VERSION
                                   WHERE TARIFFCODE_PK = T.TARIFFCODE_PK)
  GROUP BY T.TARIFFCODE_PK       
);

ALTER VIEW DEL_SERVICE_COMPONENT_MPW_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_MNPW_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_UW_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_WCA_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_SCA_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_MS_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_US_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_SW_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_HD_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_AS_V COMPILE;
ALTER VIEW DEL_SERVICE_COMPONENT_AW_V COMPILE;
ALTER VIEW DEL_DISCHARGE_POINT_TARIFF_V COMPILE;

commit;
exit;
