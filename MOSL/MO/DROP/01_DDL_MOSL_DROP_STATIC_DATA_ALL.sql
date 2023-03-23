------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS DROP 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	01_DDL_DROPSTAIC_DATA_ALL.sql
--
-- CREATED        		: 	22/02/2016
--	
-- Subversion $Revision: 4023 $
--
-- DESCRIPTION 		   	: 	Drops all static data from the database
--
-- NOTES  				:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order
-- 							as detailed below (.sql).
--
-- ASSOCIATED SCRIPTS  	:	02_DDL_MOSL_FK_DROP_ALL.sql
--					03_DDL_MOSL_TABLES_DROP_ALL.sql
--					04_DDL_MOSL_DROP_SEQUENCES_ALL.sql
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          Author         		Description
-- ---------      	---------------     -------            	 ------------------------------------------------
-- V0.01       	15/02/2016    	N.Henderson     	Initial version after generation from Excel
-- 
-- 
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

alter table	MO_ELIGIBLE_PREMISES	drop constraint	CHK02_UPRNREASONCODE;
alter table	MO_SERVICE_COMPONENT	drop constraint	CHK02_SPECIALAGREEMENTFLAG;
alter table	MO_SERVICE_COMPONENT	drop constraint	CHK02_HWAYCOMMUNITYCONFLAG;
alter table	MO_SERVICE_COMPONENT_VOL_ADJ	drop constraint	CHK02_ADJUSTMENTSVOLADJTYPE;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_VOLTRANSFERFLAG;
alter table	MO_ELIGIBLE_PREMISES	drop constraint	CHK01_VOABAREFRSNCODE;
alter table	MO_ORG	drop constraint	CHK01_VACANCYCHARGINGMETWATER;
alter table	MO_ORG	drop constraint	CHK01_VACANCYCHARGINGMETHSEW;
alter table	MO_ADDRESS	drop constraint	CHK01_UPRNREASONCODE;
alter table	MO_RETAILER_REGISTRATION	drop constraint	CHK01_TRANSFERVALIDFLAG;
alter table	MO_ORG	drop constraint	CHK01_TRADINGPARTYSERVICECAT;
alter table	MO_ORG	drop constraint	CHK01_TMPDISCONCHRGMETHSEW;
alter table	MO_ORG	drop constraint	CHK01_TMPDISCONCHRGMETHODWATER;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_TEFZTREATMENTINDICATOR;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_TEFYTREATMENTINDICATOR;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_TEFXTREATMENTINDICATOR;
alter table	MO_TARIFF	drop constraint	CHK01_TARIFFSTATUS;
alter table	MO_CALCULATED_DISCHARGE	drop constraint	CHK01_SUBMISSIONFREQ;
alter table	MO_SERVICE_COMPONENT	drop constraint	CHK01_STWSERVICETYPE;
alter table	MO_CUSTOMER	drop constraint	CHK01_STDINDUSTRYCLASSCODETYPE;
alter table	MO_SERVICE_COMPONENT	drop constraint	CHK01_SRFCWATERCOMMCONFLAG;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_SPIDSTATUS;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_SLUDGETREATMENTINDICATOR;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_SEWERAGEVOLUMEADJMENTHOD;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_SERVICECOMPTYPE;
alter table	MO_SERVICE_COMPONENT	drop constraint	CHK01_SERVICECOMPONENTTYPE;
alter table	MO_SERVICE_COMPONENT	drop constraint	CHK01_SERVICECOMPONENTENABLED;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_SERVICECATEGORY;
alter table	MO_METER_READING	drop constraint	CHK01_ROLLOVERINDICATOR;
alter table	MO_METER_READING	drop constraint	CHK01_ROLLOVERFLAG;
alter table	MO_ORG	drop constraint	CHK01_RETAILERTYPE;
alter table	MO_METER_READING	drop constraint	CHK01_REREADFLAG;
alter table	MO_METER	drop constraint	CHK01_REMOTEREADTYPE;
alter table	MO_METER	drop constraint	CHK01_REMOTEREADFLAG;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_RECEPTIONTREATMENTIND;
alter table	MO_METER_READING	drop constraint	CHK01_RDAOUTCOME;
alter table	MO_ELIGIBLE_PREMISES	drop constraint	CHK01_PUBHEALTHRELSITEARR;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_PRIMARYTREATMENTIND;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_PAIRINGREFREASONCODE;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_OTHERSRVREASON;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_OTHERSERVICECATPROVIDED;
alter table	MO_ELIGIBLE_PREMISES	drop constraint	CHK01_OCCUPENCYSTATUS;
alter table	MO_ELIGIBLE_PREMISES	drop constraint	CHK01_NONPUBHEALTHRELSITE;
alter table	MO_METER	drop constraint	CHK01_NONMARKETMETERFLAG;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_NEWCONNECTIONTYPE;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_MULTIPLEWHOLESALERFLAG;
alter table	MO_METER	drop constraint	CHK01_METERTREATMENT;
alter table	MO_METER	drop constraint	CHK01_METERREMOVALREASON;
alter table	MO_METER_READING	drop constraint	CHK01_METERREADTYPE;
alter table	MO_METER_READING	drop constraint	CHK01_METERREADSETTLEMENTFLAG;
alter table	MO_METER	drop constraint	CHK01_METERREADMINFREQUENCY;
alter table	MO_METER_READING	drop constraint	CHK01_METERREADMETHOD;
alter table	MO_METER_READING	drop constraint	CHK01_METERREADERASEDFLAG;
alter table	MO_METER	drop constraint	CHK01_METEROUTREADERLOCCODE;
alter table	MO_METER	drop constraint	CHK01_METERLOCATIONCODE;
alter table	MO_METER	drop constraint	CHK01_METERERASEDFLAG;
alter table	MO_METER	drop constraint	CHK01_METERADDITIONREASON;
alter table	MO_METER	drop constraint	CHK01_MEASUREUNITFREEATMETER;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_MARINETREATMENTINDICATOR;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_LATEREGAPPLICATION;
alter table	MO_ORG	drop constraint	CHK01_INTERIMSUPPALLOCSTAT;
alter table	MO_METER	drop constraint	CHK01_INSTALLEDBYACCREDITED;
alter table	MO_ORG	drop constraint	CHK01_GAPSITEALLOCSTATUS;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_GAPSITEALLOCATIONMETHOD;
alter table	MO_METER_READING	drop constraint	CHK01_ESTIMATEDREADREMEDIAL;
alter table	MO_METER_READING	drop constraint	CHK01_ESTIMATEDREADREASONCODE;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_DPIDSPECIALAGREEMENT;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_DISCONRECONDEREGSTATUS;
alter table	MO_CALCULATED_DISCHARGE	drop constraint	CHK01_DISCHARGETYPE;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_DISCHARGEPOINTERASEFLAG;
alter table	MO_METER	drop constraint	CHK01_DATALOGGERWHOLESALER;
alter table	MO_CUSTOMER	drop constraint	CHK01_CUSTOMERCLASSIFICATION;
alter table	MO_METER	drop constraint	CHK01_COMBIMETERFLAG;
alter table	MO_RETAILER_REGISTRATION	drop constraint	CHK01_CANCELLATIONCODE;
alter table	MO_ELIGIBLE_PREMISES	drop constraint	CHK01_BUILDINGWATERSTATUS;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_BIOLOGICALTREATMENTIND;
alter table	MO_DISCHARGE_POINT	drop constraint	CHK01_AMMONIATREATMENTIND;
alter table	MO_DISCHARGE_POINT_VOLMET_ADJ	drop constraint	CHK01_ADJUSTMENTSVOLADJTYPE;
alter table	MO_SUPPLY_POINT	drop constraint	CHK01_ACCREDITEDENTITYFLAG;

--DROP VIEWS.


--DROP PROCEDURES
DROP PROCEDURE P_MOU_TRAN_KEY_GEN
DROP PROCEDURE P_MIG_RECON
DROP PROCEDURE P_MOU_TRAN_ROLLOVER
DROP PROCEDURE P_MOU_TRAN_SC_PRE
DROP PROCEDURE P_MOU_TRAN_SC_MPW
DROP PROCEDURE P_MOU_TRAN_SC_UW
DROP PROCEDURE P_MOU_TRAN_PROPERTY
DROP PROCEDURE P_MOU_TRAN_SUPPLY_POINT
DROP PROCEDURE P_MOU_TRAN_SERVICE_COMPONENT
DROP PROCEDURE P_MOU_TRAN_DISCHARGE_POINT
DROP PROCEDURE P_MOU_TRAN_METER_TARGET
DROP PROCEDURE P_MOU_TRAN_METER_SPID_ASSOC
DROP PROCEDURE P_MOU_TRAN_METER_NETWORK
DROP PROCEDURE P_MOU_TRAN_CUSTOMER
DROP PROCEDURE P_MOU_TRAN_METER_READING
DROP PROCEDURE P_MOU_TRAN_ADDRESS
commit;

--DROP FUNCTIONS
DROP FUNCTION FN_VALIDATE_POSTCODE;

--DROP PACKAGES
DROP PACKAGE P_MIG_BATCH;
DROP PACKAGE P_MIG_TARIFF;
commit;
exit;

