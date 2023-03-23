------------------------------------------------------------------------------
-- TASK					: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	LU_P00001.sql
--
-- CREATED        		: 	24/02/2016
--
--
-- Subversion $Revision: 4023 $	
--
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Patch to LU_SERVICE_CATEGORY:
--													Problem description: Added new column service_component_type to the lookup  table LU_SERVICE_CATEGORY
--													Set the values TARGET_SERV_PROV_CODE into service_component_type
--													Set the TARGET_SERV_PROV_CODE as per the target values.
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

alter table LU_SERVICE_CATEGORY add service_component_type varchar2(10);
update LU_SERVICE_CATEGORY set service_component_type=TARGET_SERV_PROV_CODE;
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE=null;
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='MSWD' where service_component_type='SWD';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='S' where service_component_type='MS';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='SU' where service_component_type='US';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='SU' where service_component_type='AS';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='TW' where service_component_type='TE';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='W' where service_component_type='MPW';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='UW' where service_component_type='AW';
update LU_SERVICE_CATEGORY set TARGET_SERV_PROV_CODE='UW' where service_component_type='UW';
commit;
exit
