------------------------------------------------------------------------------
-- TASK					: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	LU_P00004.sql
--
--
-- Subversion $Revision: 5194 $	
--
-- CREATED        		: 	03/03/2016
--
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     Date            Author       Description
-- ---------   ----------      -------      ------------------------------------------------
-- V0.01		   25/02/2016		   N.Henderson	LU_Tariff columns renamed to MOSL column names
--													                LU_TARIFF_SPECIAL_AGREEMENTS columns renamed
--													                to MOSL column names
--													                LU_SERVICE_CATEGORY columns renamed to MOSL 
--													                column names and data has been added as required.
-- V0.02       17/08/2016      S.Badhan     I-320. LU_TARIFF AND LU_SERVICE_COMP_CHARGES no longer exist. Commented out.
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ==============
--alter table lu_tariff rename column TARIFF_CODE to TARIFFCODE_PK;
--alter table lu_tariff rename column SERVICE_COMPONENT_CODE to SERVICECOMPONENTTYPE;
--alter table lu_tariff rename column TARIFF_EFFECTIVE_FROM_DATE to TARIFFEFFECTIVEFROMDATE;
--alter table lu_tariff rename column TARIFF_NAME to TARIFFNAME;
--alter table lu_tariff rename column TARIFF_STATUS to TARIFFSTATUS;
--alter table lu_tariff rename column TARIFF_AUTHORISATION_CODE to TARIFFAUTHCODE;
--alter table lu_tariff rename column LEGACY_TARIFF_EFFECT_FROM_DATE to TARIFFLEGACYEFFECTIVEFROMDATE;

--alter table LU_TARIFF_SPECIAL_AGREEMENTS rename column CD_TARIFF to TARIFFCODE;
alter table LU_TARIFF_SPECIAL_AGREEMENTS rename column SERVICE_PROVISION to SERVICECOMPONENTTYPE;

--alter table LU_SERVICE_CATEGORY rename column service_component_type to SERVICECOMPONENTTYPE;
--alter table LU_SERVICE_CATEGORY rename column SERVICE_PROVISION_DESC to SERVICECOMPONENTDESC;
update LU_SERVICE_CATEGORY set SERVICECOMPONENTDESC='Water Charge Adjustment' where servicecomponenttype='WCA';
UPDATE LU_SERVICE_CATEGORY SET SERVICECOMPONENTDESC='Sewerage Charge Adjustment' WHERE servicecomponenttype='SCA';
--alter table LU_SERVICE_CATEGORY drop column TARGET_SERV_PROV_DESC;
truncate table LU_SERVICE_CATEGORY;
--REM INSERTING into LU_SERVICE_CATEGORY
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Metered Potable Water','W','MPW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Metered Non-Potable Water',null,'MNPW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Assessed Water','UW','AW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Unmeasured Water','UW','UW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Water Charge Adjustment',null,'WCA');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','S','MS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Assessed Sewerage','SU','AS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','SU','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','MSWD','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Highway Drainage Services',null,'HD');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Trade Effluent Services','TW','TE');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Sewerage Charge Adjustment',null,'SCA');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSYOR','MS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNTHA','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','UNSS ','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','XDNW ','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','XZNWE','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','XZWEL','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','XDYOR','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSANG','MS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUYOR','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','SW   ','MS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFYOR','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNYOR','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNNWE','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','UD   ','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSNW ','MS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUANG','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFWEL','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFNWE','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNWEL','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUTHA','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage 
Services','XZYOR','SW');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUWEL','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSNW ','MS');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','US   ','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFTHA','US');
Insert into LU_SERVICE_CATEGORY (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSTHA','MS');

commit;
exit;

