------------------------------------------------------------------------------
-- TASK			: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	LU_P00004.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	03/03/2016
--
-- Subversion $Revision: 4023 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Patch to LU_SERVICE_CATEGORY, drop all values and re-insert new ones 
--													
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ------------


truncate table  lu_service_category;
commit;
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Metered Potable Water','W','MPW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Metered Non-Potable Water',null,'MNPW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Assessed Water','UW','AW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Unmeasured Water','UW','UW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Water Charge Adjustment',null,'WCA');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','S','MS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Assessed Sewerage','SU','AS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','SU','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','MSWD','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Highway Drainage Services',null,'HD');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Trade Effluent Services','TW','TE');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Sewerage Charge Adjustment',null,'SCA');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSYOR','MS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNTHA','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','UNSS','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','XDNW','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','XZNWE','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','XZWEL','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','XDYOR','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSANG','MS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUYOR','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','SW','MS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFYOR','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNYOR','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNNWE','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','UD','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUANG','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFWEL','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFNWE','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNWEL','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUTHA','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage
Services','XZYOR','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUWEL','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSNW','MS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','US','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFTHA','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Metered Sewerage','XSTHA','MS');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Metered Potable Water','XSWEL','MPW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('W','Water Service','Metered Potable Water','XSWES','MPW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage Services','XDANG','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Not sure may be Used water','PP01','UW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Surface Water Drainage','XZTHA','SW');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUWES','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XFWES','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XUNWE','US');
Insert into lu_service_category (SUPPLY_POINT_CODE,SUPPLY_POINT_DESC,SERVICECOMPONENTDESC,TARGET_SERV_PROV_CODE,SERVICECOMPONENTTYPE) values ('S','Sewerage Service','Unmeasured Sewerage','XNWES','US');
commit;
exit;


