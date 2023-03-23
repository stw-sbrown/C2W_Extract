------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00008.sql
--
--
-- Subversion $Revision: 4023 $	
--
--Date 					08/03/2016
--Issue: 				Apply a number of alterations as requested by Ola B and Sreedhar P. 
--Changes: 				
--	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- 
--
--
-- 
------------------------------------------------------------------------------------------------------------
--Sreedhar changes
--drop unique constraints in MO ADDRESS CHILD TABLES:
------------------------------------------------------
ALTER TABLE MO_CUST_ADDRESS DROP UNIQUE (ADDRESS_PK);
ALTER TABLE MO_CUST_ADDRESS DROP UNIQUE (CUSTOMERNUMBER_PK);
ALTER TABLE MO_PROPERTY_ADDRESS DROP UNIQUE (ADDRESS_PK);
ALTER TABLE MO_PROPERTY_ADDRESS DROP UNIQUE (STWPROPERTYNUMBER_PK);
ALTER TABLE MO_METER_ADDRESS DROP UNIQUE (METERSERIALNUMBER_PK);
ALTER TABLE MO_METER_ADDRESS DROP UNIQUE (ADDRESS_PK);


--increase column lengths MO ADDRESS CHILD TABLES
------------------------------------------------
--ALTER TABLE MO_PROPERTY_ADDRESS DROP COLUMN ADDRESSUSAGEPROPERTY;
--ALTER TABLE MO_PROPERTY_ADDRESS ADD ADDRESSUSAGEPROPERTY VARCHAR2(10);
ALTER TABLE MO_PROPERTY_ADDRESS MODIFY ADDRESSUSAGEPROPERTY VARCHAR2(10);

--ALTER TABLE MO_CUST_ADDRESS DROP COLUMN ADDRESSUSAGEPROPERTY;
--ALTER TABLE MO_CUST_ADDRESS ADD ADDRESSUSAGEPROPERTY VARCHAR2(10);
ALTER TABLE MO_CUST_ADDRESS MODIFY ADDRESSUSAGEPROPERTY VARCHAR2(10);

--ALTER TABLE MO_METER_ADDRESS DROP COLUMN ADDRESSUSAGEPROPERTY;
--ALTER TABLE MO_METER_ADDRESS ADD ADDRESSUSAGEPROPERTY VARCHAR2(10);
ALTER TABLE MO_METER_ADDRESS MODIFY ADDRESSUSAGEPROPERTY VARCHAR2(10);



--Ola Changes
--ALTER TABLE MO_CUSTOMER ADD UNIQUE (STWPROPERTYNUMBER_PK); This patch has been applied at the database creation level in file "02_DDL_MOSL_PK_ALL.sql"
ALTER TABLE MO_CUSTOMER ADD SERVICECATEGORY VARCHAR(1) NULL;

commit;
exit;

