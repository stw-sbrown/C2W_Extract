------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00009.sql
--
--
-- Subversion $Revision: 4023 $	
--
--Date 						11/03/2016
--Issue: 					Removal of table MO_RETAILER_REGISTRATION.  This table is extra to requirements.
--Changes: 					Remove foreign keys
--							Drop table MO_RETAILER_REGISTRATION
--							Add new field to MO_ORG table so that a meaningful description can be added
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
-- Changes applied
ALTER TABLE MO_SUPPLY_POINT DROP CONSTRAINT FK_RETAILERID_PK01;
ALTER TABLE MO_RETAILER_REGISTRATION DROP CONSTRAINT FK_RETAILERID_PK02; 
ALTER TABLE MO_RETAILER_REGISTRATION DROP CONSTRAINT FK_SPID_PK01;
DROP TABLE MO_RETAILER_REGISTRATION;
ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT FK_RETAILERID_PK02 FOREIGN KEY ("RETAILERID_PK") REFERENCES "MO_ORG"("ORGID_PK");
ALTER TABLE MO_ORG add ORGDESCRIPTION VARCHAR(255);
-- add constraint to new field
commit;
exit;

