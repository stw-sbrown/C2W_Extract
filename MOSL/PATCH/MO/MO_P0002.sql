------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00002.sql
--
--
-- Subversion $Revision: 5333 $	
--
-- CREATED        		: 	25/02/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL database
--
-- NOTES  			:	Place a summery at the top of the file with more detailed information 
--					where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	Date            Author         	Description
-- ---------    ----------      -------         ------------------------------------------------
-- V0.02		    31/08/2016			S.Badhan		    Remove CHK02_SERVICECOMPONENTTYPE. Set up in later patch correctly.
-- V0.01		    25/02/2016			N.Henderson		Field MO_TARIFF.TARIFFNAME not long enough
--                                                  Drop constraint CHK02_SERVICECOMPONENTTYPE
--													Alter table and drop column TARIFFNAME.
--                                                  Add column TARIFFNAME VARCHAR(255)
--                                                  Add constraint CHK02_SERVICECOMPONENTTYPE
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--V0.01
-- Drop the constraint]
--alter table MO_TARIFF DROP CONSTRAINT CHK02_SERVICECOMPONENTTYPE;
-- remove the field
alter table MO_TARIFF drop column TARIFFNAME;
-- add new field
alter table MO_TARIFF add TARIFFNAME VARCHAR(255);
-- add constraint
--ALTER TABLE MO_TARIFF ADD CONSTRAINT CHK02_SERVICECOMPONENTTYPE CHECK (SERVICECOMPONENTTYPE IN ('MPW','MNPW','AW','UW','MS','AS','US','SW','HD','TE','WCA','SCA'));
--commit;
exit;

