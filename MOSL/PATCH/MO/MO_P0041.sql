------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	P00041.sql
--
-- Subversion $Revision: 4581 $	
--
-- CREATED        		: 	24/06/2016
--	
-- DESCRIPTION 		   	: 	Renames the APPLICABLESERVICECOMP column in MO_TARIFF to SEASONALFLAG
--                        also alters column size. Used to mark tariffs as seasonal (ie multi-version)
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author   Description
-- ---------      ----------     -------   ----------------------------------------------------------------
-- V0.01       		24/06/2016     K.Burton  Renames the APPLICABLESERVICECOMP column in MO_TARIFF to SEASONALFLAG
------------------------------------------------------------------------------------------------------------
ALTER TABLE MO_TARIFF DROP CONSTRAINT CH01_APPLICABLESERVICECOMP;
ALTER TABLE MO_TARIFF MODIFY APPLICABLESERVICECOMPONENT VARCHAR2(1);
ALTER TABLE MO_TARIFF RENAME COLUMN APPLICABLESERVICECOMPONENT TO SEASONALFLAG;

commit;
/
show errors;
exit;


