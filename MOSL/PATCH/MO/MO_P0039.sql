------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.badhan
--
-- FILENAME       		: 	P00039.sql
--
-- CREATED        		: 	16/06/2016
--	
-- DESCRIPTION 		   	: 	This file recreates index and constraint SYS_C00106150 as composite on CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author   Description
-- ---------      ----------     -------   ----------------------------------------------------------------
-- V0.01       		16/06/2016     S.Badhan  Amend constraint to allow 'TRADABLE' instead of 'TRADEABLE'
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_SUPPLY_POINT DROP CONSTRAINT RF01_SPIDSTATUS;

ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT RF01_SPIDSTATUS	CHECK	(SPIDSTATUS IN ('NEW', 'PARTIAL', 'TRADABLE', 'REJECTED', 'DEREG'));

commit;
exit;
