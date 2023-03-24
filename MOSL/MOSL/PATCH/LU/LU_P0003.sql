------------------------------------------------------------------------------
-- TASK					: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	LU_P00003.sql
--
--
-- Subversion $Revision: 4023 $	
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
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Patch to LU_SERVICE_CATEGORY, set service_component_type='SW' 
--													
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ==============
update LU_SERVICE_CATEGORY set service_component_type='SW' where service_component_type='SWD';
commit;
exit;

