------------------------------------------------------------------------------
-- TASK			: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	LU_P0007.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	19/04/2016
--
-- Subversion $Revision: 4023 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           	 Author         	Description
-- ---------      		----------     	 -------        	------------------------------------------------
-- V0.01		    	19/04/2016		N.Henderson		Drop all data from LU_SERVICE_CATEGORY as data loads are now handled by SQLLDR	 
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
exit;
