------------------------------------------------------------------------------
-- TASK			: 	MIG RDBMS DROP 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	01_DDL_MIG_DROP_ALL_TABLES.sql
--
-- CREATED        		: 	10/03/2016
--
-- Subversion $Revision: 4023 $
--	
-- DESCRIPTION 		: 	Drops all MIG tables
--
-- NOTES  			:	
--
-- ASSOCIATED SCRIPTS  	:	
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          Author         		Description
-- ---------      	---------------     -------            	 ------------------------------------------------
-- V0.01       	10/03/2016    	N.Henderson     	Initial version
-- 
-- 
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
DROP TABLE MIG_BATCHSTATUS;
DROP TABLE MIG_CPLOG;
DROP TABLE MIG_CPREF;
DROP TABLE MIG_ERRORLOG;
DROP TABLE MIG_ERRREF;
DROP TABLE MIG_JOBREF;
DROP TABLE MIG_JOBSTATUS;

--DROP VIEWS
drop view MIG_V_BATCHSTATUS;
drop view MIG_V_ERRLOG;

commit;
exit;

