------------------------------------------------------------------------------
-- TASK					: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	LU_P00006.sql
--
--
-- Subversion $Revision: 5194 $	
--
-- CREATED        		: 	07/04/2016
--
-- Subversion $Revision: 5194 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     Date         Author      Description
-- ---------   ----------   -------     --------------------------------------------------------------------
-- V0.01		   07/04/2016		M.Marron		Drop LU_CLOCKOVER
--													            Create BT_CLOCKOVER one off
--													            Amend BT_CLOCKOVER add Primary Key
-- V0.02       17/08/2016   S.Badhan    I-320. Move BT_CLOCKOVER comments to 03_DDL_BT_CLOCKOVER						
--
-- 
------------------------------------------------------------------------------------------------------------

DROP TABLE LU_CLOCKOVER PURGE;

commit;
exit;

