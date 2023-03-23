------------------------------------------------------------------------------
-- TASK				: 	MIG RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	P00001.sql
--
--
-- Subversion $Revision: 4023 $	
--
--Date 					06/04/2016
--Issue: 				Apply change to alter MIG_CPLOG.RECON_MEASURE_TOTAL size from 6 to 10 to cater for millions of meter readingsengh of . 
--Changes: 				Alter column length for MIG_CPLOG.RECON_MEASURE_TOTAL
--	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- v0.01			06/04/2016		M.Marron		 
--
--
-- 
------------------------------------------------------------------------------------------------------------
--increase column lengths MIG_CPLOG.RECON_MEASURE_TOTAL
------------------------------------------------
ALTER TABLE MIG_CPLOG MODIFY RECON_MEASURE_TOTAL NUMBER(10,0);
COMMIT;
EXIT;
