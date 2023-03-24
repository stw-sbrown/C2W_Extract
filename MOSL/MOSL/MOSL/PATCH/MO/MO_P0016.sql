------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	P00016.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	08/04/2016
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
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    08/04/2016		M.Marron        Add column MetrReadDateforRemoval
--                                                  
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
--CHANGES
--ADD COLUMN TO TABLE ON TABLES:
------------------------------------------------------
ALTER TABLE MO_METER_READING ADD METRREADDATEFORREMOVAL DATE;
commit;
exit;



