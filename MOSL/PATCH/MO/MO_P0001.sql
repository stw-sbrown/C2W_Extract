------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	P00022.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	21/04/2016
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
-- V0.01       		24/02/2016      M.Marron     Initial version 
--
--
-- 
------------------------------------------------------------------------------------------------------------

--DROP CONSTRAINT FK_SERVICECOMPONENTREF_PK01
ALTER TABLE MO_SERVICE_COMPONENT DROP CONSTRAINT FK_SERVICECOMPONENTREF_PK01;
COMMENT ON COLUMN MO_SERVICE_COMPONENT.METEREDFSMAXDAILYDEMAND IS 'Maximum Daily Demand~~~D2079 - Maximum daily demand in m3 for a Metered Service Component, for maximum demand tariffs';
commit;
exit;

