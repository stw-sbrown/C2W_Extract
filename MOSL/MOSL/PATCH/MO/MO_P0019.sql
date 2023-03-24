------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	P0019.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	15/04/2016
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
-- V0.01		    14/04/2016		M.Marron        I-126 - Align with MOSL constraint for METER Readings
--                                                  Drop constraint PK_METERREADING_COMP
--													Drop Column METERREF 
--                                                  ADD Colum METERREF
--                                                  Add constraint PK_METERREADING_COMP
-- 
------------------------------------------------------------------------------------------------------------

--changes
--drop unique constraints on TABLES
---Drop redundant constraint
--
ALTER TABLE MO_METER_READING DROP CONSTRAINT PK_METERREADING_COMP;
ALTER TABLE MO_METER_READING DROP COLUMN METERREF;
--added ammended constraint and METERREF as nullable
ALTER TABLE MO_METER_READING ADD METERREF NUMBER(9);
ALTER TABLE MO_METER_READING ADD CONSTRAINT PK_METERREADING_COMP PRIMARY KEY (MANUFACTURER_PK,MANUFACTURERSERIALNUM_PK,METERREADDATE); 
COMMIT;
exit;