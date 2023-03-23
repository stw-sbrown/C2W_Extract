------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.CHEUNG
--
-- FILENAME       		: 	MO_P00017.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	13/04/2016
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
-- V0.01		13/04/2016	D.CHEUNG        Drop Constraint SYS_C0041412 on MO_METER_READING
--							ADD COMPOSITE PRIMARY KEY ON MO_METER_READING
--                                                  
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
--CHANGES
------------------------------------------------------
--DROP CONSTRAINT SYS_C0041412
--ALTER TABLE MO_METER_READING DROP CONSTRAINT SYS_C0041412;
--ADD COMPOSITE PRIMARY KEY ON MO_METER_READING
ALTER TABLE MO_METER_READING ADD CONSTRAINT PK_METERREADING_COMP PRIMARY KEY (METERREF,METERREADDATE);
commit;
exit;




