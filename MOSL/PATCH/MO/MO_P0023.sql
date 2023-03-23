------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Ola BADMUS
--
-- FILENAME       		: 	P00023.sql
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
-- V0.01       		21/04/2016      M.Marron     Initial version 
 
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_METER_DPIDXREF MODIFY PERCENTAGEDISCHARGE NUMBER NOT NULL;

commit;
exit;

