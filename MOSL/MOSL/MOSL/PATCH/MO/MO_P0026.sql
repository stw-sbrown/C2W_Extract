------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Surinder Badhan
--
-- FILENAME       		: 	P00026.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	27/04/2016
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
-- Version     		Date            Author       Description
-- ---------      	----------      -------      ------------------------------------------------
-- V0.01       		27/04/2016      S.Badhan     Initial version 
 
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_SERVICE_COMPONENT ADD CONSTRAINT PK_TARIFFCODE_COMTYPE UNIQUE (TARIFFCODE_PK, SPID_PK, SERVICECOMPONENTTYPE);

commit;
exit;

