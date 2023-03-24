------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	P00027.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	29/04/2016
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
-- V0.01       		28/04/2016      M.Marron    Changed name of attribute in MO_TARIFF of SERVICECOMPONENTREF_PK to SERVICECOMPONENTTYPE
 
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_TARIFF DROP column SERVICECOMPONENTREF_PK;
ALTER TABLE MO_TARIFF ADD SERVICECOMPONENTTYPE VARCHAR2(4);
commit;
exit;

