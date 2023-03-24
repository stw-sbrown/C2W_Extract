------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00006.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	01/03/2016
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
-- V0.01       		01/03/2016      N.Henderson     Add missing sequence to TARIFF_VERSION_PK field in TARIFF_VERSION
-- 
--
--
-- 
------------------------------------------------------------------------------------------------------------



--MO_TARIFF_VERSION
CREATE SEQUENCE TARIFF_VERSION_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
commit;
exit;
