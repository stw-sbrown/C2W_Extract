--
-- CREATED        		: 	19/05/2016
--	
-- DESCRIPTION 		   	: 	Add Customer Constraints
--
--
-- Subversion $Revision: 4023 $	
--							
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		19/05/201       M.Marron	Drop FK_CUST_ADDR constraints to MOUTRAN tariff tables
------------------------------------------------------------------------------------------------------------
-- CHANGES
------------------------------------------------------------------------------------------------------------
ALTER TABLE MO_CUSTOMER DROP CONSTRAINT FK_CUST_ADDR;
commit;
exit;