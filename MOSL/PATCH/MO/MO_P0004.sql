------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00004.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	25/02/2016
--	
-- DESCRIPTION 		   	: 	Make field CUSTOMER_FK on MO_ELIGIBLE_PREMISE nullable 
--
-- NOTES  				:	
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Alter field CUSTOMER_FK so that it is nullable		
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--V0.01
alter table MO_ELIGIBLE_PREMISES	MODIFY (CUSTOMERID_PK NULL);
commit;
exit;











