------------------------------------------------------------------------------
-- TASK			: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Dominic Cheung
--
-- FILENAME       		: 	LU_P0009.sql
--
-- CREATED        		: 	27/04/2016
--
-- Subversion $Revision: 4023 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           	 Author         	Description
-- ---------      ----------     	 -------        	------------------------------------------------
-- V0.01		    	19/05/2016		M.Marron		        Create Constraint on LU_CROSSBORDER to enforce unique SPID	 
--													
--							
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ------------


-- Create Constraint set SPID to primary key
ALTER TABLE LU_CROSSBORDER ADD CONSTRAINT PK_SPID05 PRIMARY KEY (SPID_PK);
--
-- add table and field comments

/
commit;
exit;
