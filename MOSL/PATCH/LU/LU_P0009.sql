------------------------------------------------------------------------------
-- TASK			: 	LOOKUP TABLES RDBMS PATCHES 
--
-- AUTHOR         		: 	Dominic Cheung
--
-- FILENAME       		: 	LU_P0009.sql
--
-- CREATED        		: 	27/04/2016
--
-- Subversion $Revision: 4427 $
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
-- V0.02          16/06/2016    D.Cheung						Change PK Constraint on LU_SAP_FLOCA to SAPFLOCNUMBER							
--							                                    Change PK Constraint on LU_SAP_EQUIPMENT to SAPEQUIPMENT
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--Patch Script ------------


-- Create Constraint set SPID to primary key
ALTER TABLE LU_CROSSBORDER ADD CONSTRAINT PK_SPID05 PRIMARY KEY (SPID_PK);

-- Create Unique Constraints on SAP fields to SAP LUs
ALTER TABLE LU_SAP_EQUIPMENT ADD CONSTRAINT CH01_LU_SAPEQUIPMENT UNIQUE (SAPEQUIPMENT) ENABLE;
ALTER TABLE LU_SAP_FLOCA ADD CONSTRAINT CH01_LU_SAPFLOCNUMBER UNIQUE (SAPFLOCNUMBER) ENABLE;
--
-- add table and field comments
commit;
/
exit;
