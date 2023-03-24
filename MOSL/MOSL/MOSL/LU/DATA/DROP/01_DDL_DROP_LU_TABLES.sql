------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS DELETE of Supporting Lookup tables
--
-- AUTHOR         		: 	Michael Marron
--
-- FILENAME       		: 	03_DEL_MOSL_Lookup_TABLES_ALL.sql
--
-- CREATED        		: 	26/02/2016
--	
-- DESCRIPTION 		   	: 	DELETES all supporting lookup tables required for MOSL database
--
-- NOTES  			:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order
-- 							as detailed below (.sql).
--
-- ASSOCIATED FILES		:	MOSL_PDM_CORE.xlsm
-- ASSOCIATED SCRIPTS  		:	01_DDL_MOSL_Lookups_ALL.sql
--					02_Trunc_MOSL_Lookups_ALL.sql
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          	Author         		Description
-- ---------      	----------     -------            	 ------------------------------------------------
-- V0.01       		26/02/2016    	M.Marron	     	Initial version add DEL scripts for LU_SPID_RANGE, LU_CONSTUCTION_SITE and LU_PUBHEALTHRESITE
-- V0.02            14/03/2016      M.Marron            Added drop commands for LU_LANDLORD,LU_CROSSBORDER, LU_CLOCKOVER	
-- V0.03            16/03/2016      M.Marron            added drop for LU_DATALOGGERS
------------------------------------------------------------------------------------------------------------
--
-- Drop LU_SPID_RANGE table
DROP TABLE LU_SPID_RANGE purge;
-- Drop LU_PUBHEALTHRESITE table
DROP TABLE LU_PUBHEALTHRESITE purge;
-- Drop LU_CONSTRUCTION_SITE table
DROP TABLE LU_CONSTRUCTION_SITE purge;
--Drop LU_LANDLORD
drop TABLE LU_LANDLORD purge;
-- Drop LU_CROSSBORDER
drop TABLE LU_CROSSBORDER purge;
-- DROP LU_CLOCKOVER
DROP TABLE LU_CLOCKOVER;
-- Drop LU_DATALOGGERS
drop TABLE LU_DATALOGGERS purge;
-- Drop LU_TARIFF
DROP TABLE LU_TARIFF PURGE;
--Drop LU_SERVICE_CATEGORY
DROP TABLE LU_SERVICE_CATEGORY;
--DROP LU_SERVICE_COMP_CHARGES
DROP TABLE LU_SERVICE_COMP_CHARGES;
-- DROP LU_TARIFF_SPECIAL_AGREEMENTS
DROP TABLE LU_TARIFF_SPECIAL_AGREEMENTS;

commit;
exit;

