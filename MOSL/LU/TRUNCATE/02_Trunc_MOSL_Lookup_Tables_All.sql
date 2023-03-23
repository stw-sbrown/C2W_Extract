------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS Truncation of Supporting Lookup tables
--
-- AUTHOR         		: 	Michael Marron
--
-- FILENAME       		: 	02_Trunc_MOSL_Lookup_TABLES_ALL.sql
--
-- CREATED        		: 	26/02/2016
--	
-- DESCRIPTION 		   	: 	Truncate all supporting lookup tables required for MOSL database
--
-- NOTES  			:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order
-- 							as detailed below (.sql).
--
-- ASSOCIATED FILES		:	MOSL_PDM_CORE.xlsm
-- ASSOCIATED SCRIPTS  		:	01_DDL_MOSL_Lookups_ALL.sql
--					03_DEL_MOSL_Lookups_ALL.sql
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          	Author         		Description
-- ---------      	----------     -------            	 ------------------------------------------------
-- V0.01       		26/02/2016    	M.Marron	     	Initial version add trunc scripts for LU_SPID_RANGE, LU_CONSTUCTION_SITE and LU_PUBHEALTHRESITE
-- V0.02            16/03/2016      M.Marron            Add trunc scripts for LU_LANDLORD,LU_CROSSBORDER,LU_CLOCKOVER and LU_DATALOGGERS	
------------------------------------------------------------------------------------------------------------
--
-- Truncate LU_SPID_RANGE table
Truncate Table LU_SPID_RANGE;
-- Truncate LU_PUBHEALTHRESITE table
Truncate Table LU_PUBHEALTHRESITE;
-- Truncate LU_CONSTUCTION_SITE table
Truncate Table LU_CONSTUCTION_SITE;
--Truncate LU_LANDLORD
Truncate TABLE LU_LANDLORD;
-- Truncate LU_CROSSBORDER
Truncate TABLE LU_CROSSBORDER;
-- Truncate LU_CLOCKOVER
Truncate TABLE LU_CLOCKOVER;
-- Truncate LU_DATALOGGERS
Truncate TABLE LU_DATALOGGERS;
