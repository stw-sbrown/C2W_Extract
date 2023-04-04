------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS DELETE of Supporting Lookup tables
--
-- AUTHOR         		: 	Michael Marron
--
-- FILENAME       		: 	03_DEL_MOSL_Lookup_TABLES_ALL.sql
--
-- CREATED        		: 	26/02/2016
--
-- Subversion $Revision: 6255 $
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
-- Version     Date          	Author      Description
-- ---------   ----------     -------     ------------------------------------------------
-- V0.01       26/02/2016    	M.Marron	  Initial version add DEL scripts for LU_SPID_RANGE, LU_CONSTUCTION_SITE and LU_PUBHEALTHRESITE
-- V0.02       14/03/2016     M.Marron    Added drop commands for LU_LANDLORD,LU_CROSSBORDER, LU_CLOCKOVER	
-- V0.03       16/03/2016     M.Marron    added drop for LU_DATALOGGERS
-- v0.04       18/03/2016     M.Marron	  Added LU_TARIFF,  LU_SERVICE_CATEGORY, LU_SERVICE_COMP_CHARGES, LU_TARIFF_SPECIAL_AGREEMENTS
-- V0.05			 29/04/2016		  NJH					Added LU_OUTREADER_PROTOCOLS
-- V0.06	     19/05/2016	    M.Marron	  corrected  LU_CONSTRUCTION_SITE,LU_TARIFF and add LU_TE_REFDESC 
-- v0.07       13/07/2016     S.Badhan    SAP CR-16. Added drop OF table LU_DISCHARGE_VOL_LIMITS.
-- V0.08       17/08/2016     S.Badhan    I-320. LU_TARIFF AND LU_SERVICE_COMP_CHARGES no longer exist. Commented out .
-- V0.08       29/09/2016     S.Badhan    Add drop for LU_OWC_RECON_MEASURES.
-- V0.09       26/10/2016     S.Badhan    Add drop for LU_SS_LANDLORD.
-- V0.10       03/11/2016     D.Cheung    Add drop for LU_OTHER_METER_DPID
-- V0.11       10/11/2016     L.Smith     Add drop for LU_TE_EXCLUSION
-- V0.12       16/11/2016     L.Smith     Add drop for LU_TE_METER_PAIRING
-- V0.13       16/11/2016     D.Cheung    Add drop for LU_TE_METER_DPID_EXCLUSIONS
------------------------------------------------------------------------------------------------------------
--
-- Drop LU_SPID_RANGE table
DROP TABLE LU_SPID_RANGE PURGE;
-- Drop LU_PUBHEALTHRESITE table
DROP TABLE LU_PUBHEALTHRESITE PURGE;
-- Drop LU_CONSTUCTION_SITE table
DROP TABLE LU_CONSTRUCTION_SITE PURGE;
--Drop LU_LANDLORD
DROP TABLE LU_LANDLORD PURGE;
-- Drop LU_CROSSBORDER
DROP TABLE LU_CROSSBORDER PURGE;
-- DROP LU_CLOCKOVER
--DROP TABLE LU_CLOCKOVER PURGE;
-- Drop LU_DATALOGGERS
DROP TABLE LU_DATALOGGERS PURGE;
-- Create LU_TARIFF 
--DROP TABLE LU_TARIFF PURGE;
-- DROP LU_SERVICE_CATEGORY
DROP TABLE LU_SERVICE_CATEGORY PURGE;
-- Drop LU_SERVICE_COMP_CHARGES
--DROP TABLE LU_SERVICE_COMP_CHARGES PURGE;
--  DROP LU_TARIFF_SPECIAL_AGREEMENTS
DROP TABLE LU_TARIFF_SPECIAL_AGREEMENTS PURGE;
--	DROP LU_OUTREADER_PROTOCOLS
DROP TABLE LU_OUTREADER_PROTOCOLS PURGE;
--	DROP LU_TE_REFDESC
DROP TABLE LU_TE_REFDESC PURGE;
--
DROP TABLE LU_SAP_EQUIPMENT PURGE;
--
DROP TABLE LU_SAP_FLOCA PURGE;
--
drop table LU_TE_BILLING_CYCLE;
--
DROP TABLE LU_SPID_OWC_RETAILER;

DROP TABLE LU_DISCHARGE_VOL_LIMITS PURGE;

DROP TABLE LU_OWC_RECON_MEASURES PURGE;

DROP TABLE LU_SS_LANDLORD PURGE;

DROP TABLE LU_OTHER_METER_DPID PURGE;

DROP TABLE LU_TE_EXCLUSION PURGE;

DROP TABLE LU_TE_METER_PAIRING PURGE;

DROP TABLE LU_TE_METER_DPID_EXCLUSION PURGE;

commit;
exit;


