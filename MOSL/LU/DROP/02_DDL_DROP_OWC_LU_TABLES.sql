------------------------------------------------------------------------------
-- TASK				: 	DROP OWC RECEPTION LOOKUP TABLES  
--
-- AUTHOR         		: 	Dominic Cheung
--
-- FILENAME       		: 	02_DDL_DROP_OWC_LU_TABLES.sql
--
-- CREATED        		: 	09/09/2016
--	
-- Subversion $Revision: 6342 $
--
-- DESCRIPTION 		   	: 	Drop all database lookup tables for OWC file reception area
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date        	 	Author         	Description
-- --------------------------------------------------------------------------------------------------------
-- V0.01	       	09/09/2016   	 	D.Cheung      	Initial version
-- V0.02          20/09/2016      D.Cheung        Move to TRAN area
-- V0.03          04/10/2016      K.Burton        Added LU_OWC_TE_METERS and BT_OWC_TE_DPID_REF
-- V0.04          12/10/2016      K.Burton        Added LU_SPID_RANGE_DWRCYMRU
-- V0.05          25/10/2016      K.Burton        Added LU_SPID_RANGE_NOSPID
-- V0.06          09/11/2016      K.Burton        Added LU_OWC_NOT_SENSITIVE
-- V0.07          10/11/2016      K.Burton        Added LU_OWC_SSW_SPIDS
-- V0.08          21/11/2016      K.Burton        Added LU_OWC_SAP_FLOCA
-- V0.09          22/11/2016      K.Burton        Added LU_NOSPID_EXCEPTIONS
--------------------------------------------------------------------------------------------------------
DROP TABLE LU_OWC_TARIFF PURGE;
DROP TABLE LU_OWC_TE_METERS PURGE;
DROP TABLE BT_OWC_TE_DPID_REF PURGE;
DROP TABLE LU_SPID_RANGE_DWRCYMRU PURGE;
DROP TABLE LU_SPID_RANGE_NOSPID PURGE;
DROP TABLE LU_OWC_NOT_SENSITIVE PURGE;
DROP TABLE LU_OWC_SSW_SPIDS PURGE;
DROP TABLE LU_OWC_SAP_FLOCA PURGE;
DROP TABLE LU_NOSPID_EXCEPTIONS PURGE;

COMMIT;
show errors;
exit;  
