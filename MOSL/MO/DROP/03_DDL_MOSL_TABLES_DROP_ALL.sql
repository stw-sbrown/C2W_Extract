------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS DROP 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	03_DDL_MOSL_TABLES_DROP_ALL.sql
--
-- CREATED        		: 	22/02/2016
--	
-- Subversion $Revision: 4023 $
--
-- DESCRIPTION 		   	: 	Drops all tables
--
-- NOTES  				:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order
-- 							as detailed below (.sql).
--
-- ASSOCIATED SCRIPTS  	:	01_DDL_MOSL_DROP_STATIC_DATA_ALL.sql
--					02_DDL_MOSL_FK_DROP_ALL.sql
--					04_DDL_MOSL_DROP_SEQUENCES_ALL
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          Author         		Description
-- ---------      	---------------     -------            	 ------------------------------------------------
-- V0.01       	15/02/2016    	N.Henderson     	Initial version after generation from Excel
-- 
-- V0.02	15/03/2016	N.Henderson		Commented out MO_RETAILER_REGISTRATION as the table was removed
--							in patch MO_P0009
--
--
-- V0.03	11/04/2016	N.Henderson		Added the removal of three new tables as part of TARIFF additions 
-- 
------------------------------------------------------------------------------------------------------------

--DROP TABLES
--MO_CUSTOMER
DROP TABLE MO_CUSTOMER CASCADE CONSTRAINTS;
--MO_ELIGIBLE_PREMISES
DROP TABLE MO_ELIGIBLE_PREMISES CASCADE CONSTRAINTS;
--MO_SUPPLY_POINT
DROP TABLE MO_SUPPLY_POINT CASCADE CONSTRAINTS;
--MO_ORG
DROP TABLE MO_ORG CASCADE CONSTRAINTS;
--MO_RETAILER_REGISTRATION
--DROP TABLE MO_RETAILER_REGISTRATION CASCADE CONSTRAINTS;
--MO_SERVICE_COMPONENT
DROP TABLE MO_SERVICE_COMPONENT CASCADE CONSTRAINTS;
--MO_SERVICE_COMPONENT_TYPE
DROP TABLE MO_SERVICE_COMPONENT_TYPE CASCADE CONSTRAINTS;
--MO_SERVICE_COMPONENT_VOL_ADJ
DROP TABLE MO_SERVICE_COMPONENT_VOL_ADJ CASCADE CONSTRAINTS;
--MO_DISCHARGE_POINT
DROP TABLE MO_DISCHARGE_POINT CASCADE CONSTRAINTS;
--MO_CALCULATED_DISCHARGE
DROP TABLE MO_CALCULATED_DISCHARGE CASCADE CONSTRAINTS;
--MO_DISCHARGED_VOLUME
DROP TABLE MO_DISCHARGED_VOLUME CASCADE CONSTRAINTS;
--MO_DISCHARGE_POINT_VOLMET_ADJ
DROP TABLE MO_DISCHARGE_POINT_VOLMET_ADJ CASCADE CONSTRAINTS;
--MO_METER
DROP TABLE MO_METER CASCADE CONSTRAINTS;
--MO_METER_READING
DROP TABLE MO_METER_READING CASCADE CONSTRAINTS;
--MO_METER_DPIDXREF
DROP TABLE MO_METER_DPIDXREF CASCADE CONSTRAINTS;
--MO_CUST_ADDRESS
DROP TABLE MO_CUST_ADDRESS CASCADE CONSTRAINTS;
--MO_PROPERTY_ADDRESS
DROP TABLE MO_PROPERTY_ADDRESS CASCADE CONSTRAINTS;
--MO_METER_ADDRESS
DROP TABLE MO_METER_ADDRESS CASCADE CONSTRAINTS;
--MO_ADDRESS
DROP TABLE MO_ADDRESS CASCADE CONSTRAINTS;
DROP TABLE MO_METER_NETWORK CASCADE CONSTRAINTS;
DROP TABLE MO_METER_SPID_ASSOC CASCADE CONSTRAINTS;


--DROP_TABLES_DDL
--MO_TARIFF
DROP TABLE MO_TARIFF CASCADE CONSTRAINTS;
--MO_TARIFF_VERSION
DROP TABLE MO_TARIFF_VERSION CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_MPW
DROP TABLE MO_TARIFF_TYPE_MPW CASCADE CONSTRAINTS;
--MO_MPW_METER_MWMFC
DROP TABLE MO_MPW_METER_MWMFC CASCADE CONSTRAINTS;
--MO_MPW_BLOCK_MWBT
DROP TABLE MO_MPW_BLOCK_MWBT CASCADE CONSTRAINTS;
--MO_MPW_STANDBY_MWCAPCHG
DROP TABLE MO_MPW_STANDBY_MWCAPCHG CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_MNPW
DROP TABLE MO_TARIFF_TYPE_MNPW CASCADE CONSTRAINTS;
--MO_MNPW_METER_MWMFC
DROP TABLE MO_MNPW_METER_MWMFC CASCADE CONSTRAINTS;
--MO_MNPW_BLOCK_MWBT
DROP TABLE MO_MNPW_BLOCK_MWBT CASCADE CONSTRAINTS;
--MO_MNPW_STANDBY_MWCAPCHG
DROP TABLE MO_MNPW_STANDBY_MWCAPCHG CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_AW
DROP TABLE MO_TARIFF_TYPE_AW CASCADE CONSTRAINTS;
--MO_AW_METER_AWMFC
DROP TABLE MO_AW_METER_AWMFC CASCADE CONSTRAINTS;
--MO_AW_BAND_CHARGE
DROP TABLE MO_AW_BAND_CHARGE CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_UW
DROP TABLE MO_TARIFF_TYPE_UW CASCADE CONSTRAINTS;
--MO_UW_METER_UWPFC
DROP TABLE MO_UW_METER_UWPFC CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_MS
DROP TABLE MO_TARIFF_TYPE_MS CASCADE CONSTRAINTS;
--MO_MS_BLOCK_MSBT
DROP TABLE MO_MS_BLOCK_MSBT CASCADE CONSTRAINTS;
--MO_MS_METER_MSMFC
DROP TABLE MO_MS_METER_MSMFC CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_AS
DROP TABLE MO_TARIFF_TYPE_AS CASCADE CONSTRAINTS;
--MO_AS_METER_ASMFC
DROP TABLE MO_AS_METER_ASMFC CASCADE CONSTRAINTS;
--MO_AS_BAND_CHARGE
DROP TABLE MO_AS_BAND_CHARGE CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_US
DROP TABLE MO_TARIFF_TYPE_US CASCADE CONSTRAINTS;
--MO_US_METER_USPFC
DROP TABLE MO_US_METER_USPFC CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_SW
DROP TABLE MO_TARIFF_TYPE_SW CASCADE CONSTRAINTS;
--MO_SW_AREA_BAND
DROP TABLE MO_SW_AREA_BAND CASCADE CONSTRAINTS;
--MO_SW_BAND_CHARGE
DROP TABLE MO_SW_BAND_CHARGE CASCADE CONSTRAINTS;
--MO_SW_BLOCK_SWBT
DROP TABLE MO_SW_BLOCK_SWBT CASCADE CONSTRAINTS;
--MO_SW_METER_SWMFC
DROP TABLE MO_SW_METER_SWMFC CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_HD
DROP TABLE MO_TARIFF_TYPE_HD CASCADE CONSTRAINTS;
--MO_HD_AREA_BAND
DROP TABLE MO_HD_AREA_BAND CASCADE CONSTRAINTS;
--MO_HD_BAND_CHARGE
DROP TABLE MO_HD_BAND_CHARGE CASCADE CONSTRAINTS;
--MO_HD_BLOCK_HDBT
DROP TABLE MO_HD_BLOCK_HDBT CASCADE CONSTRAINTS;
--MO_HD_METER_HDMFC
DROP TABLE MO_HD_METER_HDMFC CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_TE
DROP TABLE MO_TARIFF_TYPE_TE CASCADE CONSTRAINTS;
--MO_TE_BAND_CHARGE
DROP TABLE MO_TE_BAND_CHARGE CASCADE CONSTRAINTS;
--MO_TE_BLOCK_ROBT
DROP TABLE MO_TE_BLOCK_ROBT CASCADE CONSTRAINTS;
--MO_TE_BLOCK_BOBT
DROP TABLE MO_TE_BLOCK_BOBT CASCADE CONSTRAINTS;

--NEW TABLES WERE CREATED AS PART OF PATCH16.  ADDED THE REMOVAL HERE

--MO_TARIFF_TYPE_SCA
DROP TABLE MO_TARIFF_TYPE_SCA CASCADE CONSTRAINTS;
--MO_TARIFF_TYPE_WCA
DROP TABLE MO_TARIFF_TYPE_WCA CASCADE CONSTRAINTS;
--MO_TARIFF_STANDING_DATA
DROP TABLE MO_TARIFF_STANDING_DATA CASCADE CONSTRAINTS;
DROP TABLE MS_STG;
DROP TABLE AW_STG;
DROP TABLE US_STG;
DROP TABLE AS_STG;
DROP TABLE SW_STG;
DROP TABLE TE_STG;
DROP TABLE UW_STG;
DROP TABLE MPW_STG;
DROP TABLE HD_STG;

--DROP PROCEDURES
DROP PROCEDURE P_MIG_RECON;
DROP PROCEDURE P_MOU_TRAN_ADDRESS;
DROP PROCEDURE P_MOU_TRAN_CUSTOMER;
DROP PROCEDURE P_MOU_TRAN_DISCHARGE_POINT;
DROP PROCEDURE P_MOU_TRAN_KEY_GEN;
DROP PROCEDURE P_MOU_TRAN_METER_TARGET;
DROP PROCEDURE P_MOU_TRAN_METER_NETWORK;
DROP PROCEDURE P_MOU_TRAN_METER_SPID_ASSOC;
DROP PROCEDURE P_MOU_TRAN_PROPERTY;
DROP PROCEDURE P_MOU_TRAN_SC_MPW;
DROP PROCEDURE P_MOU_TRAN_SC_PRE;
DROP PROCEDURE P_MOU_TRAN_SC_UW;
DROP PROCEDURE P_MOU_TRAN_SERVICE_COMPONENT;
DROP PROCEDURE P_MOU_TRAN_SUPPLY_POINT;
DROP PROCEDURE P_MOU_TRAN_TE_SUMMARY;
DROP PROCEDURE P_MOU_TRAN_TE_WORKING;
DROP PROCEDURE P_REMOVE_ADDRESS_PK_CONST;
DROP PROCEDURE P_TRUNCATE_MO_TABLES;
commit;

--DROP FUNCTIONS
DROP FUNCTION FN_VALIDATE_POSTCODE;

--DROP PACKAGES
DROP PACKAGE P_MIG_BATCH;
DROP PACKAGE P_MIG_TARIFF;



commit;
exit;

