------------------------------------------------------------------------------
-- TASK					: 	TRUNCATE ONLY TARIFF TABLES.   
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	TRUNCATE_TARIFF_TABLES.sql
--
-- CREATED        		: 	02/03/2016
--	
-- DESCRIPTION 		   	: 	Truncate tables related to TARIFF
--
-- NOTES  				:	Run p_enable_disable_fk first to disable all constraints.  Once this script has been run re-enable
--							constraints.  						
--
-- ASSOCIATED FILES		:	p_truncate_mo_tables.sql
-- ASSOCIATED SCRIPTS  	:	p_truncate_mo_tables.sql
--
-- PARAMETERES			:	Two parameteres required.
-- USAGE				:	
-- EXAMPLE				:	
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date                Author         		Description
-- ---------      	---------------     -------             ------------------------------------------------
-- V0.01       		02/03/2016    		N.Henderson         Initial version.
--
--
--
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
truncate table MO_AS_BAND_CHARGE;
truncate table MO_AS_METER_ASMFC;
truncate table MO_AW_BAND_CHARGE;
truncate table MO_AW_METER_AWMFC;
truncate table MO_HD_AREA_BAND;
truncate table MO_HD_BAND_CHARGE;
truncate table MO_HD_BLOCK_HDBT;
truncate table MO_HD_METER_HDMFC;
truncate table MO_MNPW_BLOCK_MWBT;
truncate table MO_MNPW_METER_MWMFC;
truncate table MO_MNPW_STANDBY_MWCAPCHG;
truncate table MO_MPW_BLOCK_MWBT;
truncate table MO_MPW_METER_MWMFC;
truncate table MO_MPW_STANDBY_MWCAPCHG;
truncate table MO_MS_BLOCK_MSBT;
truncate table MO_MS_METER_MSMFC;
truncate table MO_SW_AREA_BAND;
truncate table MO_SW_BAND_CHARGE;
truncate table MO_SW_BLOCK_SWBT;
truncate table MO_SW_METER_SWMFC;
truncate table MO_TARIFF;
truncate table MO_TARIFF_TYPE_AS;
truncate table MO_TARIFF_TYPE_AW;
truncate table MO_TARIFF_TYPE_HD;
truncate table MO_TARIFF_TYPE_MNPW;
truncate table MO_TARIFF_TYPE_MPW;
truncate table MO_TARIFF_TYPE_MS;
truncate table MO_TARIFF_TYPE_SW;
truncate table MO_TARIFF_TYPE_TE;
truncate table MO_TARIFF_TYPE_US;
truncate table MO_TARIFF_TYPE_UW;
truncate table MO_TARIFF_VERSION;
truncate table MO_TE_BAND_CHARGE;
truncate table MO_TE_BLOCK_BOBT;
truncate table MO_TE_BLOCK_ROBT;
truncate table MO_US_METER_USPFC;
truncate table MO_UW_METER_UWPFC;
