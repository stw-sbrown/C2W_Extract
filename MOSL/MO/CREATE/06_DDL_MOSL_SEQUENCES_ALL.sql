------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS CREATION 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	05_COMMENTS_TABLES_FIELDS_ALL.sql
--
--	
-- Subversion $Revision: 4023 $
--
-- CREATED        		: 	22/02/2016
--	
-- DESCRIPTION 		   	: 	Adds comments to tables and fields. 
--
-- NOTES  				:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order.
--
-- ASSOCIATED FILES		:	MOSL_PDM_CORE.xlsm
-- ASSOCIATED SCRIPTS  	:	01_DDL_MOSL_TABLES_ALL.sql
--							02_DDL_MOSL_PK_ALL.sql
--							03_DDL_MOSL_FK_ALL.sql
--							04_DDL_MOSL_STATIC_DATA_ALL.sql
--
--
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date          Author         		Description
-- ---------      	---------------     -------             ------------------------------------------------
-- V0.01       	15/02/2016    	N.Henderson         Initial version after generation from Excel
--
-- V0.02		25/02/2016		N.Henderson		Had to replace all comments symbols with --
--									Doing something wierd when running query from
--									command line.
-- V0.03		25/02/2016		N.Henderson		Duplicate names for ADDRESSPROPERTY_PK_SEQ
--									updated to suite.
--
--
--
-- 
------------------------------------------------------------------------------------------------------------




--AUTONUNUMBER SEQUENCES

--MO_SERVICE_COMPONENT_TYPE
CREATE SEQUENCE SERVICECOMPONENT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_SERVICE_COMPONENT_VOL_ADJ
CREATE SEQUENCE ADJVOLADJUNIQREF_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_DISCHARGE_POINT
--MO_CALCULATED_DISCHARGE
CREATE SEQUENCE CALCDISCHARGEID_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_DISCHARGED_VOLUME
CREATE SEQUENCE DISCHARGEVOLUME_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_DISCHARGE_POINT_VOLMET_ADJ
CREATE SEQUENCE DISCHARGEPOINTVOLMETADJ_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_METER_READING
CREATE SEQUENCE METER_READING_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_METER_DPIDXREF
CREATE SEQUENCE METERDPIDXREF_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_CUST_ADDRESS
CREATE SEQUENCE ADDRCUSTPROPERTY_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_PROPERTY_ADDRESS
CREATE SEQUENCE ADDRESSPROPERTY_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_METER_ADDRESS
CREATE SEQUENCE ADDRMETERROPERTY_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_ADDRESS
CREATE SEQUENCE ADDRESS_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_MPW
CREATE SEQUENCE MPW_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MPW_METER_MWMFC
CREATE SEQUENCE MPW_TARIFF_MWMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MPW_BLOCK_MWBT
CREATE SEQUENCE MPW_TARIFF_MWBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MPW_STANDBY_MWCAPCHG
CREATE SEQUENCE MPW_TARIFF_MWCAPCHG_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_MNPW
CREATE SEQUENCE MNPW_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MNPW_METER_MWMFC
CREATE SEQUENCE MNPW_TARIFF_MWMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MNPW_BLOCK_MWBT
CREATE SEQUENCE MNPW_TARIFF_MWBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MNPW_STANDBY_MWCAPCHG
CREATE SEQUENCE MNPW_TARIFF_MWCAPCHG_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_AW
CREATE SEQUENCE AW_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_AW_METER_AWMFC
CREATE SEQUENCE AW_TARIFF_AWMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_AW_BAND_CHARGE
CREATE SEQUENCE AW_TARIFF_BAND_CHARGE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_UW
CREATE SEQUENCE UW_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_UW_METER_UWPFC
CREATE SEQUENCE UW_TARIFF_UWPFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_MS
CREATE SEQUENCE MS_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MS_BLOCK_MSBT
CREATE SEQUENCE MS_TARIFF_MWBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_MS_METER_MSMFC
CREATE SEQUENCE MS_TARIFF_MSMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_AS
CREATE SEQUENCE AS_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_AS_METER_ASMFC
CREATE SEQUENCE AS_TARIFF_ASMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_AS_BAND_CHARGE
CREATE SEQUENCE AS_TARIFF_BAND_CHARGE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_US
CREATE SEQUENCE US_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_US_METER_USPFC
CREATE SEQUENCE US_TARIFF_USPFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_SW
CREATE SEQUENCE SW_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_SW_AREA_BAND
CREATE SEQUENCE SW_TARIFF_AREA_BAND_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_SW_BAND_CHARGE
CREATE SEQUENCE SW_TARIFF_BAND_CHARGE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_SW_BLOCK_SWBT
CREATE SEQUENCE SW_TARIFF_SWBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_SW_METER_SWMFC
CREATE SEQUENCE SW_TARIFF_SWMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_HD
CREATE SEQUENCE HD_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_HD_AREA_BAND
CREATE SEQUENCE HD_TARIFF_AREA_BAND_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_HD_BAND_CHARGE
CREATE SEQUENCE HD_TARIFF_BAND_CHARGE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_HD_BLOCK_HDBT
CREATE SEQUENCE HD_TARIFF_HDBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_HD_METER_HDMFC
CREATE SEQUENCE HD_TARIFF_HDMFC_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TARIFF_TYPE_TE
CREATE SEQUENCE TE_TARIFF_TYPE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TE_BAND_CHARGE
CREATE SEQUENCE TE_TARIFF_BAND_CHARGE_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TE_BLOCK_ROBT
CREATE SEQUENCE TE_TARIFF_ROBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
--MO_TE_BLOCK_BOBT
CREATE SEQUENCE TE_TARIFF_BOBT_PK_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;
commit;
exit;

