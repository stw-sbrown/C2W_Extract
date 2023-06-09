------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS CREATION 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	03_DDL_MOSL_FK_ALL.sql
--
-- CREATED        		: 	22/02/2016
--
--	
-- Subversion $Revision: 4023 $
--	
-- DESCRIPTION 		   	: 	Adds foreign keys
--
-- NOTES  				:	Used in conjunction with the following scripts to re-initialise the
-- 							database.  When re-creating the database run the scripts in order.
--
-- ASSOCIATED FILES		:	MOSL_PDM_CORE.xlsm
-- ASSOCIATED SCRIPTS  	:	01_DDL_MOSL_TABLES_ALL.sql
--							02_DDL_MOSL_PK_ALL.sql
--							04_DDL_MOSL_STATIC_DATA_ALL.sql
--							05_COMMENTS_TABLES_FIELDS_ALL
--
--
--
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date                Author         		Description
-- ---------      	---------------     -------             ------------------------------------------------
-- V0.01       	15/02/2016    	N.Henderson         Initial version after generation from Excel
-- V0.02		25/02/2016		N.Henderson		Had to replace all comments symbols with --
--									Doing something wierd when running query from
--									command line.
--
--			
--
--
--
-- 
------------------------------------------------------------------------------------------------------------

--FK
--RETAILER_REGISTRATION
ALTER TABLE MO_RETAILER_REGISTRATION ADD CONSTRAINT FK_RETAILERID_PK02 FOREIGN KEY ("RETAILERID_PK") REFERENCES "MO_ORG"("ORGID_PK");
ALTER TABLE MO_RETAILER_REGISTRATION ADD CONSTRAINT FK_SPID_PK01 FOREIGN KEY ("SPID_PK") REFERENCES "MO_SUPPLY_POINT"("SPID_PK");

--MO_SUPPLY_POINT
ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT FK_STWPROPERTYNUMBER_PK02 FOREIGN KEY ("STWPROPERTYNUMBER_PK") REFERENCES "MO_ELIGIBLE_PREMISES"("STWPROPERTYNUMBER_PK");
ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT FK_CORESPID_PK FOREIGN KEY ("CORESPID_PK") REFERENCES "MO_ELIGIBLE_PREMISES"("CORESPID_PK");
ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT FK_RETAILERID_PK01 FOREIGN KEY ("RETAILERID_PK") REFERENCES "MO_RETAILER_REGISTRATION"("RETAILERID_PK");
ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT FK_WHOLESALERID_PK FOREIGN KEY ("WHOLESALERID_PK") REFERENCES "MO_ORG"("ORGID_PK");

--MO_SERVICE_COMPONENT
ALTER TABLE MO_SERVICE_COMPONENT ADD CONSTRAINT FK_SERVICECOMPONENTREF_PK01 FOREIGN KEY ("SERVICECOMPONENTREF_PK") REFERENCES "MO_SERVICE_COMPONENT_TYPE"("SERVICECOMPONENT_PK");
ALTER TABLE MO_SERVICE_COMPONENT ADD CONSTRAINT FK_TARIFFCODE_PK FOREIGN KEY ("TARIFFCODE_PK") REFERENCES "MO_TARIFF"("TARIFFCODE_PK");
ALTER TABLE MO_SERVICE_COMPONENT ADD CONSTRAINT FK_SPID_PK02 FOREIGN KEY ("SPID_PK") REFERENCES "MO_SUPPLY_POINT"("SPID_PK");
ALTER TABLE MO_SERVICE_COMPONENT ADD CONSTRAINT FK_DPID_PK01 FOREIGN KEY ("DPID_PK") REFERENCES "MO_DISCHARGE_POINT"("DPID_PK");

--MO_SERVICE_COMPONENT_VOL_ADJ
ALTER TABLE MO_SERVICE_COMPONENT_VOL_ADJ ADD CONSTRAINT FK_SERVICECOMPONENTREF_PK02 FOREIGN KEY ("SERVICECOMPONENTREF_PK") REFERENCES "MO_SERVICE_COMPONENT"("SERVICECOMPONENTREF_PK");

--MO_ELIGIBLE_PREMISES
ALTER TABLE MO_ELIGIBLE_PREMISES ADD CONSTRAINT FK_CUSTOMERID_PK FOREIGN KEY ("CUSTOMERID_PK") REFERENCES "MO_CUSTOMER"("CUSTOMERNUMBER_PK");

--MO_CUSTOMER
ALTER TABLE MO_CUSTOMER ADD CONSTRAINT FK_STWPROPERTYNUMBER_PK01 FOREIGN KEY ("STWPROPERTYNUMBER_PK") REFERENCES "MO_ELIGIBLE_PREMISES"("STWPROPERTYNUMBER_PK");

--MO_DISCHARGE_POINT
ALTER TABLE MO_DISCHARGE_POINT ADD CONSTRAINT FK_SPID_PK FOREIGN KEY ("SPID_PK") REFERENCES "MO_SUPPLY_POINT"("SPID_PK");

--MO_METER
ALTER TABLE MO_METER ADD CONSTRAINT FK_SPID_PK03 FOREIGN KEY ("SPID_PK") REFERENCES "MO_SUPPLY_POINT"("SPID_PK");

--MO_METER_READING
ALTER TABLE MO_METER_READING ADD CONSTRAINT FK_MANUFACTURER_PK01 FOREIGN KEY ("MANUFACTURER_PK") REFERENCES "MO_METER"("MANUFACTURER_PK");
ALTER TABLE MO_METER_READING ADD CONSTRAINT FK_MANUFACTURERSERIALNUM_PK01 FOREIGN KEY ("MANUFACTURERSERIALNUM_PK") REFERENCES "MO_METER"("MANUFACTURERSERIALNUM_PK");


--MO_METER_DPIDXREF
ALTER TABLE MO_METER_DPIDXREF ADD CONSTRAINT FK_MANUFACTURER_PK02 FOREIGN KEY ("MANUFACTURER_PK") REFERENCES "MO_METER"("MANUFACTURER_PK");
ALTER TABLE MO_METER_DPIDXREF ADD CONSTRAINT FK_MANUFACTURERSERIALNUM_PK02 FOREIGN KEY ("MANUFACTURERSERIALNUM_PK") REFERENCES "MO_METER"("MANUFACTURERSERIALNUM_PK");
ALTER TABLE MO_METER_DPIDXREF ADD CONSTRAINT FK_DPID_PK05 FOREIGN KEY ("DPID_PK") REFERENCES "MO_DISCHARGE_POINT"("DPID_PK");


--MO_PROPERTY_ADDRESS
ALTER TABLE MO_PROPERTY_ADDRESS ADD CONSTRAINT FK_ADDRESS_PK02 FOREIGN KEY ("ADDRESS_PK") REFERENCES "MO_ADDRESS"("ADDRESS_PK");
ALTER TABLE MO_PROPERTY_ADDRESS ADD CONSTRAINT FK_STWPROPERTYNUMBER_PK03 FOREIGN KEY ("STWPROPERTYNUMBER_PK") REFERENCES "MO_ELIGIBLE_PREMISES"("STWPROPERTYNUMBER_PK");

--MO_CUST_ADDRESS
ALTER TABLE MO_CUST_ADDRESS ADD CONSTRAINT FK_ADDRESS_PK01 FOREIGN KEY ("ADDRESS_PK") REFERENCES "MO_ADDRESS"("ADDRESS_PK");
ALTER TABLE MO_CUST_ADDRESS ADD CONSTRAINT FK_CUSTOMERNUMBER_PK FOREIGN KEY ("CUSTOMERNUMBER_PK") REFERENCES "MO_CUSTOMER"("CUSTOMERNUMBER_PK");

--MO_DISCHARGE_POINT_VOLMET_ADJ
ALTER TABLE MO_DISCHARGE_POINT_VOLMET_ADJ ADD CONSTRAINT FK_DPID_PK04 FOREIGN KEY ("DPID_PK") REFERENCES "MO_DISCHARGE_POINT"("DPID_PK");

--MO_CALCULATED_DISCHARGE
ALTER TABLE MO_CALCULATED_DISCHARGE ADD CONSTRAINT FK_DPID_PK02 FOREIGN KEY ("DPID_PK") REFERENCES "MO_DISCHARGE_POINT"("DPID_PK");

--MO_METER_ADDRESS
ALTER TABLE MO_METER_ADDRESS ADD CONSTRAINT FK_METERSERIALNUMBER_PK FOREIGN KEY ("METERSERIALNUMBER_PK") REFERENCES "MO_METER"("MANUFACTURERSERIALNUM_PK");
ALTER TABLE MO_METER_ADDRESS ADD CONSTRAINT FK_ADDRESS_PK03 FOREIGN KEY ("ADDRESS_PK") REFERENCES "MO_ADDRESS"("ADDRESS_PK");

--MO_DISCHARGED_VOLUME
ALTER TABLE MO_DISCHARGED_VOLUME ADD CONSTRAINT FK_DPID_PK03 FOREIGN KEY ("DPID_PK") REFERENCES "MO_CALCULATED_DISCHARGE"("CALCDISCHARGEID_PK");


--MO_ADDRESS
--No foriegn keys here


--FK
--MO_TARIFF
ALTER TABLE "MO_TARIFF" ADD CONSTRAINT FK_SERVICECOMPONENTREF_PK03 FOREIGN KEY("SERVICECOMPONENTREF_PK") REFERENCES "MO_SERVICE_COMPONENT"("SERVICECOMPONENTREF_PK");
--MO_TARIFF_VERSION
ALTER TABLE "MO_TARIFF_VERSION" ADD CONSTRAINT FK_MO_TARIFF_CODE FOREIGN KEY("TARIFFCODE_PK") REFERENCES "MO_TARIFF"("TARIFFCODE_PK");
 

--MO_TARIFF_TYPE_MPW
ALTER TABLE MO_TARIFF_TYPE_MPW ADD CONSTRAINT FK_TARIFF_VERSION_PK_MPW FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_MPW_METER_MWMFC
ALTER TABLE MO_MPW_METER_MWMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK01 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MPW"("TARIFF_TYPE_PK");
--MO_MPW_BLOCK_MWBT
ALTER TABLE MO_MPW_BLOCK_MWBT ADD CONSTRAINT FK_TARIFF_TYPE_PK02 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MPW"("TARIFF_TYPE_PK");
--MO_MPW_STANDBY_MWCAPCHG
ALTER TABLE MO_MPW_STANDBY_MWCAPCHG ADD CONSTRAINT FK_TARIFF_TYPE_PK03 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MPW"("TARIFF_TYPE_PK");

--MO_TARIFF_TYPE_MNPW
ALTER TABLE MO_TARIFF_TYPE_MNPW ADD CONSTRAINT FK_TARIFF_VERSION_PK_MNPW FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_MNPW_METER_MWMFC
ALTER TABLE MO_MNPW_METER_MWMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK04 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MNPW"("TARIFF_TYPE_PK");
--MO_MNPW_BLOCK_MWBT
ALTER TABLE MO_MNPW_BLOCK_MWBT ADD CONSTRAINT FK_TARIFF_TYPE_PK05 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MNPW"("TARIFF_TYPE_PK");
--MO_MNPW_STANDBY_MWCAPCHG
ALTER TABLE MO_MNPW_STANDBY_MWCAPCHG ADD CONSTRAINT FK_TARIFF_TYPE_PK06 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MNPW"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_AW
ALTER TABLE MO_TARIFF_TYPE_AW ADD CONSTRAINT FK_TARIFF_VERSION_PK_AW FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_AW_METER_AWMFC
ALTER TABLE MO_AW_METER_AWMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK07 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_AW"("TARIFF_TYPE_PK");
--MO_AW_BAND_CHARGE
ALTER TABLE MO_AW_BAND_CHARGE ADD CONSTRAINT FK_TARIFF_TYPE_PK08 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_AW"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_UW
ALTER TABLE MO_TARIFF_TYPE_UW ADD CONSTRAINT FK_TARIFF_VERSION_PK_UW FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_UW_METER_UWPFC
ALTER TABLE MO_UW_METER_UWPFC ADD CONSTRAINT FK_TARIFF_TYPE_PK09 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_UW"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_MS
ALTER TABLE MO_TARIFF_TYPE_MS ADD CONSTRAINT FK_TARIFF_VERSION_PK_MS FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_MS_BLOCK_MSBT
ALTER TABLE MO_MS_BLOCK_MSBT ADD CONSTRAINT FK_TARIFF_TYPE_PK10 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MS"("TARIFF_TYPE_PK");
--MO_MS_METER_MSMFC
ALTER TABLE MO_MS_METER_MSMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK11 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_MS"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_AS
ALTER TABLE MO_TARIFF_TYPE_AS ADD CONSTRAINT FK_TARIFF_VERSION_PK_AS FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_AS_METER_ASMFC
ALTER TABLE MO_AS_METER_ASMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK12 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_AS"("TARIFF_TYPE_PK");
--MO_AS_BAND_CHARGE
ALTER TABLE MO_AS_BAND_CHARGE ADD CONSTRAINT FK_TARIFF_TYPE_PK13 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_AS"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_US
ALTER TABLE MO_TARIFF_TYPE_US ADD CONSTRAINT FK_TARIFF_VERSION_PK_US FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_US_METER_USPFC
ALTER TABLE MO_US_METER_USPFC ADD CONSTRAINT FK_TARIFF_TYPE_PK14 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_US"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_SW
ALTER TABLE MO_TARIFF_TYPE_SW ADD CONSTRAINT FK_TARIFF_VERSION_PK_SW FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_SW_AREA_BAND
ALTER TABLE MO_SW_AREA_BAND ADD CONSTRAINT FK_TARIFF_TYPE_PK15 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_SW"("TARIFF_TYPE_PK");
--MO_SW_BAND_CHARGE
ALTER TABLE MO_SW_BAND_CHARGE ADD CONSTRAINT FK_TARIFF_TYPE_PK16 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_SW"("TARIFF_TYPE_PK");
--MO_SW_BLOCK_SWBT
ALTER TABLE MO_SW_BLOCK_SWBT ADD CONSTRAINT FK_TARIFF_TYPE_PK17 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_SW"("TARIFF_TYPE_PK");
--MO_SW_METER_SWMFC
ALTER TABLE MO_SW_METER_SWMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK18 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_SW"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_HD
ALTER TABLE MO_TARIFF_TYPE_HD ADD CONSTRAINT FK_TARIFF_VERSION_PK_HD FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_HD_AREA_BAND
ALTER TABLE MO_HD_AREA_BAND ADD CONSTRAINT FK_TARIFF_TYPE_PK19 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_HD"("TARIFF_TYPE_PK");
--MO_HD_BAND_CHARGE
ALTER TABLE MO_HD_BAND_CHARGE ADD CONSTRAINT FK_TARIFF_TYPE_PK20 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_HD"("TARIFF_TYPE_PK");
--MO_HD_BLOCK_HDBT
ALTER TABLE MO_HD_BLOCK_HDBT ADD CONSTRAINT FK_TARIFF_TYPE_PK21 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_HD"("TARIFF_TYPE_PK");
--MO_HD_METER_HDMFC
ALTER TABLE MO_HD_METER_HDMFC ADD CONSTRAINT FK_TARIFF_TYPE_PK22 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_HD"("TARIFF_TYPE_PK");
--MO_TARIFF_TYPE_TE
ALTER TABLE MO_TARIFF_TYPE_TE ADD CONSTRAINT FK_TARIFF_VERSION_PK_TE FOREIGN KEY ("TARIFF_VERSION_PK") REFERENCES "MO_TARIFF_VERSION"("TARIFF_VERSION_PK");
--MO_TE_BAND_CHARGE
ALTER TABLE MO_TE_BAND_CHARGE ADD CONSTRAINT FK_TARIFF_TYPE_PK23 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_TE"("TARIFF_TYPE_PK");
--MO_TE_BLOCK_ROBT
ALTER TABLE MO_TE_BLOCK_ROBT ADD CONSTRAINT FK_TARIFF_TYPE_PK24 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_TE"("TARIFF_TYPE_PK");
--MO_TE_BLOCK_BOBT
ALTER TABLE MO_TE_BLOCK_BOBT ADD CONSTRAINT FK_TARIFF_TYPE_PK25 FOREIGN KEY ("TARIFF_TYPE_PK") REFERENCES "MO_TARIFF_TYPE_TE"("TARIFF_TYPE_PK");

exit;

