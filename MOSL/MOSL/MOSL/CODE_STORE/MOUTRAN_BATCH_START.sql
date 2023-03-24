-- N.Henderson 13/04/2016.
-- V 0.11 
-- Subversion $Revision: 4023 $
--
-- Modification history
-- Added new FK disablements, also added 
-- 	
-- This script completes the following tasks
--
-- 1, Switches off a number of FK's 
-- 2, Removes data from tables
-- 3, Switches FK's back on
-- 4, Executes the TARIFF build
-- 5, Executes the main batch run

--Query to regenerate data for enable/disable constraints
--select 'alter table '||table_name||' enable constraint '||constraint_name||';'
--from user_constraints
--where constraint_type = 'R';

--Switch off ALL FK's 
alter table MO_TE_BAND_CHARGE disable constraint FK_TARIFF_TYPE_PK23;
alter table MO_TE_BLOCK_ROBT disable constraint FK_TARIFF_TYPE_PK24;
alter table MO_TE_BLOCK_BOBT disable constraint FK_TARIFF_TYPE_PK25;
alter table MO_HD_AREA_BAND disable constraint FK_TARIFF_TYPE_PK19;
alter table MO_HD_BAND_CHARGE disable constraint FK_TARIFF_TYPE_PK20;
alter table MO_HD_BLOCK_HDBT disable constraint FK_TARIFF_TYPE_PK21;
alter table MO_HD_METER_HDMFC disable constraint FK_TARIFF_TYPE_PK22;
alter table MO_SW_AREA_BAND disable constraint FK_TARIFF_TYPE_PK15;
alter table MO_SW_BAND_CHARGE disable constraint FK_TARIFF_TYPE_PK16;
alter table MO_SW_BLOCK_SWBT disable constraint FK_TARIFF_TYPE_PK17;
alter table MO_SW_METER_SWMFC disable constraint FK_TARIFF_TYPE_PK18;
alter table MO_US_METER_USPFC disable constraint FK_TARIFF_TYPE_PK14;
alter table MO_AS_METER_ASMFC disable constraint FK_TARIFF_TYPE_PK12;
alter table MO_AS_BAND_CHARGE disable constraint FK_TARIFF_TYPE_PK13;
alter table MO_MS_BLOCK_MSBT disable constraint FK_TARIFF_TYPE_PK10;
alter table MO_MS_METER_MSMFC disable constraint FK_TARIFF_TYPE_PK11;
alter table MO_UW_METER_UWPFC disable constraint FK_TARIFF_TYPE_PK09;
alter table MO_AW_METER_AWMFC disable constraint FK_TARIFF_TYPE_PK07;
alter table MO_AW_BAND_CHARGE disable constraint FK_TARIFF_TYPE_PK08;
alter table MO_MNPW_METER_MWMFC disable constraint FK_TARIFF_TYPE_PK04;
alter table MO_MNPW_BLOCK_MWBT disable constraint FK_TARIFF_TYPE_PK05;
alter table MO_MNPW_STANDBY_MWCAPCHG disable constraint FK_TARIFF_TYPE_PK06;
alter table MO_MPW_METER_MWMFC disable constraint FK_TARIFF_TYPE_PK01;
alter table MO_MPW_BLOCK_MWBT disable constraint FK_TARIFF_TYPE_PK02;
alter table MO_MPW_STANDBY_MWCAPCHG disable constraint FK_TARIFF_TYPE_PK03;
alter table MO_TARIFF_TYPE_MPW disable constraint FK_TARIFF_VERSION_PK_MPW;
alter table MO_TARIFF_TYPE_MNPW disable constraint FK_TARIFF_VERSION_PK_MNPW;
alter table MO_TARIFF_TYPE_AW disable constraint FK_TARIFF_VERSION_PK_AW;
alter table MO_TARIFF_TYPE_UW disable constraint FK_TARIFF_VERSION_PK_UW;
alter table MO_TARIFF_TYPE_MS disable constraint FK_TARIFF_VERSION_PK_MS;
alter table MO_TARIFF_TYPE_AS disable constraint FK_TARIFF_VERSION_PK_AS;
alter table MO_TARIFF_TYPE_US disable constraint FK_TARIFF_VERSION_PK_US;
alter table MO_TARIFF_TYPE_SW disable constraint FK_TARIFF_VERSION_PK_SW;
alter table MO_TARIFF_TYPE_HD disable constraint FK_TARIFF_VERSION_PK_HD;
alter table MO_TARIFF_TYPE_TE disable constraint FK_TARIFF_VERSION_PK_TE;
alter table MO_SERVICE_COMPONENT disable constraint FK_TARIFFCODE_PK;
alter table MO_TARIFF_VERSION disable constraint FK_MO_TARIFF_CODE;
alter table MO_PROPERTY_ADDRESS disable constraint FK_ADDRESS_PK02;
alter table MO_CUST_ADDRESS disable constraint FK_ADDRESS_PK01;
alter table MO_METER_ADDRESS disable constraint FK_ADDRESS_PK03;
alter table MO_METER_NETWORK disable constraint FK_METER_MANUFACT01;
alter table MO_METER_NETWORK disable constraint FK_METER_MANUFACT02;
alter table MO_METER_SPID_ASSOC disable constraint FK_METER_MANUFACT05;
alter table MO_METER_DPIDXREF disable constraint FK_METER_READING_MANUFACT01;
alter table MO_METER_READING disable constraint FK_METER_READING_MANUFACT02;
alter table MO_DISCHARGED_VOLUME disable constraint FK_DPID_PK03;
alter table MO_SERVICE_COMPONENT disable constraint FK_DPID_PK01;
alter table MO_METER_DPIDXREF disable constraint FK_DPID_PK05;
alter table MO_DISCHARGE_POINT_VOLMET_ADJ disable constraint FK_DPID_PK04;
alter table MO_CALCULATED_DISCHARGE disable constraint FK_DPID_PK02;
alter table MO_SERVICE_COMPONENT_VOL_ADJ disable constraint FK_SERVICECOMPONENTREF_PK02;
alter table MO_SUPPLY_POINT disable constraint FK_WHOLESALERID_PK;
alter table MO_SUPPLY_POINT disable constraint FK_RETAILERID_PK02;
alter table MO_SERVICE_COMPONENT disable constraint FK_SPID_PK02;
alter table MO_DISCHARGE_POINT disable constraint FK_SPID_PK;
alter table MO_SUPPLY_POINT disable constraint FK_STWPROPERTYNUMBER_PK02;
alter table MO_SUPPLY_POINT disable constraint FK_CORESPID_PK;
alter table MO_CUSTOMER disable constraint FK_CUST_PROP;
alter table MO_PROPERTY_ADDRESS disable constraint FK_STWPROPERTYNUMBER_PK03;
alter table MO_METER_ADDRESS disable constraint FK_METER_ADDRESS_MAN_COMP;
alter table MO_METER disable constraint FK_SPID_PK03;
commit;


--TRUNCATE DATA 

TRUNCATE TABLE MO_DISCHARGED_VOLUME;
TRUNCATE TABLE MO_DISCHARGE_POINT_VOLMET_ADJ;
TRUNCATE TABLE MO_CALCULATED_DISCHARGE;
TRUNCATE TABLE MO_METER_DPIDXREF;
TRUNCATE TABLE MO_METER_READING;
TRUNCATE TABLE MO_DISCHARGE_POINT;
TRUNCATE TABLE MO_METER;
TRUNCATE TABLE MO_METER_ADDRESS;
TRUNCATE TABLE MO_CUST_ADDRESS;
TRUNCATE TABLE MO_PROPERTY_ADDRESS;
TRUNCATE TABLE MO_ADDRESS;
TRUNCATE TABLE MO_CUSTOMER;
TRUNCATE TABLE MO_SERVICE_COMPONENT_VOL_ADJ;
TRUNCATE TABLE MO_SERVICE_COMPONENT_TYPE;
TRUNCATE TABLE MO_SERVICE_COMPONENT;
--TRUNCATE TABLE MO_ORG;
TRUNCATE TABLE MO_ELIGIBLE_PREMISES;
TRUNCATE TABLE MO_SUPPLY_POINT;
TRUNCATE TABLE MO_METER_SPID_ASSOC;  
TRUNCATE TABLE MO_METER_NETWORK;


--DELETE TAFIFF DATA
delete from MO_AS_BAND_CHARGE;
delete from MO_AS_METER_ASMFC;
delete from MO_AW_BAND_CHARGE;
delete from MO_AW_METER_AWMFC;
delete from MO_HD_AREA_BAND;
delete from MO_HD_BAND_CHARGE;
delete from MO_HD_BLOCK_HDBT;
delete from MO_HD_METER_HDMFC;
delete from MO_MNPW_BLOCK_MWBT;
delete from MO_MNPW_METER_MWMFC;
delete from MO_MNPW_STANDBY_MWCAPCHG;
delete from MO_MPW_BLOCK_MWBT;
delete from MO_MPW_METER_MWMFC;
delete from MO_MPW_STANDBY_MWCAPCHG;
delete from MO_MS_BLOCK_MSBT;
delete from MO_MS_METER_MSMFC;
delete from MO_SW_AREA_BAND;
delete from MO_SW_BAND_CHARGE;
delete from MO_SW_BLOCK_SWBT;
delete from MO_SW_METER_SWMFC;
delete from MO_TE_BAND_CHARGE;
delete from MO_TE_BLOCK_BOBT;
delete from MO_TE_BLOCK_ROBT;
delete from MO_US_METER_USPFC;
delete from mo_uw_meter_uwpfc;
delete from MO_TARIFF_TYPE_AS;
delete from MO_TARIFF_TYPE_AW;
delete from MO_TARIFF_TYPE_HD;
delete from MO_TARIFF_TYPE_MNPW;
delete from MO_TARIFF_TYPE_MPW;
delete from MO_TARIFF_TYPE_MS;
delete from MO_TARIFF_TYPE_SW;
delete from MO_TARIFF_TYPE_TE;
delete from MO_TARIFF_TYPE_US;
delete from MO_TARIFF_TYPE_UW;
delete from MO_TARIFF_VERSION;
delete from MO_TARIFF;


commit;





--SWITCH ALL FK'S BACK ON 
alter table MO_TE_BAND_CHARGE enable constraint FK_TARIFF_TYPE_PK23;
alter table MO_TE_BLOCK_ROBT enable constraint FK_TARIFF_TYPE_PK24;
alter table MO_TE_BLOCK_BOBT enable constraint FK_TARIFF_TYPE_PK25;
alter table MO_HD_AREA_BAND enable constraint FK_TARIFF_TYPE_PK19;
alter table MO_HD_BAND_CHARGE enable constraint FK_TARIFF_TYPE_PK20;
alter table MO_HD_BLOCK_HDBT enable constraint FK_TARIFF_TYPE_PK21;
alter table MO_HD_METER_HDMFC enable constraint FK_TARIFF_TYPE_PK22;
alter table MO_SW_AREA_BAND enable constraint FK_TARIFF_TYPE_PK15;
alter table MO_SW_BAND_CHARGE enable constraint FK_TARIFF_TYPE_PK16;
alter table MO_SW_BLOCK_SWBT enable constraint FK_TARIFF_TYPE_PK17;
alter table MO_SW_METER_SWMFC enable constraint FK_TARIFF_TYPE_PK18;
alter table MO_US_METER_USPFC enable constraint FK_TARIFF_TYPE_PK14;
alter table MO_AS_METER_ASMFC enable constraint FK_TARIFF_TYPE_PK12;
alter table MO_AS_BAND_CHARGE enable constraint FK_TARIFF_TYPE_PK13;
alter table MO_MS_BLOCK_MSBT enable constraint FK_TARIFF_TYPE_PK10;
alter table MO_MS_METER_MSMFC enable constraint FK_TARIFF_TYPE_PK11;
alter table MO_UW_METER_UWPFC enable constraint FK_TARIFF_TYPE_PK09;
alter table MO_AW_METER_AWMFC enable constraint FK_TARIFF_TYPE_PK07;
alter table MO_AW_BAND_CHARGE enable constraint FK_TARIFF_TYPE_PK08;
alter table MO_MNPW_METER_MWMFC enable constraint FK_TARIFF_TYPE_PK04;
alter table MO_MNPW_BLOCK_MWBT enable constraint FK_TARIFF_TYPE_PK05;
alter table MO_MNPW_STANDBY_MWCAPCHG enable constraint FK_TARIFF_TYPE_PK06;
alter table MO_MPW_METER_MWMFC enable constraint FK_TARIFF_TYPE_PK01;
alter table MO_MPW_BLOCK_MWBT enable constraint FK_TARIFF_TYPE_PK02;
alter table MO_MPW_STANDBY_MWCAPCHG enable constraint FK_TARIFF_TYPE_PK03;
alter table MO_TARIFF_TYPE_MPW enable constraint FK_TARIFF_VERSION_PK_MPW;
alter table MO_TARIFF_TYPE_MNPW enable constraint FK_TARIFF_VERSION_PK_MNPW;
alter table MO_TARIFF_TYPE_AW enable constraint FK_TARIFF_VERSION_PK_AW;
alter table MO_TARIFF_TYPE_UW enable constraint FK_TARIFF_VERSION_PK_UW;
alter table MO_TARIFF_TYPE_MS enable constraint FK_TARIFF_VERSION_PK_MS;
alter table MO_TARIFF_TYPE_AS enable constraint FK_TARIFF_VERSION_PK_AS;
alter table MO_TARIFF_TYPE_US enable constraint FK_TARIFF_VERSION_PK_US;
alter table MO_TARIFF_TYPE_SW enable constraint FK_TARIFF_VERSION_PK_SW;
alter table MO_TARIFF_TYPE_HD enable constraint FK_TARIFF_VERSION_PK_HD;
alter table MO_TARIFF_TYPE_TE enable constraint FK_TARIFF_VERSION_PK_TE;
alter table MO_SERVICE_COMPONENT enable constraint FK_TARIFFCODE_PK;
alter table MO_TARIFF_VERSION enable constraint FK_MO_TARIFF_CODE;
alter table MO_PROPERTY_ADDRESS enable constraint FK_ADDRESS_PK02;
alter table MO_CUST_ADDRESS enable constraint FK_ADDRESS_PK01;
alter table MO_METER_ADDRESS enable constraint FK_ADDRESS_PK03;
alter table MO_METER_NETWORK enable constraint FK_METER_MANUFACT01;
alter table MO_METER_NETWORK enable constraint FK_METER_MANUFACT02;
alter table MO_METER_SPID_ASSOC enable constraint FK_METER_MANUFACT05;
alter table MO_METER_DPIDXREF enable constraint FK_METER_READING_MANUFACT01;
alter table MO_METER_READING enable constraint FK_METER_READING_MANUFACT02;
alter table MO_DISCHARGED_VOLUME enable constraint FK_DPID_PK03;
alter table MO_SERVICE_COMPONENT enable constraint FK_DPID_PK01;
alter table MO_METER_DPIDXREF enable constraint FK_DPID_PK05;
alter table MO_DISCHARGE_POINT_VOLMET_ADJ enable constraint FK_DPID_PK04;
alter table MO_CALCULATED_DISCHARGE enable constraint FK_DPID_PK02;
alter table MO_SERVICE_COMPONENT_VOL_ADJ enable constraint FK_SERVICECOMPONENTREF_PK02;
alter table MO_SUPPLY_POINT enable constraint FK_WHOLESALERID_PK;
alter table MO_SUPPLY_POINT enable constraint FK_RETAILERID_PK02;
alter table MO_SERVICE_COMPONENT enable constraint FK_SPID_PK02;
alter table MO_DISCHARGE_POINT enable constraint FK_SPID_PK;
alter table MO_SUPPLY_POINT enable constraint FK_STWPROPERTYNUMBER_PK02;
alter table MO_SUPPLY_POINT enable constraint FK_CORESPID_PK;
alter table MO_CUSTOMER enable constraint FK_CUST_PROP;
alter table MO_PROPERTY_ADDRESS enable constraint FK_STWPROPERTYNUMBER_PK03;
alter table MO_METER_ADDRESS enable constraint FK_METER_ADDRESS_MAN_COMP;
alter table MO_METER enable constraint FK_SPID_PK03;
commit;


--Load up data into MO_ORG, commented out. MO_ORG table is not being truncated
--/oravwa/11.2.0.4/bin/sqlldr MOUTRAN/DOWD /recload/SQLLDR_FILES/CONTROL_FILES/MO_ORG.ctl

-- Execute the TARIFF procedure
set serveroutput on;
execute P_MIG_TARIFF.P_MOU_TRAN_TARIFF_RUN();
set serveroutput off;


-- Execute the main batch
set serveroutput on;
exec  P_MIG_BATCH.P_STARTBATCH();
set serveroutput off;
exit;
