--*** SCRIPT TO GENERATE TABLE STAT SCRIPTS
--SELECT 'EXEC DBMS_STATS.EXPORT_TABLE_STATS(''FINTRAN'',''' || table_name || ''',NULL,''TRANSTATSTABLE''); '
--from dba_tables where owner = 'FINTRAN' and table_name NOT LIKE '%$%' AND status = 'VALID'
--AND table_name <> 'TRANSTATSTABLE'
--ORDER BY 1;

--1. **** DROP PREVIOUS STATS TABLE
DROP TABLE FINTRAN.TRANSTATSTABLE PURGE;

--2. **** CREATE STATS TABLE
EXEC DBMS_STATS.CREATE_STAT_TABLE('FINTRAN','TRANSTATSTABLE');

--3. **** EXPORT STATS
EXEC DBMS_STATS.EXPORT_SCHEMA_STATS('FINTRAN','TRANSTATSTABLE');

EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_CONSTRUCTION_SITE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_CROSSBORDER',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_DATALOGGERS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_DISCHARGE_VOL_LIMITS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_LANDLORD',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_METER_MANUFACTURER',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_OUTREADER_PROTOCOLS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_PUBHEALTHRESITE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_SAP_EQUIPMENT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_SAP_FLOCA',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_SERVICE_CATEGORY',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_SPID_OWC_RETAILER',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_SPID_RANGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_TARIFF_SPECIAL_AGREEMENTS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_TE_BILLING_CYCLE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','LU_TE_REFDESC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_BATCHSTATUS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_CPLOG',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_CPREF',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_ERRORLOG',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_ERRREF',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_JOBREF',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MIG_JOBSTATUS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_ADDRESS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_AS_BAND_CHARGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_AS_METER_ASMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_AW_BAND_CHARGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_AW_METER_AWMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_CALCULATED_DISCHARGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_CUSTOMER',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_CUST_ADDRESS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_DISCHARGED_VOLUME',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_DISCHARGE_POINT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_DISCHARGE_POINT_VOLMET_ADJ',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_ELIGIBLE_PREMISES',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_HD_AREA_BAND',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_HD_BAND_CHARGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_HD_BLOCK_HDBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_HD_METER_HDMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_METER',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_METER_ADDRESS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_METER_DPIDXREF',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_METER_NETWORK',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_METER_READING',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_METER_SPID_ASSOC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MNPW_BLOCK_MWBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MNPW_METER_MWMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MNPW_STANDBY_MWCAPCHG',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MPW_BLOCK_MWBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MPW_METER_MWMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MPW_STANDBY_MWCAPCHG',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MS_BLOCK_MSBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_MS_METER_MSMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_ORG',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_PROPERTY_ADDRESS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SERVICE_COMPONENT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SERVICE_COMPONENT_TYPE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SERVICE_COMPONENT_VOL_ADJ',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SUPPLY_POINT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SUPPLY_POINT_BACK',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SW_AREA_BAND',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SW_BAND_CHARGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SW_BLOCK_SWBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_SW_METER_SWMFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_STANDING_DATA',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_AS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_AW',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_HD',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_MNPW',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_MPW',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_MS',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_SCA',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_SW',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_TE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_US',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_UW',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_TYPE_WCA',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TARIFF_VERSION',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TE_BAND_CHARGE',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TE_BLOCK_BOBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_TE_BLOCK_ROBT',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_US_METER_USPFC',NULL,'TRANSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINTRAN','MO_UW_METER_UWPFC',NULL,'TRANSTATSTABLE'); 

commit;
exit;