--
-- Update statistics on RECEPTION FIN tables

--
-- Subversion $Revision: 5193 $	
--
----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      15/08/2016  S.Badhan   Intial Version.
----------------------------------------------------------------------------------------

--EXEC dbms_stats.gather_schema_stats('RECEPTION', CASCADE=>TRUE);
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_DISCHARGE_POINT');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_METER');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_METER_DISCHARGE_POINT');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_METER_NETWORK');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_METER_READING');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_METER_SUPPLY_POINT');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_SERVICE_COMPONENT');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_SUPPLY_POINT');
EXEC DBMS_STATS.gather_table_stats('RECEPTION', 'SAP_CALCULATED_DISCHARGE');

PURGE RECYCLEBIN;
/
commit;
exit;