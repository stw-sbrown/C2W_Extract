--*** SCRIPT TO GENERATE TABLE STAT SCRIPTS
--SELECT 'EXEC DBMS_STATS.EXPORT_TABLE_STATS(''FINDEL'',''' || table_name || ''',NULL,''DELSTATSTABLE''); '
--from dba_tables where owner = 'FINDEL' and table_name NOT LIKE '%$%' AND status = 'VALID'
--AND table_name <> 'DELSTATSTABLE'
--ORDER BY 1;

--1. **** DROP PREVIOUS STATS TABLE
DROP TABLE FINDEL.DELSTATSTABLE PURGE;

--2. **** CREATE STATS TABLE
EXEC DBMS_STATS.CREATE_STAT_TABLE('FINDEL','DELSTATSTABLE');

--3. **** EXPORT STATS
EXEC DBMS_STATS.EXPORT_SCHEMA_STATS('FINDEL','DELSTATSTABLE');

EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','BT_CROSSBORDER_CTRL',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_CALCULATED_DISCHARGE',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_DISCHARGE_POINT',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_METER',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_METER_DISCHARGE_POINT',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_METER_NETWORK',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_METER_READING',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_METER_SUPPLY_POINT',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_SERVICE_COMPONENT',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','DEL_SUPPLY_POINT',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_BATCHSTATUS',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_CPLOG',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_CPREF',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_ERRORLOG',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_ERRREF',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_JOBREF',NULL,'DELSTATSTABLE'); 
EXEC DBMS_STATS.EXPORT_TABLE_STATS('FINDEL','MIG_JOBSTATUS',NULL,'DELSTATSTABLE'); 

commit;
exit;