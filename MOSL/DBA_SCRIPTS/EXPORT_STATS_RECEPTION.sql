--*** SCRIPT TO GENERATE TABLE STAT SCRIPTS
--SELECT 'EXEC DBMS_STATS.EXPORT_TABLE_STATS(''RECEPTION'',''' || table_name || ''',NULL,''RECSTATSTABLE''); '
--from dba_tables where owner = 'RECEPTION' and table_name NOT LIKE '%$%' AND status = 'VALID'
--AND table_name <> 'RECSTATSTABLE'
--ORDER BY 1;

----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      02/11/2016  S.Badhan   Remove 'FINTRAN' reception tables
----------------------------------------------------------------------------------------

--1. **** DROP PREVIOUS STATS TABLE
DROP TABLE RECEPTION.RECSTATSTABLE PURGE;

--2. **** CREATE STATS TABLE
EXEC DBMS_STATS.CREATE_STAT_TABLE('RECEPTION','RECSTATSTABLE');

--3. **** EXPORT STATS
EXEC DBMS_STATS.EXPORT_SCHEMA_STATS('RECEPTION','RECSTATSTABLE');

EXEC DBMS_STATS.EXPORT_TABLE_STATS('RECEPTION','TVMNHHDTL',NULL,'RECSTATSTABLE'); 

commit;
exit;