--*** SCRIPT TO GENERATE TABLE STAT SCRIPTS
--SELECT 'EXEC DBMS_STATS.IMPORT_TABLE_STATS(''RECEPTION'',''' || table_name || ''',NULL,''RECSTATSTABLE''); '
--from dba_tables where owner = 'RECEPTION' and table_name NOT LIKE '%$%' AND status = 'VALID'
--AND table_name <> 'RECSTATSTABLE'
--ORDER BY 1;

----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      02/11/2016  S.Badhan   Remove 'FINTRAN' reception tables
----------------------------------------------------------------------------------------

--4. **** IMPORT PREVIOUS STATS
EXEC DBMS_STATS.IMPORT_SCHEMA_STATS('RECEPTION','RECSTATSTABLE');

EXEC DBMS_STATS.IMPORT_TABLE_STATS('RECEPTION','TVMNHHDTL',NULL,'RECSTATSTABLE'); 
PURGE RECYCLEBIN;
commit;
exit;