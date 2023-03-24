---------------------------------------------------
-- N.Henderson 	25/04/2016
-- Execute update statistics on RECEPTION/DOWD
--
-- Subversion $Revision: 5194 $
--
--

EXEC dbms_stats.gather_schema_stats('RECEPTION', cascade=>TRUE);
PURGE RECYCLEBIN;
commit;
exit;