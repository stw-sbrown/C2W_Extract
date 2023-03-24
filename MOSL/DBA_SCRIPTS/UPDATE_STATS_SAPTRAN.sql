---------------------------------------------------
-- M.Marron 30/06/2016
-- Execute update statistics on SAPTRAN
--
-- Subversion $Revision: 5107 $
--
--

EXEC dbms_stats.gather_schema_stats('SAPTRAN', cascade=>TRUE);
PURGE RECYCLEBIN;
commit;
exit;
