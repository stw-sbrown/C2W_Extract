---------------------------------------------------
-- M.Marron 30/06/2016
-- Execute update statistics on SAPDEL
--
-- Subversion $Revision: 5108 $
--
--

EXEC dbms_stats.gather_schema_stats('SAPDEL', cascade=>TRUE);
PURGE RECYCLEBIN;
commit;
exit;
