---------------------------------------------------
-- N.Henderson 	25/04/2016
-- Execute update statistics on MOUTRAN/DOWS
--
-- Subversion $Revision: 5105 $
--
--

EXEC dbms_stats.gather_schema_stats('MOUTRAN', cascade=>TRUE);
PURGE RECYCLEBIN;
commit;
exit;

