---------------------------------------------------
-- N.Henderson 	25/04/2016
-- Execute update statistics on MOUDEL/DOWS
--
-- Subversion $Revision: 5194 $
--
--

EXEC dbms_stats.gather_schema_stats('MOUDEL', cascade=>TRUE);
PURGE RECYCLEBIN;
commit;
exit;
