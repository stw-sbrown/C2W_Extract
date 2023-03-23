---------------------------------------------------
-- N.Henderson 	25/04/2016
-- Execute update statistics on MOUDEL/DOWS
--
-- Subversion $Revision: 4023 $
--
--

EXEC dbms_stats.gather_schema_stats('MOUDEL', cascade=>TRUE);
commit;
exit;
