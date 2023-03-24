---------------------------------------------------
-- D.Cheung 	27/07/2016
-- Execute update statistics on CIS
--
-- Subversion $Revision: 5059 $
--
--

EXEC dbms_stats.gather_schema_stats('CIS', cascade=>TRUE);
commit;
exit;