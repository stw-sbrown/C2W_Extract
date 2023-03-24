--
-- Update statistics on FINDEL
--
-- Subversion $Revision: 5193 $	
--
----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      15/08/2016  S.Badhan   Intial Version.
----------------------------------------------------------------------------------------

EXEC dbms_stats.gather_schema_stats('FINDEL', CASCADE=>TRUE);
PURGE RECYCLEBIN;

commit;
exit;
