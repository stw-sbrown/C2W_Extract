--
-- Update statistics on FINTRAN
--
-- Subversion $Revision: 5193 $	
--
----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      15/08/2016  S.Badhan   Intial Version.
----------------------------------------------------------------------------------------

EXEC dbms_stats.gather_schema_stats('FINTRAN', CASCADE=>TRUE);
PURGE RECYCLEBIN;
commit;
exit;

