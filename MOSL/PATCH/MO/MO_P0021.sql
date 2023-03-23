
-- Meter_Create_Index.sql
--
-- Subversion $Revision: 4023 $	
--
-- Creates Indexes on MO_CUSTOMER  
-- Date - 20/04/2016
-- Written By - Surinder Badhan
-- 

----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.00      20/04/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------


CREATE INDEX CUSTOMER_INDEX2 ON MO_CUSTOMER (STWPROPERTYNUMBER_PK); 

COMMIT;
exit;

