
-- ECT_Create_Indexes.sql
-- Creates Indexes on Eligibility Control tables      
-- Date - 18/04/2016
-- Written By - Surinder Badhan
-- 

----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 1.00      29/02/2016  S.Badhan   Intial
----------------------------------------------------------------------------------------


CREATE INDEX CIS.ECT_INDEX3 ON CIS.ELIGIBILITY_CONTROL_TABLE (CD_COMPANY_SYSTEM ASC, NO_PROPERTY ASC, CD_PROPERTY_USE_FUT ASC, VALIDATED_FLAG ASC, CORESPID ASC); 

COMMIT;

exit;




