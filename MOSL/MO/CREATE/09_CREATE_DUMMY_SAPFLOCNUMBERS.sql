--
-- Creates temporary dummy 5 digit SAPFLOCNUMBERs in LU_SAP_FLOC lookup
-- for unallocated OWC properties
--
-- Subversion $Revision: 6023 $	
--
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      31/10/2016  K.Burton   Intial version
----------------------------------------------------------------------------------------

INSERT INTO LU_SAP_FLOCA
SELECT STWPROPERTYNUMBER_PK,
       ROWNUM + 9999 SAPFLOCNUMBER
FROM MO_ELIGIBLE_PREMISES 
WHERE SAPFLOCNUMBER IS NULL;

COMMIT;

UPDATE MO_ELIGIBLE_PREMISES MEP
SET MEP.SAPFLOCNUMBER = (SELECT SAPFLOCNUMBER FROM LU_SAP_FLOCA WHERE STWPROPERTYNUMBER_PK = MEP.STWPROPERTYNUMBER_PK)
WHERE MEP.SAPFLOCNUMBER IS NULL;

COMMIT;
show errors;
exit;