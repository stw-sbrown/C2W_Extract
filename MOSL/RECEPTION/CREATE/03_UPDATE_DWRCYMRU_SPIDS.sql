--
-- Updated DWRCYMRU SPIDs to SPIDs from designated STW SPID range
--
-- Subversion $Revision: 6363 $	
--
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      17/10/2016  K.Burton   Intial version
-- V 0.02      25/10/2016  K.Burton   Update NOSPID SPIDs from OWCs to STW SPIDs
-- V 0.03      26/10/2016  S.Badhan   Update OWC landlord spids from look up table
-- V 0.04      07/11/2016  K.Burton   Added update to OWC_SUPPLY_POINT.STWPROPERTYNUMBER
--                                    For TE mapped properties
-- V 0.05      09/11/2016  K.Burton   Update OWC customer classification from LU_OWC_NOT_SENSITIVE table
-- V 0.06      21/11/2016  K.Burton   SAP Defect 387 - Update crossborder meter manufacturers for SAP
--                                    Assign SAPFLOCNUMBERS to unmapped OWC SPIDs
----------------------------------------------------------------------------------------

UPDATE OWC_SUPPLY_POINT OSP
SET OSP.LANDLORDSPID = NVL((SELECT LANDLORDSPID FROM MOUTRAN.LU_SS_LANDLORD WHERE SPID = OSP.SPID_PK), OSP.LANDLORDSPID);

UPDATE OWC_SERVICE_COMPONENT OSC
SET OSC.SPID_PK = NVL((SELECT SPID_PK FROM MOUTRAN.LU_SPID_RANGE_DWRCYMRU WHERE DWRCYMRU_SPID = OSC.SPID_PK),OSC.SPID_PK)
WHERE OWC = 'DWRCYMRU-W'
AND EXISTS (SELECT 1 FROM OWC_SUPPLY_POINT WHERE OWC = 'DWRCYMRU-W' AND SPID_PK = OSC.SPID_PK);

UPDATE OWC_METER_SUPPLY_POINT OMSP
SET OMSP.SPID_PK = NVL((SELECT SPID_PK FROM MOUTRAN.LU_SPID_RANGE_DWRCYMRU WHERE DWRCYMRU_SPID = OMSP.SPID_PK),OMSP.SPID_PK)
WHERE OWC = 'DWRCYMRU-W'
AND EXISTS (SELECT 1 FROM OWC_SUPPLY_POINT WHERE OWC = 'DWRCYMRU-W' AND SPID_PK = OMSP.SPID_PK);

UPDATE OWC_SUPPLY_POINT OSP
SET OSP.SPID_PK = NVL((SELECT SPID_PK FROM MOUTRAN.LU_SPID_RANGE_DWRCYMRU WHERE DWRCYMRU_SPID = OSP.SPID_PK),OSP.SPID_PK)
WHERE OWC = 'DWRCYMRU-W';

UPDATE OWC_METER OM
SET OM.SPID = (SELECT DISTINCT SPID_PK FROM OWC_METER_SUPPLY_POINT WHERE MANUFACTURER_PK = OM.MANUFACTURER_PK AND MANUFACTURERSERIALNUM_PK = OM.MANUFACTURERSERIALNUM_PK)
WHERE OWC = 'DWRCYMRU-W';

-- Update NOSPID SPIDs from OWCs to STW SPIDs
UPDATE OWC_SERVICE_COMPONENT OSC
SET OSC.SPID_PK = NVL((SELECT SPID_PK FROM MOUTRAN.LU_SPID_RANGE_NOSPID WHERE NOSPID_SPID = OSC.SPID_PK),OSC.SPID_PK)
WHERE EXISTS (SELECT 1 FROM OWC_SUPPLY_POINT WHERE PAIRINGREFREASONCODE = 'NOSPID' AND SPID_PK = OSC.SPID_PK);

UPDATE OWC_SUPPLY_POINT OSP
SET OSP.SPID_PK = NVL((SELECT SPID_PK FROM MOUTRAN.LU_SPID_RANGE_NOSPID WHERE NOSPID_SPID = OSP.SPID_PK),OSP.SPID_PK)
WHERE PAIRINGREFREASONCODE = 'NOSPID';

-- Update STW Properties from LU_OWC_TE_METERS table
UPDATE OWC_SUPPLY_POINT OSP
SET OSP.STWPROPERTYNUMBER = (SELECT DISTINCT STW_PROPERTYNUMBER FROM MOUTRAN.LU_OWC_TE_METERS WHERE OWC_SPID = OSP.SPID_PK AND ROWNUM = 1)
WHERE EXISTS (SELECT 1 FROM MOUTRAN.LU_OWC_TE_METERS WHERE OWC_SPID = OSP.SPID_PK);
--WHERE OWC = 'SOUTHSTAFF-W'
--AND EXISTS (SELECT 1 FROM MOUTRAN.LU_OWC_TE_METERS WHERE OWC_SPID = OSP.SPID_PK);

-- Updates OWC_SUPPLY_POINT to set the SAPFLOCNUMBER and STWCUSTOMERNUMBER for the TE SPIDs
-- to the values from the duplicate SPIDs as "enriched" by Harj/Graham
UPDATE OWC_SUPPLY_POINT SP
SET SAPFLOCNUMBER = (SELECT SAPFLOCNUMBER 
                     FROM (WITH DUPS AS (SELECT COUNT(*),STWPROPERTYNUMBER 
                                          FROM OWC_SUPPLY_POINT
                                          WHERE STWPROPERTYNUMBER IS NOT NULL
                                          GROUP BY STWPROPERTYNUMBER
                                          HAVING COUNT(*) > 1)
                            SELECT OSP.SPID_PK, 
                                   OSP.STWPROPERTYNUMBER, 
                                   DECODE(NVL(LU.OWC_SPID,'N'),'N','N','Y') TE_SPID,
                                   OSP.SAPFLOCNUMBER,
                                   OSP.STWCUSTOMERNUMBER,
                                   OSP.OWC
                            FROM OWC_SUPPLY_POINT OSP, DUPS, MOUTRAN.LU_OWC_TE_METERS LU
                            WHERE OSP.STWPROPERTYNUMBER = DUPS.STWPROPERTYNUMBER
                            AND OSP.SPID_PK = LU.OWC_SPID(+)
                            ORDER BY OSP.STWPROPERTYNUMBER)
                     WHERE STWPROPERTYNUMBER = SP.STWPROPERTYNUMBER
                     AND TE_SPID = 'N'),                     
    STWCUSTOMERNUMBER = (SELECT STWCUSTOMERNUMBER 
                     FROM (WITH DUPS AS (SELECT COUNT(*),STWPROPERTYNUMBER 
                                          FROM OWC_SUPPLY_POINT
                                          WHERE STWPROPERTYNUMBER IS NOT NULL
                                          GROUP BY STWPROPERTYNUMBER
                                          HAVING COUNT(*) > 1)
                            SELECT OSP.SPID_PK, 
                                   OSP.STWPROPERTYNUMBER, 
                                   DECODE(NVL(LU.OWC_SPID,'N'),'N','N','Y') TE_SPID,
                                   OSP.SAPFLOCNUMBER,
                                   OSP.STWCUSTOMERNUMBER,
                                   OSP.OWC
                            FROM OWC_SUPPLY_POINT OSP, DUPS, MOUTRAN.LU_OWC_TE_METERS LU
                            WHERE OSP.STWPROPERTYNUMBER = DUPS.STWPROPERTYNUMBER
                            AND OSP.SPID_PK = LU.OWC_SPID(+)
                            ORDER BY OSP.STWPROPERTYNUMBER)
                     WHERE STWPROPERTYNUMBER = SP.STWPROPERTYNUMBER
                     AND TE_SPID = 'N')                          
WHERE EXISTS (SELECT SAPFLOCNUMBER 
           FROM (WITH DUPS AS (SELECT COUNT(*),STWPROPERTYNUMBER 
                                FROM OWC_SUPPLY_POINT
                                WHERE STWPROPERTYNUMBER IS NOT NULL
                                GROUP BY STWPROPERTYNUMBER
                                HAVING COUNT(*) > 1)
                  SELECT OSP.SPID_PK, 
                         OSP.STWPROPERTYNUMBER, 
                         DECODE(NVL(LU.OWC_SPID,'N'),'N','N','Y') TE_SPID,
                         OSP.SAPFLOCNUMBER,
                         OSP.STWCUSTOMERNUMBER,
                         OSP.OWC
                  FROM OWC_SUPPLY_POINT OSP, DUPS, MOUTRAN.LU_OWC_TE_METERS LU
                  WHERE OSP.STWPROPERTYNUMBER = DUPS.STWPROPERTYNUMBER
                  AND OSP.SPID_PK = LU.OWC_SPID(+)
                  ORDER BY OSP.STWPROPERTYNUMBER)
           WHERE STWPROPERTYNUMBER = SP.STWPROPERTYNUMBER
           AND TE_SPID = 'Y');
           
-- Updates OWC_SUPPLY_POINT to set the STWPROPERTYNUMBER, SAPFLOCNUMBER and STWCUSTOMERNUMBER for the Non-TE SPIDs
-- to NULL so they will get processed as non-mapped SPIDs by OWC proc
UPDATE OWC_SUPPLY_POINT
SET STWPROPERTYNUMBER = NULL, SAPFLOCNUMBER = NULL, STWCUSTOMERNUMBER = NULL
WHERE SPID_PK IN (SELECT SPID_PK 
                  FROM (WITH DUPS AS (SELECT COUNT(*),STWPROPERTYNUMBER 
                                FROM OWC_SUPPLY_POINT
                                WHERE STWPROPERTYNUMBER IS NOT NULL
                                GROUP BY STWPROPERTYNUMBER
                                HAVING COUNT(*) > 1)
                        SELECT OSP.SPID_PK, 
                               OSP.STWPROPERTYNUMBER, 
                               DECODE(NVL(LU.OWC_SPID,'N'),'N','N','Y') TE_SPID,
                               OSP.SAPFLOCNUMBER,
                               OSP.STWCUSTOMERNUMBER,
                               OSP.OWC
                        FROM OWC_SUPPLY_POINT OSP, DUPS, MOUTRAN.LU_OWC_TE_METERS LU
                        WHERE OSP.STWPROPERTYNUMBER = DUPS.STWPROPERTYNUMBER
                        AND OSP.SPID_PK = LU.OWC_SPID(+)
                        ORDER BY OSP.STWPROPERTYNUMBER)
                  WHERE TE_SPID = 'N');
                  
UPDATE OWC_SUPPLY_POINT 
SET STWPROPERTYNUMBER = NULL, STWCUSTOMERNUMBER = NULL, SAPFLOCNUMBER = NULL
WHERE SPID_PK IN ('300540255XS17','3005484637S18','3005035115S15','3010212313S12','3005032094S19','301021104XS17','3008301126S11','3008251498S14');                  

-- Update customer classification from LU_OWC_NOT_SENSITIVE table
UPDATE OWC_SUPPLY_POINT OSP
SET OSP.CUSTOMERCLASSIFICATION = 'NA' 
WHERE OSP.CUSTOMERCLASSIFICATION = 'SEMDV'
AND EXISTS (SELECT 1 FROM MOUTRAN.LU_OWC_NOT_SENSITIVE WHERE OWC_SPID = OSP.SPID_PK);

UPDATE OWC_SUPPLY_POINT
SET CUSTOMERCLASSIFICATION = 'NA'
WHERE CUSTOMERCLASSIFICATION = 'SEMDV'
AND STWPROPERTYNUMBER IS NULL;

-- Update crossborder meter manufacturers for SAP
UPDATE OWC_METER OWC
SET MANUFACTURER_PK = (SELECT MANUFCODE FROM MOUTRAN.LU_METER_MANUFACTURER WHERE MANUFACTURER_PK = UPPER(OWC.MANUFACTURER_PK));

UPDATE OWC_METER_SUPPLY_POINT OWC
SET MANUFACTURER_PK = (SELECT MANUFCODE FROM MOUTRAN.LU_METER_MANUFACTURER WHERE MANUFACTURER_PK = UPPER(OWC.MANUFACTURER_PK));

UPDATE OWC_METER_READING OWC
SET MANUFACTURER_PK = (SELECT MANUFCODE FROM MOUTRAN.LU_METER_MANUFACTURER WHERE MANUFACTURER_PK = UPPER(OWC.MANUFACTURER_PK));

-- Assign SAPFLOCNUMBERS to unmapped OWC SPIDs
UPDATE OWC_SUPPLY_POINT OSP
SET SAPFLOCNUMBER = (SELECT SAPFLOCNUMBER FROM MOUTRAN.LU_OWC_SAP_FLOCA WHERE SPID_PK = OSP.SPID_PK)
WHERE STWPROPERTYNUMBER IS NULL
AND SAPFLOCNUMBER IS NULL
AND EXISTS (SELECT 1 FROM MOUTRAN.LU_OWC_SAP_FLOCA WHERE SPID_PK = OSP.SPID_PK);

UPDATE OWC_SUPPLY_POINT
SET STWPROPERTYNUMBER = 958002013,SAPFLOCNUMBER = 4423594
WHERE SPID_PK = '300540255XS17';

UPDATE OWC_SUPPLY_POINT
SET STWPROPERTYNUMBER = 529002016,SAPFLOCNUMBER = 1735967
WHERE SPID_PK = '3005484637S18';

COMMIT;
show errors;
exit;