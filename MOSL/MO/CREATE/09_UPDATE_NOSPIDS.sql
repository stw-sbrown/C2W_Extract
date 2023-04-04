--
-- Updates NOSPIDs causing errors in MOSL upload
--
-- Subversion $Revision: 6338 $	
--
---------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.01      22/11/2016  K.Burton   Intial version
----------------------------------------------------------------------------------------
-- updates the 10 TE spids
UPDATE MO_SUPPLY_POINT
SET PAIRINGREFREASONCODE = NULL, OTHERWHOLESALERID = 'SEVERN-W'
WHERE SPID_PK IN (SELECT SPID_PK FROM MO_DISCHARGE_POINT
                  WHERE SPID_PK IN (SELECT MSP.SPID_PK
                                    FROM MO_SUPPLY_POINT MSP, 
                                         LU_NOSPID_EXCEPTIONS LU,
                                         CIS.ELIGIBILITY_CONTROL_TABLE ECT,
                                         LU_OWC_TE_METERS TE
                                    WHERE MSP.SPID_PK = LU.SPID_PK
                                    AND MSP.CORESPID_PK = ECT.CORESPID
                                    AND MSP.PAIRINGREFREASONCODE = 'NOSPID'
                                    AND ECT.ID_OWC = 'STW'
                                    AND TE.OWC IS NULL
                                    AND MSP.SPID_PK = TE.OWC_SPID(+)));

-- updates the 19 TE spids
UPDATE MO_SUPPLY_POINT
SET PAIRINGREFREASONCODE = NULL, OTHERWHOLESALERID = 'SOUTHSTAFF-W'
WHERE SPID_PK IN (SELECT SPID_PK FROM RECEPTION.OWC_SUPPLY_POINT
                  WHERE SPID_PK IN (SELECT MSP.SPID_PK
                                    FROM MO_SUPPLY_POINT MSP, 
                                         LU_NOSPID_EXCEPTIONS LU,
                                         CIS.ELIGIBILITY_CONTROL_TABLE ECT,
                                         LU_OWC_TE_METERS TE
                                    WHERE MSP.SPID_PK = LU.SPID_PK
                                    AND MSP.CORESPID_PK = ECT.CORESPID
                                    AND MSP.PAIRINGREFREASONCODE = 'NOSPID'
                                    AND ECT.ID_OWC = 'STW'
                                    AND TE.OWC IS NULL
                                    AND MSP.SPID_PK = TE.OWC_SPID(+)));
                                    
UPDATE MO_SUPPLY_POINT
SET PAIRINGREFREASONCODE = NULL, OTHERWHOLESALERID = 'ANGLIAN-W'
WHERE SPID_PK IN (SELECT MSP.SPID_PK
                  FROM MO_SUPPLY_POINT MSP, 
                       LU_NOSPID_EXCEPTIONS LU,
                       CIS.ELIGIBILITY_CONTROL_TABLE ECT,
                       LU_OWC_TE_METERS TE
                  WHERE MSP.SPID_PK = LU.SPID_PK
                  AND MSP.CORESPID_PK = ECT.CORESPID
                  AND MSP.PAIRINGREFREASONCODE = 'NOSPID'
                  AND ECT.ID_OWC = 'ANGLIAN-W'
                  AND MSP.SPID_PK = TE.OWC_SPID(+)); 

commit;
/
exit;