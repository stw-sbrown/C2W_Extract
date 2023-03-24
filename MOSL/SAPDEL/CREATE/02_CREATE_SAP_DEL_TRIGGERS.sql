------------------------------------------------------------------------------
-- TASK		    		    : 	SAP DEL TRIGGERS
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	02_CREATE_SAP_DEL_TRIGGERS.sql
--
--
-- Subversion $Revision: 4645 $	
--
-- CREATED        		: 	14/06/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing triggers that are created for the SAP DEL database
--
-- NOTES  			      :	Place a summary at the top of the file with more detailed information 
--					            where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS :	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	  Date            Author         	Description
-- ---------      ----------      -------         ----------------------------------------------------------
-- V0.01          14/06/2016      D.Cheung        1. Add trigger DEL_METER_INSTALL_TRG
-- V0.02          30/06/2016      K.Burton        1. Add trigger SPA_DEL_DEV_TRG
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_INSTALL_TRG
  BEFORE INSERT OR UPDATE ON SAP_DEL_METER_INSTALL
  FOR EACH ROW
DECLARE
--local variables
    c_module_name                CONSTANT VARCHAR2(30) := 'DEL_METER_INSTALL_TRG';  -- modify
    l_dvlcount                   NUMBER;
    l_scmcount                   NUMBER;
    l_scmtecount                 NUMBER;

BEGIN
    --INITIALISE VARIABLES;
    l_dvlcount := 0;
    l_scmcount := 0;
    l_scmtecount := 0;

    --RULE: Foreign key on SAP_DEL_DVLCRT when DVLLEGACYRECNUM is not NULL
    BEGIN
        SELECT COUNT(*)
        INTO   l_dvlcount
        FROM   SAP_DEL_DVLCRT SDD
        WHERE  :NEW.DVLLEGACYRECNUM = SDD.LEGACYRECNUM;
    END;

    IF (:NEW.DVLLEGACYRECNUM IS NOT NULL AND l_dvlcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'Foreign Key violation - DVLCRT key not found in SAP_DEL_DVLCRT');
    END IF;
    
    --RULE: Foreign key on SAP_DEL_SCM and SAP_DEL_SCMTE
    BEGIN
        SELECT COUNT(*)
        INTO   l_scmcount
        FROM   SAP_DEL_SCM SDS
        WHERE  :NEW.SCMLEGACYRECNUM = SDS.LEGACYRECNUM;
    END;
    
    BEGIN
        SELECT COUNT(*)
        INTO   l_scmtecount
        FROM   SAP_DEL_SCMTE SDST
        WHERE  :NEW.SCMLEGACYRECNUM = SDST.LEGACYRECNUM;
    END;

    IF (:NEW.SCMLEGACYRECNUM IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20001, 'NULL value violation - SCMLEGACYRECNUM');
    ELSIF (l_scmcount = 0 AND l_scmtecount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'Foreign Key violation - SCM key not found in SAP_DEL_SCM or SAP_DEL_SCMTE');
    END IF;

END DEL_METER_INSTALL_TRG;
/
ALTER TRIGGER SAPDEL.DEL_METER_INSTALL_TRG ENABLE;
/
CREATE OR REPLACE TRIGGER SAP_DEL_DEV_TRG 
  BEFORE INSERT OR UPDATE ON SAP_DEL_DEV
  FOR EACH ROW
DECLARE  
  l_gis_code VARCHAR(60);
BEGIN
  IF FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) = 'INVALID' THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;
  
  IF :NEW.GPSX IS NULL AND :NEW.GPSY IS NULL THEN
    l_gis_code := FN_VALIDATE_GIS(NULL);
  ELSE
    l_gis_code := FN_VALIDATE_GIS(:NEW.GPSX || ';' || :NEW.GPSY);
  END IF;
  IF l_gis_code LIKE 'Invalid%' THEN
    RAISE_APPLICATION_ERROR( -20099, l_gis_code);
  END IF;
END;
/
ALTER TRIGGER SAPDEL.SAP_DEL_DEV_TRG ENABLE;
/
show errors;
exit;