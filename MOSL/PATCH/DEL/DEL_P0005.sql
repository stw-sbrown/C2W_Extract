------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	DEL_P0005.sql
--
--
-- Subversion $Revision: 5458 $	
--
-- CREATED        		: 	27/04/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL DEL database
--
-- NOTES  			      :	Place a summary at the top of the file with more detailed information 
--					            where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	DEL_P0005.sql
-- ASSOCIATED SCRIPTS :	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	  Date            Author         	Description
-- ---------      ----------      -------         ----------------------------------------------------------
-- V0.01          27/04/2016      D.Cheung        1. Update trigger  DEL_METER_SUPPLY_POINT_TRG
--                                                   removed TRIMs from the query check
--                                                2. Update trigger DEL_METER_NETWORK_TRG
--                                                   removed TRIMs from the query check
--                                                3. Modified field MAINMETERTREATMENT to NULLABLE on DEL_METER_NETWORK
-- V0.02          13/07/2016      D.Cheung        I-285 - Missing rows on output file SPID not in SUPPLY_POINT
-- V0.03          27/07/2016      K.Burton        Amended triggers to take into account TE meters which were causing
--                                                meters to be dropped from DEL tables
-- V0.04          15/08/2016      S.Badhan        I-320. Remove schema name from trigger.
-- V0.06          16/08/2016      S.Badhan        I-320. DEL_METER_SUPPLY_POINT_TRG amemded to compile.
-- V0.07          13/09/2016      D.Cheung        I-356 - Remove 'MS' constraint from PRIVATEWATER check
------------------------------------------------------------------------------------------------------------

create or replace
TRIGGER DEL_METER_SUPPLY_POINT_TRG
    BEFORE
        INSERT
    ON DEL_METER_SUPPLY_POINT
    FOR EACH ROW
DECLARE
--local variables
    c_module_name                 CONSTANT VARCHAR2(30) := 'DEL_METER_SUPPLY_POINT_TRG';  -- modify
    l_count                       NUMBER;
    l_meter_treatment             VARCHAR2(20);
    l_service_category            VARCHAR2(1);
BEGIN
  BEGIN
    SELECT METERTREATMENT
    INTO l_meter_treatment
    FROM DEL_METER
    WHERE MANUFACTURER_PK = :NEW.MANUFACTURER_PK
    AND MANUFACTURERSERIALNUM_PK = :NEW.MANUFACTURERSERIALNUM_PK;
  EXCEPTION
    WHEN no_data_found THEN
      RAISE_APPLICATION_ERROR( -20001, 'METERTREATMENT not found for MANUFACTURER_PK and MANUFACTURERSERIALNUM_PK');
  END;

  BEGIN
    SELECT SERVICECATEGORY
    INTO l_service_category
    FROM DEL_SUPPLY_POINT
    WHERE SPID_PK = :NEW.SPID_PK;
  EXCEPTION
    WHEN no_data_found THEN
      RAISE_APPLICATION_ERROR( -20001, 'SERVICECATEGORY not found for SPID');
  END;

  IF l_meter_treatment = 'POTABLE' THEN
    IF l_service_category = 'W' THEN
      SELECT COUNT(SERVICECOMPONENTTYPE)
      INTO l_count
      FROM MOUTRAN.MO_SERVICE_COMPONENT
      WHERE SPID_PK = :NEW.SPID_PK
      AND SERVICECOMPONENTTYPE = 'MPW';

      IF l_count = 0 THEN
        RAISE_APPLICATION_ERROR( -20001, 'SERVICECOMPONENTTYPE is incompatible with METERTREATMENT ' || l_meter_treatment || ' for SPID ');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'SERVICECATEGORY is incompatible with METERTREATMENT ' || l_meter_treatment);
    END IF;
  ELSIF l_meter_treatment = 'NONPOTABLE' THEN
    IF l_service_category = 'W' THEN
      SELECT COUNT(SERVICECOMPONENTTYPE)
      INTO l_count
      FROM MOUTRAN.MO_SERVICE_COMPONENT
      WHERE SPID_PK = :NEW.SPID_PK
      AND SERVICECOMPONENTTYPE = 'MNPW';

      IF l_count = 0 THEN
        RAISE_APPLICATION_ERROR( -20001, 'SERVICECOMPONENTTYPE is incompatible with METERTREATMENT ' || l_meter_treatment || ' for SPID ');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'SERVICECATEGORY is incompatible with METERTREATMENT ' || l_meter_treatment);
    END IF;
  ELSIF l_meter_treatment IN ('PRIVATEWATER','SEWERAGE') THEN
    IF l_service_category = 'S' THEN
      SELECT COUNT(SERVICECOMPONENTTYPE)
      INTO l_count
      FROM MOUTRAN.MO_SERVICE_COMPONENT
      WHERE SPID_PK = :NEW.SPID_PK
          AND SERVICECOMPONENTTYPE IN ('AS','US','SW','MS')
      ;

      IF l_count = 0 THEN
        RAISE_APPLICATION_ERROR( -20001, 'SERVICECOMPONENTTYPE is incompatible with METERTREATMENT ' || l_meter_treatment || ' for SPID ');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'SERVICECATEGORY is incompatible with METERTREATMENT ' || l_meter_treatment);
    END IF;
  ELSIF l_meter_treatment = 'PRIVATETE' THEN
    IF l_service_category = 'S' THEN
      SELECT COUNT(SERVICECOMPTYPE)
      INTO l_count
      FROM MOUTRAN.MO_DISCHARGE_POINT
      WHERE SPID_PK = :NEW.SPID_PK
      AND SERVICECOMPTYPE = 'TE';

      IF l_count = 0 THEN
        RAISE_APPLICATION_ERROR( -20001, 'SERVICECOMPONENTTYPE is incompatible with METERTREATMENT ' || l_meter_treatment || ' for SPID ');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'SERVICECATEGORY is incompatible with METERTREATMENT ' || l_meter_treatment);
    END IF;  ELSE
    RAISE_APPLICATION_ERROR( -20001, 'Invalid METERTREATMENT');
  END IF;
END DEL_METER_SUPPLY_POINT_TRG;
/
ALTER TRIGGER DEL_METER_SUPPLY_POINT_TRG ENABLE;
/

create or replace
TRIGGER DEL_METER_NETWORK_TRG
    BEFORE
        INSERT
    ON DEL_METER_NETWORK
    FOR EACH ROW

DECLARE
--local variables
    c_module_name                 CONSTANT VARCHAR2(30) := 'DEL_METER_NETWORK_TRG';  -- modify
    l_spidcount                   NUMBER;

BEGIN
    --INITIALISE VARIABLES;
    l_spidcount := 0;

    --RULE: Main Manufacturer and Serial Num combination must be already provided
    BEGIN
        SELECT COUNT(*)
        INTO   l_spidcount
        FROM   DEL_METER DM
--        JOIN DEL_METER_SUPPLY_POINT DS ON (DS.MANUFACTURER_PK = DM.MANUFACTURER_PK
--            AND DS.MANUFACTURERSERIALNUM_PK = DM.MANUFACTURERSERIALNUM_PK)
        WHERE  :NEW.MAINMANUFACTURER = DM.MANUFACTURER_PK
            AND :NEW.MAINMANUFACTURERSERIALNUM = DM.MANUFACTURERSERIALNUM_PK;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;

    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'MAIN Manufacturer and Serial Num combination does not exist in METER');
    END IF;

    --RULE: Sub Manufacturer and Serial Num combination must be already provided
    BEGIN
        SELECT COUNT(*)
        INTO   l_spidcount
        FROM   DEL_METER DM
--        JOIN DEL_METER_SUPPLY_POINT DS ON (DS.MANUFACTURER_PK = DM.MANUFACTURER_PK
--            AND DS.MANUFACTURERSERIALNUM_PK= DM.MANUFACTURERSERIALNUM_PK)
        WHERE  :NEW.SUBMANUFACTURER = DM.MANUFACTURER_PK
            AND :NEW.SUBMANUFACTURERSERIALNUM = DM.MANUFACTURERSERIALNUM_PK;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;

    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'SUB Manufacturer and Serial Num combination does not exist in METER');
    END IF;

    --RULE: SPID must be in DEL_SUPPLY_POINT unless non-market-meter
    IF :NEW.SPID_PK IS NOT NULL THEN
      BEGIN
          SELECT COUNT(*)
          INTO   l_spidcount
          FROM   DEL_SUPPLY_POINT DS
          WHERE  :NEW.SPID_PK = DS.SPID_PK;
      EXCEPTION
          WHEN NO_DATA_FOUND THEN
              l_spidcount := 0;
      END;
  
      IF (l_spidcount = 0) THEN
          RAISE_APPLICATION_ERROR( -20001, 'SPID missing from DEL_SUPPLY_POINT');
      END IF;
    END IF;

END DEL_METER_NETWORK_TRG;

/
ALTER TABLE DEL_METER_NETWORK MODIFY (MAINMETERTREATMENT NULL);
ALTER TABLE DEL_METER_NETWORK DROP CONSTRAINT CH01_MNSUBTREATMENT;
ALTER TRIGGER DEL_METER_NETWORK_TRG ENABLE;
/
show errors;
exit;