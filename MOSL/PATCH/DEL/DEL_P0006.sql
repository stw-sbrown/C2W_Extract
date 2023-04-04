------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	DEL_P0006.sql
--
--
-- Subversion $Revision: 6093 $	
--
-- CREATED        		: 	05/05/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL DEL database
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
-- V0.01          05/05/2016      K.Burton        1. Update triggers  DEL_SUPPLY_POINT_TRG, DEL_METER_TRG and
--                                                   DEL_DISCHARGE_POINT_TRG
--                                                   - altered call to updated FN_VALIDATE_POSTCODE function
-- V0.02	  18/05/09        K.Burton        1. Added check for future dates to DEL_SUPPLY_POINT_TRG following
--                                                   MOSL file upload feedback 
--                                                2. Change to DEL_SERVICE_COMPONENT_TRG special agreement validation
--                                                   following MOSL file upload feedback
-- V0.03         15/06/2016      L.Smith          3. Transactional guidance v1.6 10-Jun-2016
--                                                   The Special Agreement Factor field must not be populated if the tariff code
--                                                   used relates to the service types Water Charge Adjustment or Sewerage
--                                                   Charge Adjustment, else it needs to be populated. 
--                                                   If there is no special agreement (i.e. the Special Agreement Flag = 0)
--                                                   then the default value 100 can be used for the Special Agreement Factor field.
-- V0.04          15/08/2016      S.Badhan        I-320. Remove schema name from trigger.
-- V0.05          03/11/2016      K.Burton     Added DEL_METER_DISCHARGE_TRG
------------------------------------------------------------------------------------------------------------

create or replace
TRIGGER DEL_SUPPLY_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_SUPPLY_POINT
  FOR EACH ROW
BEGIN
  
  IF FN_VALIDATE_POSTCODE(UPPER(:NEW.PREMPOSTCODE)) = 'INVALID' THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;

  IF FN_VALIDATE_POSTCODE(UPPER(:NEW.CUSTPOSTCODE)) = 'INVALID' THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;
  
  IF :NEW.SUPPLYPOINTEFFECTIVEFROMDATE > SYSDATE THEN
    RAISE_APPLICATION_ERROR(-20002,'Invalid Effective From Date. Date cannot be a future date');
  END IF;
END;

/
ALTER TRIGGER DEL_SUPPLY_POINT_TRG ENABLE;
/

create or replace
TRIGGER DEL_METER_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER
  FOR EACH ROW
DECLARE  
  l_gis_code VARCHAR(60);
BEGIN
  --INITIAL METER READ DATE SHOULD BE BEFORE MARKET OPERATION DA, I.E. LESS THAN TODAY DATE
  IF( :NEW.INITIALMETERREADDATE >= SYSDATE ) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Initial Meter Read Date: INITIALMETERREADDATE should be before the Market Operation day');
  END IF;

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
ALTER TRIGGER DEL_METER_TRG ENABLE;
/
create or replace
TRIGGER DEL_DISCHARGE_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_DISCHARGE_POINT
  FOR EACH ROW
BEGIN
  IF FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) = 'INVALID' THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.POSTCODE);
  END IF;
END;
/
ALTER TRIGGER DEL_DISCHARGE_POINT_TRG ENABLE;
/
create or replace
TRIGGER DEL_METER_READING_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER_READING
  FOR EACH ROW
DECLARE
  l_init_reading DATE;
  l_last_reading NUMBER(12,0);
BEGIN
  IF( :NEW.METERREADDATE >= SYSDATE ) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Meter Read Date: METEREADDATE cannot be a future date');
  END IF;

  BEGIN
    SELECT M.INITIALMETERREADDATE
    INTO l_init_reading
    FROM DEL_METER M
    WHERE M.MANUFACTURER_PK = :NEW.MANUFACTURER_PK
    AND M.MANUFACTURERSERIALNUM_PK = :NEW.MANUFACTURERSERIALNUM_PK;

    IF (l_init_reading > :NEW.METERREADDATE) THEN
    RAISE_APPLICATION_ERROR( -20003, 'Invalid Meter Read Date: METEREADDATE cannot be earlier than INITIALMETERREADDATE');
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR( -20002, 'Missing Initial Meter Read Date: INITIALMETERREADDATE cannot be found');
  END;

  BEGIN
    IF (:NEW.METERREAD < (NVL(:NEW.PREVIOUSMETERREADING,0)) AND (:NEW.ROLLOVERINDICATOR = 0) AND (:NEW.PREVMETERREF = :NEW.METERREF)) THEN
      RAISE_APPLICATION_ERROR( -20004, 'Invalid Meter Reading: METERREAD must be > previous METERREAD value if ROLLOVERINDICATOR = 0');
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END;
END;
/
ALTER TRIGGER DEL_METER_READING_TRG ENABLE;
/
create or replace
TRIGGER DEL_SERVICE_COMPONENT_TRG
  BEFORE INSERT OR UPDATE ON DEL_SERVICE_COMPONENT
  FOR EACH ROW
BEGIN
  -- METERED POTABLE WATER
  IF(:NEW.METEREDPWTARIFFCODE IS NOT NULL) THEN -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.MPWSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.MPWSPECIALAGREEMENTFACTOR IS NULL OR :NEW.MPWSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MPWSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.MPWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.MPWSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MPWSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.MPWSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.MPWSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.MPWSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  --METERED NON-POTABLE WATER
  IF(:NEW.METEREDNPWTARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.MNPWSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.MNPWSPECIALAGREEMENTFACTOR IS NULL OR :NEW.MNPWSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MNPWSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.MNPWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.MNPWSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MNPWSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.MNPWSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.MNPWSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.MNPWSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- ASSESSED WATER
  IF(:NEW.AWASSESSEDTARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.AWSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.AWSPECIALAGREEMENTFACTOR IS NULL OR :NEW.AWSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.AWSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.AWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.AWSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.AWSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.AWSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.AWSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.AWSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- ASSESSED SEWERAGE
  IF(:NEW.ASASSESSEDTARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.ASSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.ASSPECIALAGREEMENTFACTOR IS NULL OR :NEW.ASSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.ASSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.ASSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.ASSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.ASSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.ASSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.ASSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.ASSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- UNMEASURED WATER
  IF(:NEW.UWUNMEASUREDTARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.UWSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.UWSPECIALAGREEMENTFACTOR IS NULL OR :NEW.UWSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.UWSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.UWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.UWSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.UWSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.UWSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.UWSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.UWSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- UNMEASURED SEWERAGE
  IF(:NEW.USUNMEASUREDTARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.USSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.USSPECIALAGREEMENTFACTOR IS NULL OR :NEW.USSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.USSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.USSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.USSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.USSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.USSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.USSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.USSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- METERED SEWERAGE
  IF(:NEW.METEREDFSTARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.MFSSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.MFSSPECIALAGREEMENTFACTOR IS NULL OR :NEW.MFSSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MFSSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.MFSSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.MFSSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MFSSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.MFSSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.MFSSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.MFSSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- SURFACE WATER DRAINAGE
  IF(:NEW.SRFCWATERTARRIFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.SWSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.SWSPECIALAGREEMENTFACTOR IS NULL OR :NEW.SWSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.SWSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.SWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.SWSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.SWSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.SWSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.SWSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.SWSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;

  -- HIGHWAY DRAINAGE
  IF(:NEW.HWAYDRAINAGETARIFFCODE IS NOT NULL) THEN  -- if there is a tariff code then the special agreement flag must be set
    IF(:NEW.HDSPECIALAGREEMENTFLAG = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
      IF(:NEW.HDSPECIALAGREEMENTFACTOR IS NULL OR :NEW.HDSPECIALAGREEMENTFACTOR < 0) THEN
        RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.HDSPECIALAGREEMENTREF IS NULL) THEN
        RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSIF (:NEW.HDSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor should be 0 and reference data should be NA
      IF(:NEW.HDSPECIALAGREEMENTFACTOR <> 100) THEN
        RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.HDSPECIALAGREEMENTREF <> 'NA') THEN
        RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Special Agreement Reference invalid');
      END IF;
    ELSE
      RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Special Agreement Flag invalid');
    END IF;
  ELSE -- if there is no tariff code then the special agreement fields should all be NULL
    IF (:NEW.HDSPECIALAGREEMENTFLAG IS NOT NULL OR
        :NEW.HDSPECIALAGREEMENTFACTOR IS NOT NULL OR
        :NEW.HDSPECIALAGREEMENTREF IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Tariff code is missing');
    END IF;
  END IF;
END;
/
ALTER TRIGGER DEL_SERVICE_COMPONENT_TRG ENABLE;
/
create or replace
TRIGGER DEL_METER_DISCHARGE_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER_DISCHARGE_POINT
  FOR EACH ROW
DECLARE
  l_count NUMBER;
  l_owc VARCHAR2(20);
BEGIN
  -- check if the new meter is an OWC TE meter
  SELECT COUNT(*)
  INTO l_count
  FROM MOUTRAN.MO_METER_DPIDXREF
  WHERE UPPER(MANUFACTURER_PK) = :NEW.MANUFACTURER_PK
  AND MANUFACTURERSERIALNUM_PK = :NEW.MANUFACTURERSERIALNUM_PK
  AND OWC IS NOT NULL;
  
  -- if it is an OWC TE meter then just add it otherwise check if the meter existing in the DEL_METER table
  IF l_count = 0 THEN
    SELECT COUNT(*)
    INTO l_count
    FROM DEL_METER
    WHERE MANUFACTURER_PK = :NEW.MANUFACTURER_PK
    AND MANUFACTURERSERIALNUM_PK = :NEW.MANUFACTURERSERIALNUM_PK;    
    
    IF l_count = 0 THEN
      RAISE_APPLICATION_ERROR( -20001, 'Meter does not exist in DEL_METER table');
    END IF;
  END IF;
END;
/
ALTER TRIGGER DEL_METER_DISCHARGE_TRG ENABLE;
/
exit;