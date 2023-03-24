----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Service Component Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 20/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for service component delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_SERVICE_COMPONENT_TRG
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
    ELSIF (:NEW.MPWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.MPWSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20001, 'Metered Potable Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MPWSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.MNPWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.MNPWSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20002, 'Metered Non-Potable Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MNPWSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.AWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.AWSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20003, 'Assessed Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.AWSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.ASSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.ASSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20004, 'Assessed Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.ASSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.UWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.UWSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20005, 'Unmeasured Water Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.UWSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.USSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.USSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20006, 'Unmeasured Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.USSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.MFSSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.MFSSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20007, 'Metered Sewerage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.MFSSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.SWSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.SWSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20008, 'Surface Water Drainage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.SWSPECIALAGREEMENTREF IS NOT NULL) THEN
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
    ELSIF (:NEW.HDSPECIALAGREEMENTFLAG = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
      IF(:NEW.HDSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
        RAISE_APPLICATION_ERROR( -20009, 'Highway Drainage Special Agreement Data Error: Special Agreement Factor invalid');
      END IF;
      IF(:NEW.HDSPECIALAGREEMENTREF IS NOT NULL) THEN
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
exit;
