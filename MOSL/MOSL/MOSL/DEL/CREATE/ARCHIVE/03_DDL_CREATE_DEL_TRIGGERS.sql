------------------------------------------------------------------------------
-- TASK				: 	MOSL DELIVERY CREATION 
--
-- AUTHOR         		: 	Kevin Burton
--
-- FILENAME       		: 	03_DDL_CREATE_DEL_TRIGGERS.sql
--
-- CREATED        		: 	20/04/2016
--	
-- DESCRIPTION 		   	: 	Creates all database triggers for initial MOSL upload
--
-- NOTES  			:	
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS 	 	:	01_DDL_CREATE_DEL.sql
--                                      02_DDL_CREATE_DEL_VIEW.sql
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date        	 	Author         	Description
-- --------------------------------------------------------------------------------------------------------
-- V0.01	       	20/04/2016   	 	K.Burton	Initial version
-- V0.02                20/04/2016		K.Burton	Additional triggers for DEL_SERVICE_COMPONENT
--								and DEL_METER
------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Meter Reading Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 19/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter reading delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_READING_TRG
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
    IF (:NEW.METERREAD < (NVL(:NEW.PREVIOUSMETERREADING,0)) AND (:NEW.ROLLOVERINDICATOR = 0)) THEN
      RAISE_APPLICATION_ERROR( -20004, 'Invalid Meter Reading: METERREAD must be > previous METERREAD value if ROLLOVERINDICATOR = 0');
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END;
END;

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

----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Meter Delivery Extract 
-- AUTHOR         : Dominic Cheung
-- CREATED        : 20/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER
  FOR EACH ROW
BEGIN
  --INITIAL METER READ DATE SHOULD BE BEFORE MARKET OPERATION DA, I.E. LESS THAN TODAY DATE
  IF( :NEW.INITIALMETERREADDATE >= SYSDATE ) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Initial Meter Read Date: INITIALMETERREADDATE should be before the Market Operation day');
  END IF;

  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.POSTCODE);
  END IF;
END;

----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Supply Point Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 20/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for supply point delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_SUPPLY_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_SUPPLY_POINT
  FOR EACH ROW
BEGIN
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.PREMPOSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.PREMPOSTCODE);
  END IF;
  
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.CUSTPOSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.PREMPOSTCODE);
  END IF;
END;

----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Discharge Point Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 20/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for discharge point delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_DISCHARGE_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_DISCHARGE_POINT
  FOR EACH ROW
BEGIN
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.POSTCODE);
  END IF;
END;

----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Discharge Point Delivery Extract 
-- AUTHOR         : Dominic Cheung
-- CREATED        : 21/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter supply point delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_SUPPLY_POINT_TRG
    BEFORE
        INSERT
    ON DEL_METER_SUPPLY_POINT
    FOR EACH ROW
DECLARE
--local variables
    c_module_name                 CONSTANT VARCHAR2(30) := 'DEL_METER_SUPPLY_POINT_TRG';  -- modify
    l_spidcount                   NUMBER;
  
BEGIN
    --INITIALISE VARIABLES;
    l_spidcount := 0;

    --RULE: Service category and service components must be appropriate for the Supply Point
    BEGIN 
        SELECT COUNT(*)
        INTO   l_spidcount
        FROM   DEL_METER DM,
        DEL_SUPPLY_POINT DS
        WHERE  TRIM(:NEW.MANUFACTURER_PK) = TRIM(DM.MANUFACTURER_PK)
            AND TRIM(:NEW.MANUFACTURERSERIALNUM_PK)= TRIM(DM.MANUFACTURERSERIALNUM_PK)
            AND TRIM(:NEW.SPID_PK) = TRIM(DS.SPID_PK)
            AND (
                (TRIM(DM.METERTREATMENT) = 'POTABLE' AND DS.SERVICECATEGORY = 'W')
                OR
                (TRIM(DM.METERTREATMENT) = 'SEWERAGE' AND DS.SERVICECATEGORY = 'S')
            );
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;
        
    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'SPID association not valid for Service Category and MeterTreatment');
    END IF;
      
END DEL_METER_SUPPLY_POINT_TRG;

----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Discharge Point Delivery Extract 
-- AUTHOR         : Dominic Cheung
-- CREATED        : 21/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter network delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_NETWORK_TRG
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
        JOIN DEL_METER_SUPPLY_POINT DS ON (TRIM(DS.MANUFACTURER_PK) = TRIM(DM.MANUFACTURER_PK)
            AND TRIM(DS.MANUFACTURERSERIALNUM_PK)= TRIM(DM.MANUFACTURERSERIALNUM_PK))
        WHERE  TRIM(:NEW.MAINMANUFACTURER) = TRIM(DM.MANUFACTURER_PK)
            AND TRIM(:NEW.MAINMANUFACTURERSERIALNUM)= TRIM(DM.MANUFACTURERSERIALNUM_PK);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;
        
    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'MAIN Manufacturer and Serial Num combination missing in METER or METER_SUPPLY_POINT');
    END IF;
    
    --RULE: Sub Manufacturer and Serial Num combination must be already provided
    BEGIN 
        SELECT COUNT(*)
        INTO   l_spidcount
        FROM   DEL_METER DM
        JOIN DEL_METER_SUPPLY_POINT DS ON (TRIM(DS.MANUFACTURER_PK) = TRIM(DM.MANUFACTURER_PK)
            AND TRIM(DS.MANUFACTURERSERIALNUM_PK)= TRIM(DM.MANUFACTURERSERIALNUM_PK))
        WHERE  TRIM(:NEW.SUBMANUFACTURER) = TRIM(DM.MANUFACTURER_PK)
            AND TRIM(:NEW.SUBMANUFACTURERSERIALNUM)= TRIM(DM.MANUFACTURERSERIALNUM_PK);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;
        
    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'SUB Manufacturer and Serial Num combination missing in METER or METER_SUPPLY_POINT');
    END IF;
      
END DEL_METER_NETWORK_TRG;

commit;
exit;
