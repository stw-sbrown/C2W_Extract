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
