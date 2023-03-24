----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Discharge Point Delivery Extract 
-- AUTHOR         : Dominic Cheung
-- CREATED        : 21/04/2016
-- Subversion $Revision: 5148 $
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter network delivery extract
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version  Date        Author      Description
-- -------  ----------  ----------- -----------------------------------------------------------------------
-- V0.01	  11/08/2016	S.Badhan    column on join MANUFACTURERSERIALNUM_P changed to MANUFACTURERSERIALNUM_PK.
-- V0.01	  21/04/2016	D.Cheung    Initial version.
------------------------------------------------------------------------------------------------------------
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
        JOIN DEL_METER_SUPPLY_POINT DS ON (DS.MANUFACTURER_PK = DM.MANUFACTURER_PK
            AND DS.MANUFACTURERSERIALNUM_PK = DM.MANUFACTURERSERIALNUM_PK)
        WHERE  TRIM(:NEW.MAINMANUFACTURER) = DM.MANUFACTURER_PK
            AND TRIM(:NEW.MAINMANUFACTURERSERIALNUM) = DM.MANUFACTURERSERIALNUM_PK;
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
        JOIN DEL_METER_SUPPLY_POINT DS ON (DS.MANUFACTURER_PK = DM.MANUFACTURER_PK
            AND DS.MANUFACTURERSERIALNUM_PK= DM.MANUFACTURERSERIALNUM_PK)
        WHERE  TRIM(:NEW.SUBMANUFACTURER) = DM.MANUFACTURER_PK
            AND TRIM(:NEW.SUBMANUFACTURERSERIALNUM) = DM.MANUFACTURERSERIALNUM_PK;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;

    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'SUB Manufacturer and Serial Num combination missing in METER or METER_SUPPLY_POINT');
    END IF;

END DEL_METER_NETWORK_TRG;
/
show error;
exit;