------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	DEL_P0005.sql
--
--
-- Subversion $Revision: 4023 $	
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
        WHERE  :NEW.MANUFACTURER_PK = DM.MANUFACTURER_PK
            AND :NEW.MANUFACTURERSERIALNUM_PK= DM.MANUFACTURERSERIALNUM_PK
            AND :NEW.SPID_PK = DS.SPID_PK
            AND (
                (DM.METERTREATMENT = 'POTABLE' AND DS.SERVICECATEGORY = 'W')
                OR
                (DM.METERTREATMENT = 'SEWERAGE' AND DS.SERVICECATEGORY = 'S')
            );
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;

    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'SPID association not valid for Service Category and MeterTreatment');
    END IF;

END DEL_METER_SUPPLY_POINT_TRG;
/
ALTER TRIGGER MOUDEL.DEL_METER_SUPPLY_POINT_TRG ENABLE;
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
        JOIN DEL_METER_SUPPLY_POINT DS ON (DS.MANUFACTURER_PK = DM.MANUFACTURER_PK
            AND DS.MANUFACTURERSERIALNUM_PK = DM.MANUFACTURERSERIALNUM_PK)
        WHERE  :NEW.MAINMANUFACTURER = DM.MANUFACTURER_PK
            AND :NEW.MAINMANUFACTURERSERIALNUM = DM.MANUFACTURERSERIALNUM_PK;
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
        WHERE  :NEW.SUBMANUFACTURER = DM.MANUFACTURER_PK
            AND :NEW.SUBMANUFACTURERSERIALNUM = DM.MANUFACTURERSERIALNUM_PK;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_spidcount := 0;
    END;

    IF (l_spidcount = 0) THEN
        RAISE_APPLICATION_ERROR( -20001, 'SUB Manufacturer and Serial Num combination missing in METER or METER_SUPPLY_POINT');
    END IF;

END DEL_METER_NETWORK_TRG;
/
ALTER TRIGGER MOUDEL.DEL_METER_NETWORK_TRG ENABLE;
/
ALTER TABLE DEL_METER_NETWORK MODIFY (MAINMETERTREATMENT NULL);
ALTER TABLE DEL_METER_NETWORK DROP CONSTRAINT CH01_MNSUBTREATMENT;
/
exit;