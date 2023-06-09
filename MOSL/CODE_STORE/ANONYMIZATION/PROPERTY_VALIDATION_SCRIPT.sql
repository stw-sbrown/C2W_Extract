
------------------------------------------------------------------------------------------------------------
-- DESCRIPTION: 	      Script to get all MO data from MOUTRAN tables related to a single NO_PROPERTY
--                      Used for validating transform data for a single property number
-- $Revision: 5375 $
--
-- EXAMPLE NO_PROPERTY: 831174712 831111846
--
-- DIRECTIONS FOR USE:  Open script in SQL Developer
--      OPTION 1:         Select whole script, 
--                        Select run as QUERY STATEMENTS (F9 or CTRL-ENTER)
--                        Input NO_PROPERTY value when prompted
--                        Click OK
--                        (Datasets open in separate Query Result Windows)
--
--      OPTION 2:         Select run as SCRIPT (F5)
--                        Input NO_PROPERTY value when prompted
--                        Click OK
--                        (Output all results in a single Script Output window)
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      ----------      -------         ----------------------------------------------------------
-- V0.03          06/09/2016      D.Cheung        Add MO_METER_DPIDXREF
-- V0.02          23/05/2016      D.Cheung        Add Header to identify datasets, hide substitution comments   
-- V0.01		      20/05/2016      D.Cheung	      Initial Draft
--
------------------------------------------------------------------------------------------------------------
 
 --*** CLEAR THE OUTPUT SCREEN ***
CLEAR SCREEN;
--*** HIDE SUBSTITUTION COMMENTS ***
SET VERIFY OFF;

--*** PROMPT FOR NO_PROPERTY VALUE ***
accept v_no_property prompt 'Please enter NO_PROPERTY:';
    
--*** WRITE HEADER ****
prompt **********************************************;
prompt MO details for property: &v_no_property;
prompt **********************************************;
prompt ;

--prompt *** PROPERTY ***;
SELECT * FROM MO_ELIGIBLE_PREMISES WHERE STWPROPERTYNUMBER_PK = &v_no_property;

prompt *** PROPERTY_ADDRESS ***;
SELECT ' ' PROPERTY_ADDRESS, PA.ADDRESSPROPERTY_PK, MA.PRIMARYADDRESSABLEOBJECT, MA.SECONDADDRESABLEOBJECT, MA.ADDRESSLINE01, MA.ADDRESSLINE02, MA.ADDRESSLINE03, MA.ADDRESSLINE04, MA.ADDRESSLINE05, MA.POSTCODE, MA.COUNTRY 
FROM MO_PROPERTY_ADDRESS PA JOIN MO_ADDRESS MA ON MA.ADDRESS_PK = PA.ADDRESS_PK
WHERE PA.STWPROPERTYNUMBER_PK = &v_no_property;

PROMPT *** CUSTOMERS ***;
SELECT mc.CUSTOMERNUMBER_PK, mc.CUSTOMERNAME, mc.CUSTOMERBANNERNAME, MC.STDINDUSTRYCLASSCODE, MC.STDINDUSTRYCLASSCODETYPE, MC.COMPANIESHOUSEREFNUM, MC.CUSTOMERCLASSIFICATION FROM MO_CUSTOMER MC, MO_SUPPLY_POINT msp WHERE msp.STWPROPERTYNUMBER_PK = &v_no_property and MC.CUSTOMERNUMBER_PK = msp.CUSTOMERNUMBER_PK;

prompt *** CUSTOMER_ADDRESS ***;
SELECT ' ' CUSTOMER_ADDRESS, CA.CUSTOMERNUMBER_PK, CA.ADDRESSPROPERTY_PK
, MA.PRIMARYADDRESSABLEOBJECT, MA.SECONDADDRESABLEOBJECT, MA.ADDRESSLINE01, MA.ADDRESSLINE02, MA.ADDRESSLINE03, MA.ADDRESSLINE04, MA.ADDRESSLINE05, MA.POSTCODE, MA.COUNTRY 
FROM MO_CUST_ADDRESS CA JOIN MO_ADDRESS MA ON MA.ADDRESS_PK = CA.ADDRESS_PK
WHERE CA.STWPROPERTYNUMBER_PK = &v_no_property;

prompt *** SUPPLY_POINT ***;
SELECT * FROM MO_SUPPLY_POINT WHERE STWPROPERTYNUMBER_PK = &v_no_property;

PROMPT *** SERVICE_COMPONENT ***;
--SELECT ' ' SERVICE_COMPONENT, SERVICECOMPONENTREF_PK, TARIFFCODE_PK, SPID_PK, DPID_PK, STWSERVICETYPE, SERVICECOMPONENTTYPE FROM MO_SERVICE_COMPONENT WHERE STWPROPERTYNUMBER_PK = &v_no_property;
SELECT * FROM MO_SERVICE_COMPONENT  WHERE STWPROPERTYNUMBER_PK = &v_no_property; 

PROMPT *** DISCHARGE_POINT ***;
--SELECT ' ' DISCHARGE_POINT, DPID_PK, SPID_PK, SERVICECOMPTYPE, TARRIFCODE FROM MO_DISCHARGE_POINT WHERE STWPROPERTYNUMBER_PK = &v_no_property;
SELECT * FROM MO_DISCHARGE_POINT WHERE STWPROPERTYNUMBER_PK = &v_no_property;



PROMPT *** METER_TARGET ***;
--SELECT ' ' METER, METERREF, MANUFCODE, MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK, SPID_PK, METERLOCFREEDESCRIPTOR FROM MO_METER WHERE INSTALLEDPROPERTYNUMBER = &v_no_property;
SELECT * FROM MO_METER WHERE NVL(MASTER_PROPERTY,INSTALLEDPROPERTYNUMBER) = &v_no_property;

PROMPT *** METER_SPID_ASSOC ***; 
SELECT * FROM MO_METER_SPID_ASSOC WHERE STWPROPERTYNUMBER_PK = &v_no_property;

PROMPT *** METER_DPIDXREF ***; 
SELECT * FROM MO_METER_DPIDXREF WHERE INSTALLEDPROPERTYNUMBER = &v_no_property ORDER BY METERDPIDXREF_PK;

prompt *** METER_NETWORK ***;
/*
SELECT ' ' METER_NETWORK, MN.MAIN_METERREF, MN.MAIN_MANUFCODE, MN.MAIN_MANUFACTURER_PK, MN.MAIN_MANSERIALNUM_PK, MN.MAIN_SPID
, MN.SUB_METERREF, MN.SUB_MANUFCODE, MN.SUB_MANUFACTURER_PK, MN.SUB_MANSERIALNUM_PK, MN.SUB_SPID 
FROM MO_METER_NETWORK MN
WHERE MN.MAIN_STWPROPERTYNUMBER_PK = &v_no_property OR MN.SUB_STWPROPERTYNUMBER_PK = &v_no_property
ORDER BY MN.MAIN_METERREF;
*/
SELECT *
FROM MO_METER_NETWORK MN
WHERE MN.MAIN_STWPROPERTYNUMBER_PK = &V_NO_PROPERTY OR MN.SUB_STWPROPERTYNUMBER_PK = &V_NO_PROPERTY
ORDER BY MN.MAIN_METERREF;

prompt *** METER_READING ***;
SELECT ' ' METER_READING, MR.METERREF, MR.MANUFCODE, MR.MANUFACTURER_PK, MR.MANUFACTURERSERIALNUM_PK, MR.METERREADDATE, MR.METERREAD, MR.METERREADMETHOD, MR.ROLLOVERINDICATOR 
FROM MO_METER_READING MR 
WHERE MR.INSTALLEDPROPERTYNUMBER = &v_no_property
ORDER BY MR.METERREADDATE;

prompt *** METER_ADDRESS ***;
SELECT ' ' METER_ADDRESS, MMA.ADDRESSPROPERTY_PK, MMA.MANUFCODE, MMA.MANUFACTURER_PK, MMA.METERSERIALNUMBER_PK
, MA.PRIMARYADDRESSABLEOBJECT, MA.SECONDADDRESABLEOBJECT, MA.ADDRESSLINE01, MA.ADDRESSLINE02, MA.ADDRESSLINE03, MA.ADDRESSLINE04, MA.ADDRESSLINE05, MA.POSTCODE, MA.COUNTRY 
FROM MO_METER_ADDRESS MMA JOIN MO_ADDRESS MA ON MA.ADDRESS_PK = MMA.ADDRESS_PK
WHERE MMA.INSTALLEDPROPERTYNUMBER = &v_no_property;

--*** END OF SCRIPT TIDY UP ***
prompt ;
prompt **********************************************;
prompt END OF RESULTS FOR PROPERTY:  &v_no_property;
prompt **********************************************;
SET VERIFY ON;
/
