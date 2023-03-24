------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	DEL_P0001.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	22/04/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL DEL database
--
-- NOTES  			      :	Place a summary at the top of the file with more detailed information 
--					            where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS :	
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	  Date            Author         	Description
-- ---------      ----------      -------         ----------------------------------------------------------
-- V0.01	  22/04/2016	  K.Burton        1. Alter constraints CH02_WATERCHARGEMETERSIZE and
--                                                   CH01_SEWCHARGEABLEMETERSIZE on table DEL_METER
-- V0.02          25/04/2016      K.Burton        1. Remove unique key constraints from DEL_SERVICE_COMPONENT
--                                                   table and replace with single composite key
--                                                2. Drop constraint CH01_SPECIALAGREEMENTREF from table
--                                                   DEL_DISCHARGE_POINT. This is replaced by trigger
--                                                   DEL_DISCHARGE_POINT_TRG (Patch DEL_P0002.sql)
-- V0.03          26/04/2016      K.Burton        1. Drop column COUNTRY from table DEL_METER (Defect 21)
-- V0.04          06/05/2016      K.Burton        1. Add columns  METERREF and PREVMETERREF to DEL_METER_READING
------------------------------------------------------------------------------------------------------------
-- CHANGES
-- DROP CONSTRAINTS AND REBUILD
-- WATERCHARGEMETERSIZE must be 0 unless the associated SPID is a water SPID. This is indicated by the
-- METERTREATMENT field. If METERTREATMENT is 'POTABLE' or 'NON-POTABLE' then WATERCHARGEMETERSIZE should be
-- > 0 otherwise it should be 0. 
-- SEWCHARGEABLEMETERSIZE should be NULL unless METERTREATMENT = 'SEWERAGE'
------------------------------------------------------------------------------------------------------------
ALTER TABLE DEL_METER DROP CONSTRAINT CH02_WATERCHARGEMETERSIZE;
ALTER TABLE DEL_METER DROP CONSTRAINT CH01_SEWCHARGEABLEMETERSIZE;

ALTER TABLE DEL_METER ADD CONSTRAINT CH02_WATERCHARGEMETERSIZE CHECK ((METERTREATMENT IN ('POTABLE','NONPOTABLE') AND WATERCHARGEMETERSIZE > 0) OR (METERTREATMENT NOT IN ('POTABLE','NONPOTABLE') AND WATERCHARGEMETERSIZE = 0));
ALTER TABLE DEL_METER ADD CONSTRAINT CH01_SEWCHARGEABLEMETERSIZE CHECK ((METERTREATMENT = 'SEWERAGE' AND SEWCHARGEABLEMETERSIZE IS NOT NULL) OR (METERTREATMENT <> 'SEWERAGE' AND SEWCHARGEABLEMETERSIZE IS NULL));

commit;
------------------------------------------------------------------------------------------------------------
-- CHANGES
-- DROP CONSTRAINTS AND REBUILD
-- Combination to SPID and TARIFFCODE must be unique
------------------------------------------------------------------------------------------------------------
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_MPWUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_UWUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_ASUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_USUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_HDUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_AWUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_SWUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_MSUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_MNPWUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_SCAUNIQUEKEY;
ALTER TABLE DEL_SERVICE_COMPONENT DROP CONSTRAINT CH01_WCAUNIQUEKEY;

ALTER TABLE DEL_SERVICE_COMPONENT ADD CONSTRAINT CH01_SCUNIQUEKEY UNIQUE (SPID_PK,
                                                                          METEREDPWTARIFFCODE,
                                                                          UWUNMEASUREDTARIFFCODE,
                                                                          ASASSESSEDTARIFFCODE,
                                                                          USUNMEASUREDTARIFFCODE,
                                                                          HWAYDRAINAGETARIFFCODE,
                                                                          AWASSESSEDTARIFFCODE,
                                                                          SRFCWATERTARRIFCODE,
                                                                          METEREDFSTARIFFCODE,
                                                                          METEREDNPWTARIFFCODE,
                                                                          SADJCHARGEADJTARIFFCODE,
                                                                          WADJCHARGEADJTARIFFCODE);

ALTER TABLE DEL_SERVICE_COMPONENT ADD CONSTRAINT CH01_SWDRAINAGEAREA CHECK ((SRFCWATERTARRIFCODE IS NOT NULL AND SRFCWATERAREADRAINED IS NOT NULL) OR (SRFCWATERTARRIFCODE IS NULL AND SRFCWATERAREADRAINED IS NULL));
ALTER TABLE DEL_SERVICE_COMPONENT ADD CONSTRAINT CH01_HDSURFACEAREA CHECK ((HWAYDRAINAGETARIFFCODE IS NOT NULL AND HWAYSURFACEAREA IS NOT NULL) OR (HWAYDRAINAGETARIFFCODE IS NULL AND HWAYSURFACEAREA IS NULL));
                                                                          
commit;                                                                          
------------------------------------------------------------------------------------------------------------
-- CHANGES
-- DROP CONSTRAINT
-- Constraint CH01_SPECIALAGREEMENTREF is replaced by trigger DEL_DISCHARGE_POINT_TRG
------------------------------------------------------------------------------------------------------------
ALTER TABLE DEL_DISCHARGE_POINT DROP CONSTRAINT CH01_SPECIALAGREEMENTREF;

commit;

------------------------------------------------------------------------------------------------------------
-- CHANGES
-- DROP TABLE COLUMN
-- Column COUNTRY is not required in table DEL_METER
------------------------------------------------------------------------------------------------------------
ALTER TABLE DEL_METER DROP COLUMN COUNTRY;

commit;
------------------------------------------------------------------------------------------------------------
-- CHANGES
-- ADD TABLE COLUMNS
-- Addition columns METERREF and PREVMETERREF added to DEL_METER_READING for ROLLOVER constraint
------------------------------------------------------------------------------------------------------------
ALTER TABLE DEL_METER_READING ADD METERREF NUMBER(9,0);
ALTER TABLE DEL_METER_READING ADD PREVMETERREF NUMBER (9,0);
------------------------------------------------------------------------------------------------------------
-- CHANGES
-- ADD TABLE COLUMNS
-- Addition of column SPID to DEL_METER for cross border view generation
------------------------------------------------------------------------------------------------------------
ALTER TABLE DEL_METER ADD SPID VARCHAR2(13);
/
exit;