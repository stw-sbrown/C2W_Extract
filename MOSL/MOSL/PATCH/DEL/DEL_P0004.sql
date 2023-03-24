------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	DEL_P0004.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	26/04/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL DEL database
--
-- NOTES  			      :	Place a summary at the top of the file with more detailed information 
--					            where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	DEL_P0001.sql
-- ASSOCIATED SCRIPTS :	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	  Date            Author         	Description
-- ---------      ----------      -------         ----------------------------------------------------------
-- V0.01          26/04/2016      K.Burton        1. Update trigger  DEL_METER_TRG
--                                                   changed the postcode error output format
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER
  FOR EACH ROW
BEGIN
  --INITIAL METER READ DATE SHOULD BE BEFORE MARKET OPERATION DA, I.E. LESS THAN TODAY DATE
  IF( :NEW.INITIALMETERREADDATE >= SYSDATE ) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Initial Meter Read Date: INITIALMETERREADDATE should be before the Market Operation day');
  END IF;

  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;
END;
/
exit;