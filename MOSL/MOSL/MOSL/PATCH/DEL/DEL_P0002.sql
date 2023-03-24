------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	DEL_P0002.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	25/04/2016
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
-- V0.01          25/04/2016      K.Burton        1. Update trigger DEL_DISCHARGE_POINT_TRG
--                                                   Added checks for special agreements
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_DISCHARGE_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_DISCHARGE_POINT
  FOR EACH ROW
BEGIN
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;

  IF(:NEW.DPIDSPECIALAGREEMENTINPLACE = 1) THEN   -- if the special agreement flag = 1 then the factor and reference data should be given
    IF(:NEW.DPIDSPECIALAGREEMENTFACTOR IS NULL OR :NEW.DPIDSPECIALAGREEMENTFACTOR < 0) THEN
      RAISE_APPLICATION_ERROR( -20002, 'Discharge Point Special Agreement Data Error: Special Agreement Factor invalid');
    END IF;
    IF(:NEW.DPIDSPECIALAGREEMENTREFERENCE IS NULL) THEN
      RAISE_APPLICATION_ERROR( -20002, 'Discharge Point Special Agreement Data Error: Special Agreement Reference invalid');
    END IF;
  ELSIF (:NEW.DPIDSPECIALAGREEMENTINPLACE = 0) THEN -- if the special agreement flag = 0 then the factor and reference data should be NULL
    IF(:NEW.DPIDSPECIALAGREEMENTFACTOR IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20002, 'Discharge Point Special Agreement Data Error: Special Agreement Factor invalid');
    END IF;
    IF(:NEW.DPIDSPECIALAGREEMENTREFERENCE IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR( -20002, 'Discharge Point Special Agreement Data Error: Special Agreement Reference invalid');
    END IF;
  ELSE
    RAISE_APPLICATION_ERROR( -20002, 'Discharge Point Special Agreement Data Error: Special Agreement Flag invalid');
  END IF;
END;
/
exit;