------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	DEL_P0003.sql
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
-- V0.01          25/04/2016      K.Burton        1. Update trigger DEL_SUPPLY_POINT_TRG
--                                                   changed the postcode error output format
------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_SUPPLY_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_SUPPLY_POINT
  FOR EACH ROW
BEGIN
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.PREMPOSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;

  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.CUSTPOSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode Error');
  END IF;
END;
/
exit;