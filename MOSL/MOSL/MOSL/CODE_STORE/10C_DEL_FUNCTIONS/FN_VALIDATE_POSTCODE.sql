----------------------------------------------------------------------------------------
-- FUNCTION SPECIFICATION: Postcode Validtion
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : FN_VALIDATE_POSTCODE.sql
--
-- Subversion $Revision: 4023 $
--
-- CREATED        : 20/04/2016
--
-- DESCRIPTION    : Function to validate postcodes according to MOSL specification
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      20/04/2016  K.Burton   Initial Draft
-- V 0.02      03/05/2016  K.Burton   Added 'I' to list of excluded letters for 3rd alpha
--                                    character as per MOSL spec
-- V 0.03      05/05/2016  K.Burton   Added code to pre-format postcodes into CCCC CCC format
--                                    Return type also changed to VARCHAR2	
-----------------------------------------------------------------------------------------
create or replace
FUNCTION FN_VALIDATE_POSTCODE(PA_PCODE IN VARCHAR2) RETURN VARCHAR2 AS
  l_postcode VARCHAR2(8);    
BEGIN
  l_postcode := REPLACE(PA_PCODE,chr(32),'');
  l_postcode := SUBSTR(l_postcode,1,LENGTH(l_postcode)-3) || ' ' || SUBSTR(l_postcode,LENGTH(l_postcode)-2);
  
  IF L_POSTCODE = 'GIR 0AA' THEN
    RETURN l_postcode;
  END IF;

  IF LENGTH(l_postcode) > 8 THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_postcode,1,1) IN ('Q','V','X') THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_postcode,2,1) IN ('I','J','Z') THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_postcode,3,1) IN ('L','M','N','O','Q','R','V','X','Y','Z') THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_postcode,4,1) IN ('C','D','F','G','I','J','K','L','O','Q','S','T','U','Z') THEN
    RETURN 'INVALID';
  END IF;

  IF REGEXP_LIKE(l_postcode, '^[[:alpha:]]{1,2}[[:digit:]]{1,2}[[:alpha:]]{0,1}[[:space:]]{1}[[:digit:]]{1}[[:alpha:]]{2}') THEN
    RETURN l_postcode;
  END IF;

  RETURN 'INVALID';
END;
/
exit;